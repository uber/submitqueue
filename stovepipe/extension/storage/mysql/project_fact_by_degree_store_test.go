// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"errors"
	"math"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestProjectFactByDegreeStoreCreate(t *testing.T) {
	entry := entity.ProjectFactByDegree{URI: "commit", Degree: 0.5, Project: "p"}
	readErr := errors.New("connection reset")
	for _, tt := range []struct {
		name    string
		dbErr   error
		wantErr error
	}{
		{name: "created"},
		{name: "duplicate", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "write failure", dbErr: readErr, wantErr: readErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			query := mock.ExpectExec("INSERT INTO project_fact_by_degree").WithArgs("queue", entry.URI, entry.Degree, entry.Project)
			if tt.dbErr != nil {
				query.WillReturnError(tt.dbErr)
			} else {
				query.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			store := NewProjectFactByDegreeStore(db, testMetrics(), "queue")
			err = store.Create(context.Background(), entry)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestProjectFactByDegreeStoreList(t *testing.T) {
	columns := []string{"uri", "degree", "project"}
	want := entity.ProjectFactByDegree{URI: "commit", Degree: 0.5, Project: "p"}
	readErr := errors.New("connection reset")
	for _, after := range []entity.ProjectFactByDegree{
		{},
		{URI: "commit", Degree: 1, Project: "z"},
		{URI: "commit", Degree: 0.75, Project: "z"},
		{URI: "commit", Degree: 0, Project: "z"},
	} {
		tests := []struct {
			name     string
			rows     *sqlmock.Rows
			queryErr error
			wantErr  bool
			want     []entity.ProjectFactByDegree
		}{
			{name: "found", rows: sqlmock.NewRows(columns).AddRow(want.URI, want.Degree, want.Project), want: []entity.ProjectFactByDegree{want}},
			{name: "empty", rows: sqlmock.NewRows(columns)},
			{name: "query failure", queryErr: readErr, wantErr: true},
			{name: "scan failure", rows: sqlmock.NewRows(columns).AddRow(want.URI, "invalid", want.Project), wantErr: true},
			{name: "iteration failure", rows: sqlmock.NewRows(columns).AddRow(want.URI, want.Degree, want.Project).RowError(0, readErr), wantErr: true},
		}
		for _, tt := range tests {
			t.Run(tt.name+"/"+after.Project, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				querySQL := "SELECT uri, degree, project FROM project_fact_by_degree WHERE queue = ? AND uri = ?"
				if after != (entity.ProjectFactByDegree{}) {
					querySQL += " AND (degree < ? OR (degree = ? AND project > ?))"
				}
				querySQL += " ORDER BY degree DESC, project ASC LIMIT ?"
				query := mock.ExpectQuery(regexp.QuoteMeta(querySQL))
				if after == (entity.ProjectFactByDegree{}) {
					query.WithArgs("queue", "commit", 3)
				} else {
					query.WithArgs("queue", "commit", after.Degree, after.Degree, after.Project, 3)
				}
				if tt.queryErr != nil {
					query.WillReturnError(tt.queryErr)
				} else {
					query.WillReturnRows(tt.rows)
				}
				store := NewProjectFactByDegreeStore(db, testMetrics(), "queue")
				got, err := store.List(context.Background(), "commit", after, 3)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.want, got)
				}
				if tt.queryErr != nil {
					assert.ErrorIs(t, err, tt.queryErr)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestProjectFactByDegreeStoreRejectsInvalidInput(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewProjectFactByDegreeStore(db, testMetrics(), "queue")
	for _, entry := range []entity.ProjectFactByDegree{
		{Project: "p"},
		{URI: "commit"},
		{URI: "commit", Project: "p", Degree: -1},
		{URI: "commit", Project: "p", Degree: 1.1},
		{URI: "commit", Project: "p", Degree: math.NaN()},
		{URI: "commit", Project: "p", Degree: math.Inf(1)},
	} {
		require.Error(t, store.Create(context.Background(), entry))
	}
	for _, after := range []entity.ProjectFactByDegree{
		{Degree: -1},
		{Degree: 1},
		{URI: "commit"},
		{URI: "other", Project: "p"},
		{URI: "commit", Project: "p", Degree: -0.1},
		{URI: "commit", Project: "p", Degree: 1.1},
		{URI: "commit", Project: "p", Degree: math.NaN()},
		{URI: "commit", Project: "p", Degree: math.Inf(1)},
	} {
		_, err := store.List(context.Background(), "commit", after, 3)
		require.Error(t, err)
	}
	for _, limit := range []int{0, -1} {
		_, err := store.List(context.Background(), "commit", entity.ProjectFactByDegree{}, limit)
		require.Error(t, err)
	}
	_, err = store.List(context.Background(), "", entity.ProjectFactByDegree{}, 3)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
