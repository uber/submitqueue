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
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestRequestProjectsByIssueStoreCreate(t *testing.T) {
	writeErr := errors.New("connection reset")
	for _, tt := range []struct {
		name    string
		dbErr   error
		wantErr error
	}{
		{name: "affected project"},
		{name: "duplicate", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "write failure", dbErr: writeErr, wantErr: writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//example:test", Project: "service"}
			query := mock.ExpectExec("INSERT INTO request_projects_by_issue").
				WithArgs("queue", "1", entry.TaskID, entry.UnitID, entry.Project)
			if tt.dbErr == nil {
				query.WillReturnResult(sqlmock.NewResult(0, 1))
			} else {
				query.WillReturnError(tt.dbErr)
			}
			store := NewRequestProjectsByIssueStore(db, testMetrics(), "queue")
			err = store.Create(context.Background(), "1", entry)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestProjectsByIssueStoreList(t *testing.T) {
	columns := []string{"task_id", "unit_id", "project"}
	entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "//example:test", Project: "service"}
	readErr := errors.New("connection reset")
	for _, after := range []entity.ProjectIssueAttribution{
		{},
		{TaskID: "test", UnitID: "//example:before", Project: "service"},
	} {
		for _, tt := range []struct {
			name     string
			rows     *sqlmock.Rows
			queryErr error
			want     []entity.ProjectIssueAttribution
			wantErr  bool
		}{
			{name: "found", rows: sqlmock.NewRows(columns).AddRow(entry.TaskID, entry.UnitID, entry.Project), want: []entity.ProjectIssueAttribution{entry}},
			{name: "empty", rows: sqlmock.NewRows(columns)},
			{name: "query failure", queryErr: readErr, wantErr: true},
			{name: "scan failure", rows: sqlmock.NewRows(columns).AddRow(nil, entry.UnitID, entry.Project), wantErr: true},
			{name: "iteration failure", rows: sqlmock.NewRows(columns).AddRow(entry.TaskID, entry.UnitID, entry.Project).RowError(0, readErr), wantErr: true},
		} {
			t.Run(tt.name+"/"+after.UnitID+"/"+after.Project, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				var query *sqlmock.ExpectedQuery
				if after == (entity.ProjectIssueAttribution{}) {
					query = mock.ExpectQuery(regexp.QuoteMeta(listRequestProjectsByIssueQuery)).
						WithArgs("queue", "1", 2)
				} else {
					query = mock.ExpectQuery(regexp.QuoteMeta(listRequestProjectsByIssueAfterCursorQuery)).
						WithArgs("queue", "1", after.TaskID, after.TaskID, after.UnitID, after.TaskID, after.UnitID, after.Project, 2)
				}
				if tt.queryErr == nil {
					query.WillReturnRows(tt.rows).RowsWillBeClosed()
				} else {
					query.WillReturnError(tt.queryErr)
				}
				store := NewRequestProjectsByIssueStore(db, testMetrics(), "queue")
				got, err := store.List(context.Background(), "1", after, 2)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tt.want, got)
				}
				if tt.queryErr != nil {
					require.ErrorIs(t, err, tt.queryErr)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestRequestProjectsByIssueStoreRejectsInvalidInput(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()
	entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "unit", Project: "service"}
	for _, tt := range []struct {
		name      string
		queue     string
		requestID string
	}{
		{name: "missing queue", requestID: "1"},
		{name: "long queue", queue: strings.Repeat("q", 256), requestID: "1"},
		{name: "missing request", queue: "queue"},
		{name: "noncanonical request", queue: "queue", requestID: "01"},
		{name: "invalid request", queue: "queue", requestID: "request"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := NewRequestProjectsByIssueStore(db, testMetrics(), tt.queue)
			require.Error(t, store.Create(ctx, tt.requestID, entry))
			_, err := store.List(ctx, tt.requestID, entity.ProjectIssueAttribution{}, 1)
			require.Error(t, err)
		})
	}
	store := NewRequestProjectsByIssueStore(db, testMetrics(), "queue")
	for _, invalid := range []entity.ProjectIssueAttribution{
		{UnitID: "unit", Project: "service"},
		{TaskID: "test", Project: "service"},
		{TaskID: "test", UnitID: "unit"},
		{Project: "service"},
		{TaskID: strings.Repeat("t", 256), UnitID: "unit", Project: "service"},
		{TaskID: "test", UnitID: strings.Repeat("u", 1025), Project: "service"},
		{TaskID: "test", UnitID: "unit", Project: strings.Repeat("p", 1021)},
	} {
		require.Error(t, store.Create(ctx, "1", invalid))
		_, err := store.List(ctx, "1", invalid, 1)
		require.Error(t, err)
	}
	for _, limit := range []int{0, -1} {
		_, err := store.List(ctx, "1", entity.ProjectIssueAttribution{}, limit)
		require.Error(t, err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequestProjectsByIssueStoreListByIssue(t *testing.T) {
	entry := entity.ProjectIssueAttribution{TaskID: "test", UnitID: "unit", Project: "app"}
	for _, after := range []string{"", "before"} {
		for _, tt := range []struct {
			name    string
			rows    *sqlmock.Rows
			err     error
			want    []entity.ProjectIssueAttribution
			wantErr bool
		}{
			{name: "found", rows: sqlmock.NewRows([]string{"task_id", "unit_id", "project"}).AddRow(entry.TaskID, entry.UnitID, entry.Project), want: []entity.ProjectIssueAttribution{entry}},
			{name: "empty", rows: sqlmock.NewRows([]string{"task_id", "unit_id", "project"})},
			{name: "query error", err: errors.New("query failed"), wantErr: true},
			{name: "scan error", rows: sqlmock.NewRows([]string{"task_id", "unit_id", "project"}).AddRow(nil, entry.UnitID, entry.Project), wantErr: true},
			{name: "iteration error", rows: sqlmock.NewRows([]string{"task_id", "unit_id", "project"}).AddRow(entry.TaskID, entry.UnitID, entry.Project).RowError(0, errors.New("read failed")), wantErr: true},
		} {
			t.Run(tt.name+"/"+after, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				query := mock.ExpectQuery(regexp.QuoteMeta(listProjectsForRequestIssueQuery)).WithArgs("queue", "7", "test", "unit", after, 2)
				if tt.err != nil {
					query.WillReturnError(tt.err)
				} else {
					query.WillReturnRows(tt.rows).RowsWillBeClosed()
				}
				got, err := NewRequestProjectsByIssueStore(db, testMetrics(), "queue").ListByIssue(context.Background(), "7", "test", "unit", after, 2)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, tt.want, got)
				}
				if tt.err != nil {
					require.ErrorIs(t, err, tt.err)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestRequestProjectsByIssueStoreListByIssueInvalidInput(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	for _, tt := range []struct {
		queue, request, task, unit, after string
		limit                             int
	}{
		{"", "7", "test", "unit", "", 1},
		{strings.Repeat("q", 256), "7", "test", "unit", "", 1},
		{"queue", "07", "test", "unit", "", 1},
		{"queue", "7", "", "unit", "", 1},
		{"queue", "7", strings.Repeat("t", 256), "unit", "", 1},
		{"queue", "7", "test", "", "", 1},
		{"queue", "7", "test", strings.Repeat("u", 1025), "", 1},
		{"queue", "7", "test", "unit", strings.Repeat("p", 1021), 1},
		{"queue", "7", "test", "unit", "", 0},
		{"queue", "7", "test", "unit", "", -1},
	} {
		_, err := NewRequestProjectsByIssueStore(db, testMetrics(), tt.queue).ListByIssue(context.Background(), tt.request, tt.task, tt.unit, tt.after, tt.limit)
		require.Error(t, err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
