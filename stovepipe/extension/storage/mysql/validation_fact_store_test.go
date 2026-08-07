// Copyright (c) 2025 Uber Technologies, Inc.
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
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	testFactQueue = "monorepo/main"
	testFactURI   = "git://remote/monorepo/main/deadbeef"
)

func setupValidationFactStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.ValidationFactStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewValidationFactStore(db, testMetrics(), testFactQueue)

	return db, mock, store
}

func TestValidationFactStore_Create(t *testing.T) {
	fact := entity.ValidationFact{
		URI:       testFactURI,
		Project:   "",
		Degree:    entity.DegreeGreen,
		RequestID: "request/monorepo/main/1",
		CreatedAt: 1735689600000,
	}

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO validation_fact").
					WithArgs(testFactQueue, fact.URI, fact.Project, fact.Degree, fact.RequestID, fact.CreatedAt).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate identity returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO validation_fact").
					WithArgs(testFactQueue, fact.URI, fact.Project, fact.Degree, fact.RequestID, fact.CreatedAt).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO validation_fact").
					WithArgs(testFactQueue, fact.URI, fact.Project, fact.Degree, fact.RequestID, fact.CreatedAt).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupValidationFactStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), fact)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestValidationFactStore_Get(t *testing.T) {
	want := entity.ValidationFact{
		URI:       testFactURI,
		Project:   "",
		Degree:    entity.DegreeBroken,
		RequestID: "request/monorepo/main/2",
		CreatedAt: 1735689600000,
	}

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.ValidationFact
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"uri", "project", "degree", "request_id", "created_at"}).
					AddRow(want.URI, want.Project, want.Degree, want.RequestID, want.CreatedAt)
				mock.ExpectQuery("SELECT uri, project, degree, request_id, created_at FROM validation_fact").
					WithArgs(testFactQueue, want.URI, want.Project).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT uri, project, degree, request_id, created_at FROM validation_fact").
					WithArgs(testFactQueue, want.URI, want.Project).
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT uri, project, degree, request_id, created_at FROM validation_fact").
					WithArgs(testFactQueue, want.URI, want.Project).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupValidationFactStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), want.URI, want.Project)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
