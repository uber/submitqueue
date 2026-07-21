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

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func setupBuildStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.BuildStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewBuildStore(db, testMetrics())

	return db, mock, store
}

func TestBuildStore_Create(t *testing.T) {
	build := entity.Build{
		ID:        "bk-1001",
		RequestID: "request/monorepo/main/1",
		Status:    entity.BuildStatusAccepted,
		Version:   1,
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
				mock.ExpectExec("INSERT INTO build").
					WithArgs(build.ID, build.RequestID, build.Status, build.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO build").
					WithArgs(build.ID, build.RequestID, build.Status, build.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO build").
					WithArgs(build.ID, build.RequestID, build.Status, build.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBuildStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), build)
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

func TestBuildStore_Get(t *testing.T) {
	want := entity.Build{
		ID:        "bk-1001",
		RequestID: "request/monorepo/main/1",
		Status:    entity.BuildStatusRunning,
		Version:   2,
	}

	tests := []struct {
		name      string
		id        string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.Build
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			id:   want.ID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "request_id", "status", "version"}).
					AddRow(want.ID, want.RequestID, string(want.Status), want.Version)
				mock.ExpectQuery("SELECT id, request_id, status, version").
					WithArgs(want.ID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			id:   "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, request_id, status, version").
					WithArgs("missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: errs.ErrNotFound,
		},
		{
			name: "query error",
			id:   "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, request_id, status, version").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBuildStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), tt.id)
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

func TestBuildStore_Update(t *testing.T) {
	build := entity.Build{ID: "bk-1001", Status: entity.BuildStatusRunning}
	const oldVersion, newVersion = int32(1), int32(2)

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE build").
					WithArgs(build.Status, newVersion, build.ID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE build").
					WithArgs(build.Status, newVersion, build.ID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: errs.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE build").
					WithArgs(build.Status, newVersion, build.ID, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE build").
					WithArgs(build.Status, newVersion, build.ID, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBuildStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), build, oldVersion, newVersion)
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
