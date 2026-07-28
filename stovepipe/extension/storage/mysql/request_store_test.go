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
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func setupRequestStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestStore(db, testMetrics())

	return db, mock, store
}

func TestRequestStore_Create(t *testing.T) {
	request := entity.Request{
		ID:            "request/monorepo/main/1",
		Queue:         "monorepo/main",
		URI:           "git://remote/monorepo/main/deadbeef",
		State:         entity.RequestStateAccepted,
		BuildStrategy: entity.BuildStrategyUnknown,
		BaseURI:       "",
		Version:       1,
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
				mock.ExpectExec("INSERT INTO request").
					WithArgs(request.ID, request.Queue, request.URI, request.State, request.BuildStrategy, request.BaseURI, request.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request").
					WithArgs(request.ID, request.Queue, request.URI, request.State, request.BuildStrategy, request.BaseURI, request.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request").
					WithArgs(request.ID, request.Queue, request.URI, request.State, request.BuildStrategy, request.BaseURI, request.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), request)
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

func TestRequestStore_Get(t *testing.T) {
	want := entity.Request{
		ID:            "request/monorepo/main/1",
		Queue:         "monorepo/main",
		URI:           "git://remote/monorepo/main/deadbeef",
		State:         entity.RequestStateProcessing,
		BuildStrategy: entity.BuildStrategyFull,
		BaseURI:       "",
		Version:       2,
	}

	tests := []struct {
		name      string
		id        string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.Request
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			id:   want.ID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "queue", "uri", "state", "build_strategy", "base_uri", "version"}).
					AddRow(want.ID, want.Queue, want.URI, string(want.State), string(want.BuildStrategy), want.BaseURI, want.Version)
				mock.ExpectQuery("SELECT id, queue, uri, state, build_strategy, base_uri, version").
					WithArgs(want.ID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			id:   "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, queue, uri, state, build_strategy, base_uri, version").
					WithArgs("missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "query error",
			id:   "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, queue, uri, state, build_strategy, base_uri, version").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestStoreTest(t)
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

func TestRequestStore_Update(t *testing.T) {
	request := entity.Request{
		ID:            "request/monorepo/main/1",
		URI:           "git://remote/monorepo/main/deadbeef",
		State:         entity.RequestStateProcessing,
		BuildStrategy: entity.BuildStrategyFull,
		BaseURI:       "",
	}
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
				mock.ExpectExec("UPDATE request").
					WithArgs(request.URI, request.State, request.BuildStrategy, request.BaseURI, newVersion, request.ID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(request.URI, request.State, request.BuildStrategy, request.BaseURI, newVersion, request.ID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(request.URI, request.State, request.BuildStrategy, request.BaseURI, newVersion, request.ID, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(request.URI, request.State, request.BuildStrategy, request.BaseURI, newVersion, request.ID, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), request, oldVersion, newVersion)
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

func TestIsDuplicateEntry(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "mysql duplicate entry error",
			err:  &mysql.MySQLError{Number: mysqlErrDuplicateEntry},
			want: true,
		},
		{
			name: "other mysql error",
			err:  &mysql.MySQLError{Number: 1213},
			want: false,
		},
		{
			name: "wrapped mysql duplicate entry error",
			err:  fmt.Errorf("insert failed: %w", &mysql.MySQLError{Number: mysqlErrDuplicateEntry}),
			want: true,
		},
		{
			name: "non-mysql error",
			err:  errors.New("connection reset"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDuplicateEntry(tt.err))
		})
	}
}
