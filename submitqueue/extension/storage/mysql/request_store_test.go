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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupRequestStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestStore(db, testMetrics())

	return db, mock, store
}

func TestRequestStore_Get(t *testing.T) {
	want := entity.Request{
		ID:           "monorepo/1",
		Queue:        "monorepo",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/submitqueue/pull/123/deadbeef"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	changeURIsJSON, err := json.Marshal(want.Change.URIs)
	require.NoError(t, err)

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
				rows := sqlmock.NewRows([]string{"id", "queue", "change_uri", "land_strategy", "state", "version"}).
					AddRow(want.ID, want.Queue, changeURIsJSON, string(want.LandStrategy), string(want.State), want.Version)
				mock.ExpectQuery("SELECT id, queue, change_uri, land_strategy, state, version FROM request").
					WithArgs(want.ID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			id:   "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, queue, change_uri, land_strategy, state, version FROM request").
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
				mock.ExpectQuery("SELECT id, queue, change_uri, land_strategy, state, version FROM request").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "malformed change URIs JSON",
			id:   "malformed",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "queue", "change_uri", "land_strategy", "state", "version"}).
					AddRow(want.ID, want.Queue, []byte("not json"), string(want.LandStrategy), string(want.State), want.Version)
				mock.ExpectQuery("SELECT id, queue, change_uri, land_strategy, state, version FROM request").
					WithArgs("malformed").
					WillReturnRows(rows)
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

func TestRequestStore_Create(t *testing.T) {
	request := entity.Request{
		ID:           "monorepo/1",
		Queue:        "monorepo",
		Change:       change.Change{URIs: []string{"github://github.example.com/uber/submitqueue/pull/123/deadbeef"}},
		LandStrategy: mergestrategy.MergeStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
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
					WithArgs(request.ID, request.Queue, sqlmock.AnyArg(), request.LandStrategy, request.State, request.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request").
					WithArgs(request.ID, request.Queue, sqlmock.AnyArg(), request.LandStrategy, request.State, request.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request").
					WithArgs(request.ID, request.Queue, sqlmock.AnyArg(), request.LandStrategy, request.State, request.Version).
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

func TestRequestStore_UpdateState(t *testing.T) {
	const id = "monorepo/1"
	const oldVersion, newVersion = int32(1), int32(2)
	const newState = entity.RequestStateValidated

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
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: errs.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request").
					WithArgs(newState, newVersion, id, oldVersion).
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

			err := store.UpdateState(context.Background(), id, oldVersion, newVersion, newState)
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
