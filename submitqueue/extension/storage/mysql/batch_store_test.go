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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupBatchStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.BatchStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewBatchStore(db, testMetrics())

	return db, mock, store
}

func TestBatchStore_Get(t *testing.T) {
	want := entity.Batch{
		ID:           "monorepo/batch/1",
		Queue:        "monorepo",
		Contains:     []string{"monorepo/1", "monorepo/2"},
		Dependencies: []string{"monorepo/batch/0"},
		Score:        0.9,
		State:        entity.BatchStateCreated,
		Version:      1,
	}
	containsJSON, err := json.Marshal(want.Contains)
	require.NoError(t, err)
	dependenciesJSON, err := json.Marshal(want.Dependencies)
	require.NoError(t, err)

	tests := []struct {
		name      string
		id        string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.Batch
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			id:   want.ID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "queue", "contains", "dependencies", "score", "state", "version"}).
					AddRow(want.ID, want.Queue, containsJSON, dependenciesJSON, want.Score, string(want.State), want.Version)
				mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
					WithArgs(want.ID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			id:   "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
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
				mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "malformed contains JSON",
			id:   "malformed",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "queue", "contains", "dependencies", "score", "state", "version"}).
					AddRow(want.ID, want.Queue, []byte("not json"), dependenciesJSON, want.Score, string(want.State), want.Version)
				mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
					WithArgs("malformed").
					WillReturnRows(rows)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchStoreTest(t)
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

func TestBatchStore_Create(t *testing.T) {
	batch := entity.Batch{
		ID:           "monorepo/batch/1",
		Queue:        "monorepo",
		Contains:     []string{"monorepo/1"},
		Dependencies: nil,
		Score:        0,
		State:        entity.BatchStateCreated,
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
				mock.ExpectExec("INSERT INTO batch").
					WithArgs(batch.ID, batch.Queue, sqlmock.AnyArg(), sqlmock.AnyArg(), batch.Score, batch.State, batch.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO batch").
					WithArgs(batch.ID, batch.Queue, sqlmock.AnyArg(), sqlmock.AnyArg(), batch.Score, batch.State, batch.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO batch").
					WithArgs(batch.ID, batch.Queue, sqlmock.AnyArg(), sqlmock.AnyArg(), batch.Score, batch.State, batch.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), batch)
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

func TestBatchStore_UpdateState(t *testing.T) {
	const id = "monorepo/batch/1"
	const oldVersion, newVersion = int32(1), int32(2)
	const newState = entity.BatchStateMerging

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchStoreTest(t)
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

func TestBatchStore_UpdateScoreAndState(t *testing.T) {
	const id = "monorepo/batch/1"
	const oldVersion, newVersion = int32(1), int32(2)
	const newState = entity.BatchStateScored
	const score = 0.75

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(score, newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(score, newState, newVersion, id, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch").
					WithArgs(score, newState, newVersion, id, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.UpdateScoreAndState(context.Background(), id, oldVersion, newVersion, score, newState)
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

func TestBatchStore_GetByQueueAndStates(t *testing.T) {
	t.Run("empty states returns nil without querying", func(t *testing.T) {
		db, mock, store := setupBatchStoreTest(t)
		defer db.Close()

		got, err := store.GetByQueueAndStates(context.Background(), "monorepo", nil)
		require.NoError(t, err)
		assert.Nil(t, got)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("found", func(t *testing.T) {
		db, mock, store := setupBatchStoreTest(t)
		defer db.Close()

		batch := entity.Batch{ID: "monorepo/batch/1", Queue: "monorepo", State: entity.BatchStateCreated, Version: 1}
		containsJSON, err := json.Marshal(batch.Contains)
		require.NoError(t, err)
		dependenciesJSON, err := json.Marshal(batch.Dependencies)
		require.NoError(t, err)

		rows := sqlmock.NewRows([]string{"id", "queue", "contains", "dependencies", "score", "state", "version"}).
			AddRow(batch.ID, batch.Queue, containsJSON, dependenciesJSON, batch.Score, string(batch.State), batch.Version)
		mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
			WithArgs("monorepo", entity.BatchStateCreated, entity.BatchStateMerging).
			WillReturnRows(rows)

		got, err := store.GetByQueueAndStates(context.Background(), "monorepo", []entity.BatchState{entity.BatchStateCreated, entity.BatchStateMerging})
		require.NoError(t, err)
		assert.Equal(t, []entity.Batch{batch}, got)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query error", func(t *testing.T) {
		db, mock, store := setupBatchStoreTest(t)
		defer db.Close()

		mock.ExpectQuery("SELECT id, queue, contains, dependencies, score, state, version FROM batch").
			WithArgs("monorepo", entity.BatchStateCreated).
			WillReturnError(fmt.Errorf("connection reset"))

		_, err := store.GetByQueueAndStates(context.Background(), "monorepo", []entity.BatchState{entity.BatchStateCreated})
		require.Error(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
