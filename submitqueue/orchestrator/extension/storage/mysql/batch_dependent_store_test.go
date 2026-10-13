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

func setupBatchDependentStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.BatchDependentStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewBatchDependentStore(db, testMetrics(), "monorepo")

	return db, mock, store
}

func TestBatchDependentStore_Get(t *testing.T) {
	want := entity.BatchDependent{
		BatchID:    "1",
		Dependents: []string{"2", "3"},
		Version:    1,
	}
	dependentsJSON, err := json.Marshal(want.Dependents)
	require.NoError(t, err)

	tests := []struct {
		name      string
		batchID   string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.BatchDependent
		wantErr   bool
		wantErrIs error
	}{
		{
			name:    "found",
			batchID: want.BatchID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"batch_id", "dependents", "version"}).
					AddRow(want.BatchID, dependentsJSON, want.Version)
				mock.ExpectQuery("SELECT batch_id, dependents, version FROM batch_dependent").
					WithArgs("monorepo", want.BatchID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name:    "found with nil dependents",
			batchID: "10",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"batch_id", "dependents", "version"}).
					AddRow("10", []byte("null"), int32(2))
				mock.ExpectQuery("SELECT batch_id, dependents, version FROM batch_dependent").
					WithArgs("monorepo", "10").
					WillReturnRows(rows)
			},
			want: entity.BatchDependent{
				BatchID: "10",
				Version: 2,
			},
		},
		{
			name:    "found with empty dependents",
			batchID: "11",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"batch_id", "dependents", "version"}).
					AddRow("11", []byte("[]"), int32(3))
				mock.ExpectQuery("SELECT batch_id, dependents, version FROM batch_dependent").
					WithArgs("monorepo", "11").
					WillReturnRows(rows)
			},
			want: entity.BatchDependent{
				BatchID:    "11",
				Dependents: []string{},
				Version:    3,
			},
		},
		{
			name:    "not found",
			batchID: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT batch_id, dependents, version FROM batch_dependent").
					WithArgs("monorepo", "missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name:    "query error",
			batchID: "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT batch_id, dependents, version FROM batch_dependent").
					WithArgs("monorepo", "bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchDependentStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), tt.batchID)
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

func TestBatchDependentStore_Create(t *testing.T) {
	bd := entity.BatchDependent{
		BatchID:    "1",
		Dependents: []string{"2"},
		Version:    1,
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
				mock.ExpectExec("INSERT INTO batch_dependent").
					WithArgs("monorepo", bd.BatchID, sqlmock.AnyArg(), bd.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate batch id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO batch_dependent").
					WithArgs("monorepo", bd.BatchID, sqlmock.AnyArg(), bd.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO batch_dependent").
					WithArgs("monorepo", bd.BatchID, sqlmock.AnyArg(), bd.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchDependentStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), bd)
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

func TestBatchDependentStore_Update(t *testing.T) {
	const oldVersion, newVersion = int32(1), int32(2)
	batchDependent := entity.BatchDependent{
		BatchID:    "1",
		Dependents: []string{"2", "3"},
		Version:    oldVersion,
	}

	tests := []struct {
		name      string
		entity    entity.BatchDependent
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:   "success",
			entity: batchDependent,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte(`["2","3"]`), newVersion, "monorepo", batchDependent.BatchID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "success with nil dependents",
			entity: entity.BatchDependent{
				BatchID: "10",
				Version: oldVersion,
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte("null"), newVersion, "monorepo", "10", oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "success with empty dependents",
			entity: entity.BatchDependent{
				BatchID:    "11",
				Dependents: []string{},
				Version:    oldVersion,
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte("[]"), newVersion, "monorepo", "11", oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:   "version mismatch",
			entity: batchDependent,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte(`["2","3"]`), newVersion, "monorepo", batchDependent.BatchID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name:   "exec error",
			entity: batchDependent,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte(`["2","3"]`), newVersion, "monorepo", batchDependent.BatchID, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name:   "rows affected error",
			entity: batchDependent,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE batch_dependent").
					WithArgs([]byte(`["2","3"]`), newVersion, "monorepo", batchDependent.BatchID, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupBatchDependentStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), tt.entity, oldVersion, newVersion)
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
