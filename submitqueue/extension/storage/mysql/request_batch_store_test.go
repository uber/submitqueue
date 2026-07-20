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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupRequestBatchStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestBatchStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewRequestBatchStore(db, testMetrics())
}

func TestRequestBatchStore_Get(t *testing.T) {
	want := entity.RequestBatch{
		RequestID: "monorepo/1",
		BatchID:   "monorepo/batch/7",
		Version:   1,
	}

	tests := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs(want.RequestID).
					WillReturnRows(sqlmock.NewRows([]string{"request_id", "batch_id", "version"}).
						AddRow(want.RequestID, want.BatchID, want.Version))
			},
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs(want.RequestID).
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs(want.RequestID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestBatchStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.Get(context.Background(), want.RequestID)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestBatchStore_Create(t *testing.T) {
	requestBatch := entity.RequestBatch{
		RequestID: "monorepo/1",
		BatchID:   "monorepo/batch/7",
		Version:   1,
	}

	tests := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs(requestBatch.RequestID, requestBatch.BatchID, requestBatch.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate request returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs(requestBatch.RequestID, requestBatch.BatchID, requestBatch.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs(requestBatch.RequestID, requestBatch.BatchID, requestBatch.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestBatchStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Create(context.Background(), requestBatch)
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
