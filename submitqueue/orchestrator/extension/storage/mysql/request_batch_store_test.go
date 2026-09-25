// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupRequestBatchStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestBatchStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewRequestBatchStore(db, testMetrics(), "monorepo")
}

func TestRequestBatchStore_GetByRequestID(t *testing.T) {
	association1 := entity.RequestBatch{
		RequestID: "monorepo/1",
		BatchID:   "monorepo/batch/1",
		Version:   1,
	}
	association2 := entity.RequestBatch{
		RequestID: association1.RequestID,
		BatchID:   "monorepo/batch/2",
		Version:   1,
	}
	storeErr := errors.New("storage failed")
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		want   []entity.RequestBatch
		errMsg string
	}{
		"query fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs("monorepo", association1.RequestID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"no associations": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs("monorepo", association1.RequestID).
					WillReturnRows(sqlmock.NewRows([]string{"request_id", "batch_id", "version"}))
			},
		},
		"scan fails": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id", "batch_id", "version"}).
					AddRow(association1.RequestID, association1.BatchID, "invalid")
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs("monorepo", association1.RequestID).
					WillReturnRows(rows)
			},
			errMsg: "failed to scan request batch association",
		},
		"row iteration fails": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id", "batch_id", "version"}).
					AddRow(association1.RequestID, association1.BatchID, association1.Version).
					RowError(0, storeErr)
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs("monorepo", association1.RequestID).
					WillReturnRows(rows)
			},
			errMsg: storeErr.Error(),
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id", "batch_id", "version"}).
					AddRow(association1.RequestID, association1.BatchID, association1.Version).
					AddRow(association2.RequestID, association2.BatchID, association2.Version)
				mock.ExpectQuery("SELECT request_id, batch_id, version FROM request_batch").
					WithArgs("monorepo", association1.RequestID).
					WillReturnRows(rows)
			},
			want: []entity.RequestBatch{association1, association2},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupRequestBatchStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.GetByRequestID(context.Background(), association1.RequestID)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestBatchStore_Create(t *testing.T) {
	association := entity.RequestBatch{
		RequestID: "monorepo/1",
		BatchID:   "monorepo/batch/2",
		Version:   1,
	}
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		errMsg string
	}{
		"duplicate association returns already exists": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs("monorepo", association.RequestID, association.BatchID, association.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			errMsg: storage.ErrAlreadyExists.Error(),
		},
		"insert fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs("monorepo", association.RequestID, association.BatchID, association.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_batch").
					WithArgs("monorepo", association.RequestID, association.BatchID, association.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupRequestBatchStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Create(context.Background(), association)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
