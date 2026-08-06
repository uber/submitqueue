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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupQueueBatchStateStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.QueueBatchStateStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewQueueBatchStateStore(db, testMetrics(), "monorepo")
}

func TestQueueBatchStateStore_List(t *testing.T) {
	record1 := entity.QueueBatchState{
		Queue:   "monorepo",
		State:   entity.BatchStateSpeculating,
		BatchID: "monorepo/batch/1",
	}
	record2 := entity.QueueBatchState{
		Queue:   record1.Queue,
		State:   record1.State,
		BatchID: "monorepo/batch/2",
	}
	storeErr := errors.New("storage failed")
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		want   []entity.QueueBatchState
		errMsg string
	}{
		"query fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, batch_id FROM queue_batch_state").
					WithArgs(record1.Queue, string(record1.State)).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"empty bucket": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, batch_id FROM queue_batch_state").
					WithArgs(record1.Queue, string(record1.State)).
					WillReturnRows(sqlmock.NewRows([]string{"queue", "state", "batch_id"}))
			},
		},
		"row iteration fails": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "state", "batch_id"}).
					AddRow(record1.Queue, string(record1.State), record1.BatchID).
					RowError(0, storeErr)
				mock.ExpectQuery("SELECT queue, state, batch_id FROM queue_batch_state").
					WithArgs(record1.Queue, string(record1.State)).
					WillReturnRows(rows)
			},
			errMsg: storeErr.Error(),
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "state", "batch_id"}).
					AddRow(record1.Queue, string(record1.State), record1.BatchID).
					AddRow(record2.Queue, string(record2.State), record2.BatchID)
				mock.ExpectQuery("SELECT queue, state, batch_id FROM queue_batch_state").
					WithArgs(record1.Queue, string(record1.State)).
					WillReturnRows(rows)
			},
			want: []entity.QueueBatchState{record1, record2},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueBatchStateStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.List(context.Background(), record1.State)
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

func TestQueueBatchStateStore_Put(t *testing.T) {
	record := entity.QueueBatchState{
		Queue:   "monorepo",
		State:   entity.BatchStateCreated,
		BatchID: "monorepo/batch/1",
	}
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		errMsg string
	}{
		"insert fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT IGNORE INTO queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"existing record is a no-op success": {
			setup: func(mock sqlmock.Sqlmock) {
				// INSERT IGNORE reports zero affected rows on a PK conflict.
				mock.ExpectExec("INSERT IGNORE INTO queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT IGNORE INTO queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueBatchStateStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Put(context.Background(), record)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueueBatchStateStore_Delete(t *testing.T) {
	record := entity.QueueBatchState{
		Queue:   "monorepo",
		State:   entity.BatchStateSucceeded,
		BatchID: "monorepo/batch/1",
	}
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		errMsg string
	}{
		"delete fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"absent record is a no-op success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_batch_state").
					WithArgs(record.Queue, string(record.State), record.BatchID).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueBatchStateStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Delete(context.Background(), record.State, record.BatchID)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
