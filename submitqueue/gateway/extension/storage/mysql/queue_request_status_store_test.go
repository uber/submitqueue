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

var queueRequestStatusColumns = []string{"queue", "status", "request_id", "received_at_ms", "entered_at_ms"}

func setupQueueRequestStatusStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.QueueRequestStatusStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewQueueRequestStatusStore(db, testMetrics(), "monorepo")
}

func TestQueueRequestStatusStore_List(t *testing.T) {
	record1 := entity.QueueRequestStatus{Queue: "monorepo", Status: entity.RequestStatusSpeculating, RequestID: "1", ReceivedAtMs: 10, EnteredAtMs: 20}
	record2 := entity.QueueRequestStatus{Queue: "monorepo", Status: entity.RequestStatusSpeculating, RequestID: "2", ReceivedAtMs: 11, EnteredAtMs: 21}
	storeErr := errors.New("storage failed")
	addRow := func(rows *sqlmock.Rows, rec entity.QueueRequestStatus) *sqlmock.Rows {
		return rows.AddRow(rec.Queue, string(rec.Status), rec.RequestID, rec.ReceivedAtMs, rec.EnteredAtMs)
	}
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		want   []entity.QueueRequestStatus
		errMsg string
	}{
		"query fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, status, request_id, received_at_ms, entered_at_ms FROM queue_request_status").
					WithArgs(record1.Queue, string(record1.Status)).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"empty bucket": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, status, request_id, received_at_ms, entered_at_ms FROM queue_request_status").
					WithArgs(record1.Queue, string(record1.Status)).
					WillReturnRows(sqlmock.NewRows(queueRequestStatusColumns))
			},
		},
		"row iteration fails": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := addRow(sqlmock.NewRows(queueRequestStatusColumns), record1).RowError(0, storeErr)
				mock.ExpectQuery("SELECT queue, status, request_id, received_at_ms, entered_at_ms FROM queue_request_status").
					WithArgs(record1.Queue, string(record1.Status)).
					WillReturnRows(rows)
			},
			errMsg: storeErr.Error(),
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				rows := addRow(addRow(sqlmock.NewRows(queueRequestStatusColumns), record1), record2)
				mock.ExpectQuery("SELECT queue, status, request_id, received_at_ms, entered_at_ms FROM queue_request_status").
					WithArgs(record1.Queue, string(record1.Status)).
					WillReturnRows(rows)
			},
			want: []entity.QueueRequestStatus{record1, record2},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueRequestStatusStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.List(context.Background(), record1.Status)
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

func TestQueueRequestStatusStore_Put(t *testing.T) {
	record := entity.QueueRequestStatus{Queue: "monorepo", Status: entity.RequestStatusValidating, RequestID: "1", ReceivedAtMs: 10, EnteredAtMs: 20}
	const insert = "INSERT INTO queue_request_status \\(queue, status, request_id, received_at_ms, entered_at_ms\\) VALUES \\(\\?, \\?, \\?, \\?, \\?\\) ON DUPLICATE KEY UPDATE entered_at_ms = entered_at_ms"
	tests := map[string]struct {
		record entity.QueueRequestStatus
		setup  func(sqlmock.Sqlmock)
		errMsg string
	}{
		"record for another queue is rejected": {
			record: entity.QueueRequestStatus{Queue: "other", Status: record.Status, RequestID: record.RequestID},
			setup:  func(sqlmock.Sqlmock) {},
			errMsg: "does not match the store's bound queue",
		},
		"insert fails": {
			record: record,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(insert).
					WithArgs(record.Queue, string(record.Status), record.RequestID, record.ReceivedAtMs, record.EnteredAtMs).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"existing record is a no-op success": {
			record: record,
			setup: func(mock sqlmock.Sqlmock) {
				// The no-op ON DUPLICATE KEY UPDATE reports zero affected rows on a PK conflict.
				mock.ExpectExec(insert).
					WithArgs(record.Queue, string(record.Status), record.RequestID, record.ReceivedAtMs, record.EnteredAtMs).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		"success": {
			record: record,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(insert).
					WithArgs(record.Queue, string(record.Status), record.RequestID, record.ReceivedAtMs, record.EnteredAtMs).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueRequestStatusStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Put(context.Background(), tt.record)
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueueRequestStatusStore_Delete(t *testing.T) {
	tests := map[string]struct {
		setup  func(sqlmock.Sqlmock)
		errMsg string
	}{
		"delete fails": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_request_status").
					WithArgs("monorepo", string(entity.RequestStatusStarted), "1").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			errMsg: "connection reset",
		},
		"absent record is a no-op success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_request_status").
					WithArgs("monorepo", string(entity.RequestStatusStarted), "1").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		"success": {
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_request_status").
					WithArgs("monorepo", string(entity.RequestStatusStarted), "1").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, store := setupQueueRequestStatusStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Delete(context.Background(), entity.RequestStatusStarted, "1")
			if tt.errMsg != "" {
				assert.ErrorContains(t, err, tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
