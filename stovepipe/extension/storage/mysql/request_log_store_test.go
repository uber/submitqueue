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

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	testLogQueue     = "monorepo/main"
	testLogRequestID = "request/monorepo/main/1"
)

var requestLogColumnNames = []string{
	"queue", "request_id", "log_id", "timestamp_ms", "state", "event",
	"request_version", "outcome_reason", "metadata",
}

func setupRequestLogStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestLogStore) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewRequestLogStore(db, testMetrics(), testLogQueue)
}

func acceptedRequestLog() entity.RequestLog {
	return entity.RequestLog{
		ID:             "state/1",
		Queue:          testLogQueue,
		RequestID:      testLogRequestID,
		TimestampMs:    1735689600000,
		State:          entity.RequestStateAccepted,
		RequestVersion: 1,
		Metadata:       map[string]string{"source": "test"},
	}
}

func requestLogMetadataJSON(t *testing.T, entry entity.RequestLog) []byte {
	t.Helper()
	metadata := entry.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)
	return metadataJSON
}

func requestLogRow(t *testing.T, entry entity.RequestLog) *sqlmock.Rows {
	t.Helper()
	return requestLogRowWithMetadata(entry, requestLogMetadataJSON(t, entry))
}

func requestLogRowWithMetadata(entry entity.RequestLog, metadata any) *sqlmock.Rows {
	return sqlmock.NewRows(requestLogColumnNames).AddRow(
		entry.Queue,
		entry.RequestID,
		entry.ID,
		entry.TimestampMs,
		entry.State,
		entry.Event,
		entry.RequestVersion,
		entry.OutcomeReason,
		metadata,
	)
}

func TestRequestLogStoreCreate(t *testing.T) {
	entry := acceptedRequestLog()
	metadataJSON, err := json.Marshal(entry.Metadata)
	require.NoError(t, err)
	emptyMetadata := entry
	emptyMetadata.ID = "state/empty-metadata"
	emptyMetadata.Metadata = nil

	tests := []struct {
		name      string
		entry     entity.RequestLog
		setup     func(sqlmock.Sqlmock)
		wantErrIs error
		wantErr   bool
	}{
		{
			name:  "success",
			entry: entry,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(entry.Queue, entry.RequestID, entry.ID, entry.TimestampMs, entry.State, entry.Event, entry.RequestVersion, entry.OutcomeReason, metadataJSON).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:  "nil metadata normalized",
			entry: emptyMetadata,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(emptyMetadata.Queue, emptyMetadata.RequestID, emptyMetadata.ID, emptyMetadata.TimestampMs, emptyMetadata.State, emptyMetadata.Event, emptyMetadata.RequestVersion, emptyMetadata.OutcomeReason, []byte("{}")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:  "duplicate identity",
			entry: entry,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(entry.Queue, entry.RequestID, entry.ID, entry.TimestampMs, entry.State, entry.Event, entry.RequestVersion, entry.OutcomeReason, metadataJSON).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name:  "database failure",
			entry: entry,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(entry.Queue, entry.RequestID, entry.ID, entry.TimestampMs, entry.State, entry.Event, entry.RequestVersion, entry.OutcomeReason, metadataJSON).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "invalid entry",
			entry: func() entity.RequestLog {
				invalid := entry
				invalid.State = entity.RequestStateUnknown
				return invalid
			}(),
			wantErr: true,
		},
		{
			name: "wrong queue",
			entry: func() entity.RequestLog {
				wrongQueue := entry
				wrongQueue.Queue = "other"
				return wrongQueue
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestLogStoreTest(t)
			defer db.Close()
			if tt.setup != nil {
				tt.setup(mock)
			}

			err := store.Create(context.Background(), tt.entry)
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

func TestRequestLogStoreGet(t *testing.T) {
	future := acceptedRequestLog()
	future.State = entity.RequestState("future_state")
	emptyMetadata := future
	emptyMetadata.Metadata = map[string]string{}

	tests := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		want      entity.RequestLog
		wantErrIs error
		wantErr   bool
	}{
		{
			name: "found without validating future vocabulary",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, log_id, timestamp_ms, state, event").
					WithArgs(testLogQueue, future.RequestID, future.ID).
					WillReturnRows(requestLogRow(t, future))
			},
			want: future,
		},
		{
			name: "JSON null normalized to empty metadata",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, log_id, timestamp_ms, state, event").
					WithArgs(testLogQueue, future.RequestID, future.ID).
					WillReturnRows(requestLogRowWithMetadata(future, []byte("null")))
			},
			want: emptyMetadata,
		},
		{
			name: "invalid metadata value type",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, log_id, timestamp_ms, state, event").
					WithArgs(testLogQueue, future.RequestID, future.ID).
					WillReturnRows(requestLogRowWithMetadata(future, []byte(`{"attempt":2}`)))
			},
			wantErr: true,
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, log_id, timestamp_ms, state, event").
					WithArgs(testLogQueue, future.RequestID, future.ID).
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "database failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, log_id, timestamp_ms, state, event").
					WithArgs(testLogQueue, future.RequestID, future.ID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestLogStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.Get(context.Background(), future.RequestID, future.ID)
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

func TestRequestLogStoreList(t *testing.T) {
	first := acceptedRequestLog()
	second := first
	second.ID = "state/2"
	second.State = entity.RequestStateProcessing
	second.RequestVersion = 2

	tests := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		want      []entity.RequestLog
		wantErrIs error
		wantErr   bool
	}{
		{
			name: "all records",
			setup: func(mock sqlmock.Sqlmock) {
				rows := requestLogRow(t, first).AddRow(second.Queue, second.RequestID, second.ID, second.TimestampMs, second.State, second.Event, second.RequestVersion, second.OutcomeReason, requestLogMetadataJSON(t, second))
				mock.ExpectQuery("ORDER BY timestamp_ms ASC, log_id ASC").
					WithArgs(testLogQueue, testLogRequestID).
					WillReturnRows(rows)
			},
			want: []entity.RequestLog{first, second},
		},
		{
			name: "empty log",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("ORDER BY timestamp_ms ASC, log_id ASC").
					WithArgs(testLogQueue, testLogRequestID).
					WillReturnRows(sqlmock.NewRows(requestLogColumnNames))
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "database failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("ORDER BY timestamp_ms ASC, log_id ASC").
					WithArgs(testLogQueue, testLogRequestID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestLogStoreTest(t)
			defer db.Close()
			if tt.setup != nil {
				tt.setup(mock)
			}

			got, err := store.List(context.Background(), testLogRequestID)
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
