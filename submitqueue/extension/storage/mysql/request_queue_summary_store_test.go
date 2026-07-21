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
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupRequestQueueSummaryStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestQueueSummaryStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestQueueSummaryStore(db, testMetrics())

	return db, mock, store
}

func TestRequestQueueSummaryStore_Create(t *testing.T) {
	summary := entity.RequestQueueSummary{
		RequestID:    "monorepo/1",
		Queue:        "monorepo",
		ChangeURIs:   []string{"github://github.example.com/uber/submitqueue/pull/123/deadbeef"},
		ReceivedAtMs: 1000,
		Status:       entity.RequestStatusStarted,
		Version:      1,
		LastError:    "",
		Metadata:     map[string]string{"key": "value"},
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
				mock.ExpectExec("INSERT INTO request_summary_by_queue").
					WithArgs(summary.Queue, summary.ReceivedAtMs, summary.RequestID, sqlmock.AnyArg(),
						summary.Status, summary.Version, summary.LastError, sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate key returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_summary_by_queue").
					WithArgs(summary.Queue, summary.ReceivedAtMs, summary.RequestID, sqlmock.AnyArg(),
						summary.Status, summary.Version, summary.LastError, sqlmock.AnyArg()).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_summary_by_queue").
					WithArgs(summary.Queue, summary.ReceivedAtMs, summary.RequestID, sqlmock.AnyArg(),
						summary.Status, summary.Version, summary.LastError, sqlmock.AnyArg()).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestQueueSummaryStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), summary)
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

func TestRequestQueueSummaryStore_Get(t *testing.T) {
	want := entity.RequestQueueSummary{
		RequestID:    "monorepo/1",
		Queue:        "monorepo",
		ChangeURIs:   []string{"github://github.example.com/uber/submitqueue/pull/123/deadbeef"},
		ReceivedAtMs: 1000,
		Status:       entity.RequestStatusStarted,
		Version:      1,
		LastError:    "",
		Metadata:     map[string]string{"key": "value"},
	}

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.RequestQueueSummary
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "received_at_ms", "request_id", "change_uris", "status", "version", "last_error", "metadata"}).
					AddRow(want.Queue, want.ReceivedAtMs, want.RequestID, []byte(`["github://github.example.com/uber/submitqueue/pull/123/deadbeef"]`),
						string(want.Status), want.Version, want.LastError, []byte(`{"key":"value"}`))
				mock.ExpectQuery("SELECT queue, received_at_ms, request_id, change_uris, status").
					WithArgs(want.Queue, want.ReceivedAtMs, want.RequestID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, received_at_ms, request_id, change_uris, status").
					WithArgs("monorepo", int64(1000), "missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: errs.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestQueueSummaryStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			requestID := want.RequestID
			if tt.name == "not found" {
				requestID = "missing"
			}

			got, err := store.Get(context.Background(), "monorepo", 1000, requestID)
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

func TestRequestQueueSummaryStore_Update(t *testing.T) {
	summary := entity.RequestQueueSummary{
		RequestID:    "monorepo/1",
		Queue:        "monorepo",
		ReceivedAtMs: 1000,
		Status:       entity.RequestStatusValidated,
		LastError:    "",
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
				mock.ExpectExec("UPDATE request_summary_by_queue").
					WithArgs(summary.Status, newVersion, summary.LastError, sqlmock.AnyArg(),
						summary.Queue, summary.ReceivedAtMs, summary.RequestID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request_summary_by_queue").
					WithArgs(summary.Status, newVersion, summary.LastError, sqlmock.AnyArg(),
						summary.Queue, summary.ReceivedAtMs, summary.RequestID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: errs.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE request_summary_by_queue").
					WithArgs(summary.Status, newVersion, summary.LastError, sqlmock.AnyArg(),
						summary.Queue, summary.ReceivedAtMs, summary.RequestID, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestQueueSummaryStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), summary, oldVersion, newVersion)
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

func TestRequestQueueSummaryStore_List(t *testing.T) {
	summary := entity.RequestQueueSummary{
		RequestID:    "monorepo/1",
		Queue:        "monorepo",
		ReceivedAtMs: 1000,
		Status:       entity.RequestStatusStarted,
		Version:      1,
	}

	tests := []struct {
		name    string
		query   storage.RequestQueueSummaryQuery
		setup   func(mock sqlmock.Sqlmock)
		want    []entity.RequestQueueSummary
		wantErr bool
	}{
		{
			name: "without cursor",
			query: storage.RequestQueueSummaryQuery{
				Queue:               "monorepo",
				ReceivedAtOrAfterMs: 0,
				ReceivedBeforeMs:    2000,
				Limit:               10,
			},
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "received_at_ms", "request_id", "change_uris", "status", "version", "last_error", "metadata"}).
					AddRow(summary.Queue, summary.ReceivedAtMs, summary.RequestID, []byte(`[]`), string(summary.Status), summary.Version, "", []byte(`{}`))
				mock.ExpectQuery("SELECT queue, received_at_ms, request_id, change_uris, status").
					WithArgs("monorepo", int64(0), int64(2000), 10).
					WillReturnRows(rows)
			},
			want: []entity.RequestQueueSummary{{
				RequestID:    summary.RequestID,
				Queue:        summary.Queue,
				ChangeURIs:   []string{},
				ReceivedAtMs: summary.ReceivedAtMs,
				Status:       summary.Status,
				Version:      summary.Version,
				LastError:    "",
				Metadata:     map[string]string{},
			}},
		},
		{
			name: "with cursor",
			query: storage.RequestQueueSummaryQuery{
				Queue:               "monorepo",
				ReceivedAtOrAfterMs: 0,
				ReceivedBeforeMs:    2000,
				Limit:               10,
				HasCursor:           true,
				Cursor: storage.RequestQueueSummaryCursor{
					ReceivedAtMs: 1500,
					RequestID:    "monorepo/2",
				},
			},
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "received_at_ms", "request_id", "change_uris", "status", "version", "last_error", "metadata"})
				mock.ExpectQuery("SELECT queue, received_at_ms, request_id, change_uris, status").
					WithArgs("monorepo", int64(0), int64(2000), int64(1500), int64(1500), "monorepo/2", 10).
					WillReturnRows(rows)
			},
			want: []entity.RequestQueueSummary{},
		},
		{
			name: "query error",
			query: storage.RequestQueueSummaryQuery{
				Queue:               "monorepo",
				ReceivedAtOrAfterMs: 0,
				ReceivedBeforeMs:    2000,
				Limit:               10,
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, received_at_ms, request_id, change_uris, status").
					WithArgs("monorepo", int64(0), int64(2000), 10).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestQueueSummaryStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.List(context.Background(), tt.query)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
