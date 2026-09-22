// Copyright (c) 2026 Uber Technologies, Inc.
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

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const testRequestSummaryQueue = "monorepo/main"

func testRequestSummary() entity.RequestSummary {
	return entity.RequestSummary{
		RequestID:        "request/monorepo/main/1",
		Queue:            testRequestSummaryQueue,
		URI:              "git://repo/head",
		BaseURI:          "git://repo/base",
		State:            entity.RequestStateProcessing,
		RequestVersion:   2,
		StateTimestampMs: 1735689600000,
		Version:          1,
	}
}

func setupRequestSummaryStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestSummaryStore) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return db, mock, NewRequestSummaryStore(db, testMetrics(), testRequestSummaryQueue)
}

func TestRequestSummaryStoreCreate(t *testing.T) {
	summary := testRequestSummary()
	tests := []struct {
		name      string
		summary   entity.RequestSummary
		setup     func(sqlmock.Sqlmock)
		wantErrIs error
		wantErr   bool
	}{
		{
			name:    "success",
			summary: summary,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_summary").
					WithArgs(summary.Queue, summary.RequestID, summary.URI, summary.BaseURI, summary.State, summary.RequestVersion, summary.StateTimestampMs, summary.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name:    "duplicate",
			summary: summary,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_summary").
					WithArgs(summary.Queue, summary.RequestID, summary.URI, summary.BaseURI, summary.State, summary.RequestVersion, summary.StateTimestampMs, summary.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "wrong queue",
			summary: func() entity.RequestSummary {
				other := summary
				other.Queue = "other"
				return other
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestSummaryStoreTest(t)
			defer db.Close()
			if tt.setup != nil {
				tt.setup(mock)
			}

			err := store.Create(context.Background(), tt.summary)
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

func TestRequestSummaryStoreGet(t *testing.T) {
	want := testRequestSummary()
	tests := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		wantErrIs error
		wantErr   bool
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "request_id", "uri", "base_uri", "state", "request_version", "state_timestamp_ms", "version"}).
					AddRow(want.Queue, want.RequestID, want.URI, want.BaseURI, want.State, want.RequestVersion, want.StateTimestampMs, want.Version)
				mock.ExpectQuery("SELECT queue, request_id, uri, base_uri, state").WithArgs(testRequestSummaryQueue, want.RequestID).WillReturnRows(rows)
			},
		},
		{
			name: "not found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, request_id, uri, base_uri, state").WithArgs(testRequestSummaryQueue, want.RequestID).WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestSummaryStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, err := store.Get(context.Background(), want.RequestID)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
				assert.Equal(t, want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestSummaryStoreUpdate(t *testing.T) {
	summary := testRequestSummary()
	const oldVersion, newVersion = int32(1), int32(2)
	tests := []struct {
		name      string
		result    sql.Result
		execErr   error
		wantErrIs error
		wantErr   bool
	}{
		{name: "success", result: sqlmock.NewResult(0, 1)},
		{name: "version mismatch", result: sqlmock.NewResult(0, 0), wantErr: true, wantErrIs: storage.ErrVersionMismatch},
		{name: "database failure", execErr: fmt.Errorf("connection reset"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestSummaryStoreTest(t)
			defer db.Close()
			expectation := mock.ExpectExec("UPDATE request_summary").
				WithArgs(summary.BaseURI, summary.State, summary.RequestVersion, summary.StateTimestampMs, newVersion, summary.Queue, summary.RequestID, oldVersion)
			if tt.execErr != nil {
				expectation.WillReturnError(tt.execErr)
			} else {
				expectation.WillReturnResult(tt.result)
			}

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
