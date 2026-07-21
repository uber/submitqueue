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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupRequestLogStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestLogStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestLogStore(db, testMetrics())

	return db, mock, store
}

func TestRequestLogStore_Insert(t *testing.T) {
	log := entity.RequestLog{
		RequestID:      "monorepo/1",
		TimestampMs:    1000,
		Status:         entity.RequestStatusStarted,
		RequestVersion: 1,
		LastError:      "",
		Metadata:       map[string]string{"key": "value"},
	}

	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(log.RequestID, log.TimestampMs, sqlmock.AnyArg(), log.Status, log.RequestVersion, log.LastError, sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_log").
					WithArgs(log.RequestID, log.TimestampMs, sqlmock.AnyArg(), log.Status, log.RequestVersion, log.LastError, sqlmock.AnyArg()).
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

			err := store.Insert(context.Background(), log)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestLogStore_List(t *testing.T) {
	log := entity.RequestLog{
		RequestID:      "monorepo/1",
		TimestampMs:    1000,
		Status:         entity.RequestStatusStarted,
		RequestVersion: 1,
		LastError:      "",
		Metadata:       map[string]string{},
	}

	tests := []struct {
		name      string
		requestID string
		setup     func(mock sqlmock.Sqlmock)
		want      []entity.RequestLog
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "found",
			requestID: log.RequestID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id", "timestamp_ms", "status", "request_version", "last_error", "metadata"}).
					AddRow(log.RequestID, log.TimestampMs, string(log.Status), log.RequestVersion, log.LastError, []byte(`{}`))
				mock.ExpectQuery("SELECT request_id, timestamp_ms, status, request_version, last_error, metadata FROM request_log").
					WithArgs(log.RequestID).
					WillReturnRows(rows)
			},
			want: []entity.RequestLog{log},
		},
		{
			name:      "no rows returns ErrNotFound",
			requestID: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id", "timestamp_ms", "status", "request_version", "last_error", "metadata"})
				mock.ExpectQuery("SELECT request_id, timestamp_ms, status, request_version, last_error, metadata FROM request_log").
					WithArgs("missing").
					WillReturnRows(rows)
			},
			wantErr:   true,
			wantErrIs: errs.ErrNotFound,
		},
		{
			name:      "query error",
			requestID: "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id, timestamp_ms, status, request_version, last_error, metadata FROM request_log").
					WithArgs("bad").
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

			got, err := store.List(context.Background(), tt.requestID)
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
