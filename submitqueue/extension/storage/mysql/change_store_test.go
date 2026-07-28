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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupChangeStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.ChangeStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewChangeStore(db, testMetrics())

	return db, mock, store
}

func TestChangeStore_Create(t *testing.T) {
	record := entity.ChangeRecord{
		URI:       "github://github.example.com/uber/submitqueue/pull/123/deadbeef",
		RequestID: "monorepo/1",
		Queue:     "monorepo",
		CreatedAt: 1000,
		UpdatedAt: 1000,
		Version:   1,
	}

	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT IGNORE INTO `change`").
					WithArgs(record.URI, record.RequestID, record.Queue, sqlmock.AnyArg(), record.CreatedAt, record.UpdatedAt, record.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "redelivery is a no-op via INSERT IGNORE",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT IGNORE INTO `change`").
					WithArgs(record.URI, record.RequestID, record.Queue, sqlmock.AnyArg(), record.CreatedAt, record.UpdatedAt, record.Version).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT IGNORE INTO `change`").
					WithArgs(record.URI, record.RequestID, record.Queue, sqlmock.AnyArg(), record.CreatedAt, record.UpdatedAt, record.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupChangeStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), record)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestChangeStore_GetByURI(t *testing.T) {
	record := entity.ChangeRecord{
		URI:       "github://github.example.com/uber/submitqueue/pull/123/deadbeef",
		RequestID: "monorepo/1",
		Queue:     "monorepo",
		CreatedAt: 1000,
		UpdatedAt: 1000,
		Version:   1,
	}

	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		want    []entity.ChangeRecord
		wantErr bool
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"uri", "request_id", "queue", "details", "created_at", "updated_at", "version"}).
					AddRow(record.URI, record.RequestID, record.Queue, []byte(`{"author":{},"changed_files":null}`), record.CreatedAt, record.UpdatedAt, record.Version)
				mock.ExpectQuery("SELECT uri, request_id, queue, details, created_at, updated_at, version FROM `change`").
					WithArgs(record.Queue, record.URI).
					WillReturnRows(rows)
			},
			want: []entity.ChangeRecord{record},
		},
		{
			name: "no rows returns empty slice",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"uri", "request_id", "queue", "details", "created_at", "updated_at", "version"})
				mock.ExpectQuery("SELECT uri, request_id, queue, details, created_at, updated_at, version FROM `change`").
					WithArgs(record.Queue, record.URI).
					WillReturnRows(rows)
			},
			want: nil,
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT uri, request_id, queue, details, created_at, updated_at, version FROM `change`").
					WithArgs(record.Queue, record.URI).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "malformed details JSON",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"uri", "request_id", "queue", "details", "created_at", "updated_at", "version"}).
					AddRow(record.URI, record.RequestID, record.Queue, []byte("not json"), record.CreatedAt, record.UpdatedAt, record.Version)
				mock.ExpectQuery("SELECT uri, request_id, queue, details, created_at, updated_at, version FROM `change`").
					WithArgs(record.Queue, record.URI).
					WillReturnRows(rows)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupChangeStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.GetByURI(context.Background(), record.Queue, record.URI)
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
