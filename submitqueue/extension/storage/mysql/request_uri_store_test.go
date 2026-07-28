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

func setupRequestURIStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestURIStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestURIStore(db, testMetrics())

	return db, mock, store
}

func TestRequestURIStore_Create(t *testing.T) {
	mapping := entity.RequestURI{
		ChangeURI:    "github://github.example.com/uber/submitqueue/pull/123/deadbeef",
		ReceivedAtMs: 1000,
		RequestID:    "monorepo/1",
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
				mock.ExpectExec("INSERT INTO change_uri_request_mapping").
					WithArgs(mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate mapping returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO change_uri_request_mapping").
					WithArgs(mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO change_uri_request_mapping").
					WithArgs(mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestURIStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), mapping)
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

func TestRequestURIStore_ListByURI(t *testing.T) {
	mapping := entity.RequestURI{
		ChangeURI:    "github://github.example.com/uber/submitqueue/pull/123/deadbeef",
		ReceivedAtMs: 1000,
		RequestID:    "monorepo/1",
	}

	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		want    []entity.RequestURI
		wantErr bool
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"change_uri", "received_at_ms", "request_id"}).
					AddRow(mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID)
				mock.ExpectQuery("SELECT change_uri, received_at_ms, request_id").
					WithArgs(mapping.ChangeURI, 10).
					WillReturnRows(rows)
			},
			want: []entity.RequestURI{mapping},
		},
		{
			name: "no rows returns empty slice",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"change_uri", "received_at_ms", "request_id"})
				mock.ExpectQuery("SELECT change_uri, received_at_ms, request_id").
					WithArgs(mapping.ChangeURI, 10).
					WillReturnRows(rows)
			},
			want: []entity.RequestURI{},
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT change_uri, received_at_ms, request_id").
					WithArgs(mapping.ChangeURI, 10).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupRequestURIStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.ListByURI(context.Background(), mapping.ChangeURI, 10)
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
