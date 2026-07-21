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
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func setupRequestURIStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.RequestURIStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewRequestURIStore(db, testMetrics())

	return db, mock, store
}

func TestRequestURIStore_Create(t *testing.T) {
	const queue, uri, id = "monorepo/main", "git://remote/monorepo/main/deadbeef", "request/monorepo/main/1"

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_uri").
					WithArgs(queue, uri, id, requestURIInitialVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate mapping returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_uri").
					WithArgs(queue, uri, id, requestURIInitialVersion).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO request_uri").
					WithArgs(queue, uri, id, requestURIInitialVersion).
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

			err := store.Create(context.Background(), queue, uri, id)
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

func TestRequestURIStore_GetIDByURI(t *testing.T) {
	const queue, uri, wantID = "monorepo/main", "git://remote/monorepo/main/deadbeef", "request/monorepo/main/1"

	tests := []struct {
		name      string
		queue     string
		uri       string
		setup     func(mock sqlmock.Sqlmock)
		want      string
		wantErr   bool
		wantErrIs error
	}{
		{
			name:  "found",
			queue: queue,
			uri:   uri,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"request_id"}).AddRow(wantID)
				mock.ExpectQuery("SELECT request_id FROM request_uri").
					WithArgs(queue, uri).
					WillReturnRows(rows)
			},
			want: wantID,
		},
		{
			name:  "not found",
			queue: queue,
			uri:   "git://remote/monorepo/main/missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id FROM request_uri").
					WithArgs(queue, "git://remote/monorepo/main/missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: errs.ErrNotFound,
		},
		{
			name:  "query error",
			queue: queue,
			uri:   "git://remote/monorepo/main/bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT request_id FROM request_uri").
					WithArgs(queue, "git://remote/monorepo/main/bad").
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

			got, err := store.GetIDByURI(context.Background(), tt.queue, tt.uri)
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
