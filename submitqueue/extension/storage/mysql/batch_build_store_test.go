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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func TestBatchBuildStore_CreateAndGet(t *testing.T) {
	mapping := entity.BatchBuild{BatchID: "queue/batch/1", BuildID: "build-1"}

	tests := []struct {
		name string
		run  func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore)
	}{
		{
			name: "create",
			run: func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore) {
				mock.ExpectExec("INSERT INTO batch_build").
					WithArgs(mapping.BatchID, mapping.BuildID).
					WillReturnResult(sqlmock.NewResult(0, 1))
				require.NoError(t, store.Create(context.Background(), mapping))
			},
		},
		{
			name: "create duplicate",
			run: func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore) {
				mock.ExpectExec("INSERT INTO batch_build").
					WithArgs(mapping.BatchID, mapping.BuildID).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
				err := store.Create(context.Background(), mapping)
				require.Error(t, err)
				assert.ErrorIs(t, err, storage.ErrAlreadyExists)
			},
		},
		{
			name: "get",
			run: func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore) {
				mock.ExpectQuery("SELECT batch_id, build_id FROM batch_build").
					WithArgs(mapping.BatchID).
					WillReturnRows(sqlmock.NewRows([]string{"batch_id", "build_id"}).AddRow(mapping.BatchID, mapping.BuildID))
				got, err := store.Get(context.Background(), mapping.BatchID)
				require.NoError(t, err)
				assert.Equal(t, mapping, got)
			},
		},
		{
			name: "get missing",
			run: func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore) {
				mock.ExpectQuery("SELECT batch_id, build_id FROM batch_build").
					WithArgs(mapping.BatchID).
					WillReturnError(sql.ErrNoRows)
				_, err := store.Get(context.Background(), mapping.BatchID)
				require.Error(t, err)
				assert.ErrorIs(t, err, storage.ErrNotFound)
			},
		},
		{
			name: "get failure",
			run: func(t *testing.T, mock sqlmock.Sqlmock, store storage.BatchBuildStore) {
				mock.ExpectQuery("SELECT batch_id, build_id FROM batch_build").
					WithArgs(mapping.BatchID).
					WillReturnError(fmt.Errorf("connection reset"))
				_, err := store.Get(context.Background(), mapping.BatchID)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.run(t, mock, NewBatchBuildStore(db, testMetrics()))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
