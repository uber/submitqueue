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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestFixStoresErrors(t *testing.T) {
	ctx := context.Background()
	dbError := errors.New("database unavailable")
	for _, scenario := range []string{"create duplicate", "create error", "get missing", "get error", "update error", "update stale", "rows affected error"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			backend, err := NewStorage(db, testMetrics())
			require.NoError(t, err)
			bound, err := backend.For("queue")
			require.NoError(t, err)
			store := bound.GetFixStore()
			fix := entity.Fix{ID: "fix", Queue: "queue"}
			var want error
			switch scenario {
			case "create duplicate":
				mock.ExpectExec("INSERT INTO fix").WillReturnError(&driver.MySQLError{Number: mysqlErrDuplicateEntry})
				want = storage.ErrAlreadyExists
				err = store.Create(ctx, fix)
			case "create error":
				mock.ExpectExec("INSERT INTO fix").WillReturnError(dbError)
				want = dbError
				err = store.Create(ctx, fix)
			case "get missing":
				mock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)
				want = storage.ErrNotFound
				_, err = store.Get(ctx, fix.ID)
			case "get error":
				mock.ExpectQuery("SELECT").WillReturnError(dbError)
				want = dbError
				_, err = store.Get(ctx, fix.ID)
			case "update error":
				mock.ExpectExec("UPDATE fix").WillReturnError(dbError)
				want = dbError
				err = store.Update(ctx, fix, 1, 2)
			case "update stale":
				mock.ExpectExec("UPDATE fix").WillReturnResult(sqlmock.NewResult(0, 0))
				want = storage.ErrVersionMismatch
				err = store.Update(ctx, fix, 1, 2)
			case "rows affected error":
				mock.ExpectExec("UPDATE fix").WillReturnResult(sqlmock.NewErrorResult(dbError))
				want = dbError
				err = store.Update(ctx, fix, 1, 2)
			}
			require.ErrorIs(t, err, want)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestFixStoresRejectInvalidWrites(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"create", "update"} {
		t.Run(operation, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			fixStore := NewFixStore(db, testMetrics(), "queue")
			for _, fix := range []entity.Fix{{Queue: "queue"}, {ID: "fix", Queue: "other"}} {
				if operation == "create" {
					err = fixStore.Create(ctx, fix)
				} else {
					err = fixStore.Update(ctx, fix, 1, 2)
				}
				require.Error(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestFixStoresRejectCorruptCollections(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct{ name, issues, metadata string }{
		{"repair issue openings", `{}`, `{}`},
		{"repair metadata", `[]`, `[]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id", "build_id", "state", "issues", "execution_ref", "metadata", "created_at", "updated_at", "version"}).
				AddRow("fix", "build-1", "queued", tt.issues, "", tt.metadata, 1, 1, 1))
			_, err = NewFixStore(db, testMetrics(), "queue").Get(ctx, "fix")
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
