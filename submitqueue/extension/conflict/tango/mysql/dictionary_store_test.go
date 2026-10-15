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

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupDictionaryStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, tango.DictionaryStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	return db, mock, NewDictionaryStore(db, testMetrics(), "monorepo")
}

func TestTargetDictionaryStore_Create(t *testing.T) {
	label := tango.TargetLabel{Queue: "monorepo", ID: 42, Label: "//app/server:lib", CreatedAt: 1700000000000}
	mismatched := label
	mismatched.Queue = "other-queue"

	tests := []struct {
		name      string
		label     tango.TargetLabel
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name:  "success",
			label: label,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_dictionary").
					WithArgs("monorepo", uint64(42), label.Label, label.CreatedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			name:  "id already recorded",
			label: label,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_dictionary").
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name:  "insert error",
			label: label,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO target_dictionary").
					WillReturnError(fmt.Errorf("data too long for column label"))
			},
			wantErr: true,
		},
		{
			name:    "queue differs from the bound queue",
			label:   mismatched,
			setup:   func(mock sqlmock.Sqlmock) {},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupDictionaryStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			err := store.Create(context.Background(), tt.label)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}
			} else {
				require.NoError(t, err)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTargetDictionaryStore_Get(t *testing.T) {
	columns := []string{"label", "created_at"}

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		want      tango.TargetLabel
		wantFound bool
		wantErr   bool
	}{
		{
			name: "found",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT label, created_at FROM target_dictionary").
					WithArgs("monorepo", uint64(42)).
					WillReturnRows(sqlmock.NewRows(columns).AddRow("//app:lib", int64(10)))
			},
			want:      tango.TargetLabel{Queue: "monorepo", ID: 42, Label: "//app:lib", CreatedAt: 10},
			wantFound: true,
		},
		{
			name: "none recorded",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT label, created_at FROM target_dictionary").
					WithArgs("monorepo", uint64(42)).
					WillReturnRows(sqlmock.NewRows(columns))
			},
		},
		{
			name: "query error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT label, created_at FROM target_dictionary").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupDictionaryStoreTest(t)
			defer db.Close()
			tt.setup(mock)

			got, found, err := store.Get(context.Background(), 42)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantFound, found)
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
