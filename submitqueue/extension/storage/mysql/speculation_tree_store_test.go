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

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func setupSpeculationTreeStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.SpeculationTreeStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewSpeculationTreeStore(db, testMetrics())

	return db, mock, store
}

func TestSpeculationTreeStore_Get(t *testing.T) {
	want := entity.SpeculationTree{
		BatchID: "monorepo/batch/1",
		Paths: []entity.SpeculationPathInfo{
			{ID: "path-1", Path: entity.SpeculationPath{Head: "monorepo/batch/1"}, Status: entity.SpeculationPathStatusCandidate},
		},
		Version: 1,
	}
	pathsJSON, err := json.Marshal(want.Paths)
	require.NoError(t, err)

	tests := []struct {
		name      string
		batchID   string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.SpeculationTree
		wantErr   bool
		wantErrIs error
	}{
		{
			name:    "found",
			batchID: want.BatchID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"batch_id", "paths", "version"}).
					AddRow(want.BatchID, pathsJSON, want.Version)
				mock.ExpectQuery("SELECT batch_id, paths, version FROM speculation_tree").
					WithArgs(want.BatchID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name:    "not found",
			batchID: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT batch_id, paths, version FROM speculation_tree").
					WithArgs("missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name:    "query error",
			batchID: "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT batch_id, paths, version FROM speculation_tree").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name:    "malformed paths JSON",
			batchID: "malformed",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"batch_id", "paths", "version"}).
					AddRow(want.BatchID, []byte("not json"), want.Version)
				mock.ExpectQuery("SELECT batch_id, paths, version FROM speculation_tree").
					WithArgs("malformed").
					WillReturnRows(rows)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationTreeStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), tt.batchID)
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

func TestSpeculationTreeStore_Create(t *testing.T) {
	tree := entity.SpeculationTree{
		BatchID: "monorepo/batch/1",
		Paths: []entity.SpeculationPathInfo{
			{ID: "path-1", Path: entity.SpeculationPath{Head: "monorepo/batch/1"}, Status: entity.SpeculationPathStatusCandidate},
		},
		Version: 1,
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
				mock.ExpectExec("INSERT INTO speculation_tree").
					WithArgs(tree.BatchID, sqlmock.AnyArg(), tree.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate batch id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_tree").
					WithArgs(tree.BatchID, sqlmock.AnyArg(), tree.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_tree").
					WithArgs(tree.BatchID, sqlmock.AnyArg(), tree.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationTreeStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), tree)
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

func TestSpeculationTreeStore_Update(t *testing.T) {
	const batchID = "monorepo/batch/1"
	const oldVersion, newVersion = int32(1), int32(2)
	paths := []entity.SpeculationPathInfo{
		{ID: "path-1", Path: entity.SpeculationPath{Head: batchID}, Status: entity.SpeculationPathStatusCandidate},
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
				mock.ExpectExec("UPDATE speculation_tree").
					WithArgs(sqlmock.AnyArg(), newVersion, batchID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_tree").
					WithArgs(sqlmock.AnyArg(), newVersion, batchID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name: "exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_tree").
					WithArgs(sqlmock.AnyArg(), newVersion, batchID, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_tree").
					WithArgs(sqlmock.AnyArg(), newVersion, batchID, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
		{
			name: "unexpected rows affected",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_tree").
					WithArgs(sqlmock.AnyArg(), newVersion, batchID, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 2))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationTreeStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), batchID, oldVersion, newVersion, paths)
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
