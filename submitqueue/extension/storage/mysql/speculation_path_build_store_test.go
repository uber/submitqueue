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

func setupSpeculationPathBuildStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.SpeculationPathBuildStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewSpeculationPathBuildStore(db, testMetrics())

	return db, mock, store
}

func TestSpeculationPathBuildStore_Create(t *testing.T) {
	pathBuild := entity.SpeculationPathBuild{
		PathID:    "path-1",
		BuildID:   "bk-1001",
		BatchID:   "monorepo/batch/1",
		CreatedAt: 1000,
		Version:   1,
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
				mock.ExpectExec("INSERT INTO speculation_path_build").
					WithArgs(pathBuild.PathID, pathBuild.BuildID, pathBuild.BatchID, pathBuild.Version, pathBuild.CreatedAt).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate path id returns ErrAlreadyExists",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_path_build").
					WithArgs(pathBuild.PathID, pathBuild.BuildID, pathBuild.BatchID, pathBuild.Version, pathBuild.CreatedAt).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_path_build").
					WithArgs(pathBuild.PathID, pathBuild.BuildID, pathBuild.BatchID, pathBuild.Version, pathBuild.CreatedAt).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationPathBuildStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), pathBuild)
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

func TestSpeculationPathBuildStore_Get(t *testing.T) {
	want := entity.SpeculationPathBuild{
		PathID:    "path-1",
		BuildID:   "bk-1001",
		BatchID:   "monorepo/batch/1",
		CreatedAt: 1000,
		Version:   1,
	}

	tests := []struct {
		name      string
		pathID    string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.SpeculationPathBuild
		wantErr   bool
		wantErrIs error
	}{
		{
			name:   "found",
			pathID: want.PathID,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"path_id", "build_id", "batch_id", "version", "created_at"}).
					AddRow(want.PathID, want.BuildID, want.BatchID, want.Version, want.CreatedAt)
				mock.ExpectQuery("SELECT path_id, build_id, batch_id, version, created_at").
					WithArgs(want.PathID).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name:   "not found",
			pathID: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT path_id, build_id, batch_id, version, created_at").
					WithArgs("missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name:   "query error",
			pathID: "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT path_id, build_id, batch_id, version, created_at").
					WithArgs("bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationPathBuildStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), tt.pathID)
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
