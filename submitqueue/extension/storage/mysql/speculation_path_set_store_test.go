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

// testSpecQueue is the queue every speculation-path-set store in this file is bound to.
const testSpecQueue = "monorepo"

func setupSpeculationPathSetStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, storage.SpeculationPathSetStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := NewSpeculationPathSetStore(db, testMetrics(), testSpecQueue)

	return db, mock, store
}

// testPathSet returns a two-path set for the given head: one assuming its
// dependency succeeds, one assuming it fails.
func testPathSet(head string) entity.SpeculationPathSet {
	const dep = "monorepo/batch/1"
	succeeds := entity.SpeculationPath{
		Head:         head,
		Dependencies: []entity.PathDependency{{Batch: dep, Assumption: entity.DependencyAssumptionSucceeds}},
	}
	fails := entity.SpeculationPath{
		Head:         head,
		Dependencies: []entity.PathDependency{{Batch: dep, Assumption: entity.DependencyAssumptionFails}},
	}
	return entity.SpeculationPathSet{
		Queue: testSpecQueue,
		Head:  head,
		Paths: []entity.SpeculationPathEntry{
			{
				ID:          succeeds.ID(),
				Path:        succeeds,
				Status:      entity.SpeculationPathStatusBuilding,
				Attempt:     1,
				Version:     1,
				CreatedAtMs: 1000,
				UpdatedAtMs: 2000,
			},
			{
				ID:          fails.ID(),
				Path:        fails,
				Status:      entity.SpeculationPathStatusPending,
				Attempt:     1,
				Version:     1,
				CreatedAtMs: 1000,
				UpdatedAtMs: 1000,
			},
		},
		Version: 3,
	}
}

func TestSpeculationPathSetStore_Get(t *testing.T) {
	want := testPathSet("monorepo/batch/2")
	pathsJSON, err := json.Marshal(want.Paths)
	require.NoError(t, err)

	tests := []struct {
		name      string
		head      string
		setup     func(mock sqlmock.Sqlmock)
		want      entity.SpeculationPathSet
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "found",
			head: want.Head,
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "head", "paths", "version"}).
					AddRow(want.Queue, want.Head, pathsJSON, want.Version)
				mock.ExpectQuery("SELECT queue, head, paths, version FROM speculation_path_set").
					WithArgs(testSpecQueue, want.Head).
					WillReturnRows(rows)
			},
			want: want,
		},
		{
			name: "not found",
			head: "missing",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, head, paths, version FROM speculation_path_set").
					WithArgs(testSpecQueue, "missing").
					WillReturnError(sql.ErrNoRows)
			},
			wantErr:   true,
			wantErrIs: storage.ErrNotFound,
		},
		{
			name: "malformed paths json",
			head: "corrupt",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"queue", "head", "paths", "version"}).
					AddRow(testSpecQueue, "corrupt", []byte("{not json"), 1)
				mock.ExpectQuery("SELECT queue, head, paths, version FROM speculation_path_set").
					WithArgs(testSpecQueue, "corrupt").
					WillReturnRows(rows)
			},
			wantErr: true,
		},
		{
			name: "query error",
			head: "bad",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, head, paths, version FROM speculation_path_set").
					WithArgs(testSpecQueue, "bad").
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationPathSetStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			got, err := store.Get(context.Background(), tt.head)
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

func TestSpeculationPathSetStore_Create(t *testing.T) {
	set := testPathSet("monorepo/batch/2")
	otherQueueSet := testPathSet("monorepo/batch/2")
	otherQueueSet.Queue = "other-queue"

	tests := []struct {
		name      string
		set       entity.SpeculationPathSet
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_path_set").
					WithArgs(set.Queue, set.Head, sqlmock.AnyArg(), set.Version).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "duplicate head returns ErrAlreadyExists",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_path_set").
					WithArgs(set.Queue, set.Head, sqlmock.AnyArg(), set.Version).
					WillReturnError(&mysql.MySQLError{Number: mysqlErrDuplicateEntry})
			},
			wantErr:   true,
			wantErrIs: storage.ErrAlreadyExists,
		},
		{
			name: "other exec error",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO speculation_path_set").
					WithArgs(set.Queue, set.Head, sqlmock.AnyArg(), set.Version).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name:    "queue mismatch is rejected without touching the database",
			set:     otherQueueSet,
			setup:   func(sqlmock.Sqlmock) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationPathSetStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Create(context.Background(), tt.set)
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

func TestSpeculationPathSetStore_Update(t *testing.T) {
	const oldVersion, newVersion = int32(3), int32(4)
	set := testPathSet("monorepo/batch/2")
	otherQueueSet := testPathSet("monorepo/batch/2")
	otherQueueSet.Queue = "other-queue"

	tests := []struct {
		name      string
		set       entity.SpeculationPathSet
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantErrIs error
	}{
		{
			name: "success",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_path_set").
					WithArgs(sqlmock.AnyArg(), newVersion, set.Queue, set.Head, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			name: "version mismatch",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_path_set").
					WithArgs(sqlmock.AnyArg(), newVersion, set.Queue, set.Head, oldVersion).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr:   true,
			wantErrIs: storage.ErrVersionMismatch,
		},
		{
			name: "exec error",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_path_set").
					WithArgs(sqlmock.AnyArg(), newVersion, set.Queue, set.Head, oldVersion).
					WillReturnError(fmt.Errorf("connection reset"))
			},
			wantErr: true,
		},
		{
			name: "rows affected error",
			set:  set,
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE speculation_path_set").
					WithArgs(sqlmock.AnyArg(), newVersion, set.Queue, set.Head, oldVersion).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("driver error")))
			},
			wantErr: true,
		},
		{
			name:    "queue mismatch is rejected without touching the database",
			set:     otherQueueSet,
			setup:   func(sqlmock.Sqlmock) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSpeculationPathSetStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			err := store.Update(context.Background(), tt.set, oldVersion, newVersion)
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

// TestSpeculationPathSetStore_UpdateIgnoresEntityVersion pins the documented
// contract that the guard and the written value come from the explicit
// arguments, not from the entity — a caller that forgot to refresh set.Version
// must not accidentally write it.
func TestSpeculationPathSetStore_UpdateIgnoresEntityVersion(t *testing.T) {
	db, mock, store := setupSpeculationPathSetStoreTest(t)
	defer db.Close()

	set := testPathSet("monorepo/batch/2")
	set.Version = 99 // deliberately disagrees with both arguments

	const oldVersion, newVersion = int32(3), int32(4)
	mock.ExpectExec("UPDATE speculation_path_set").
		WithArgs(sqlmock.AnyArg(), newVersion, set.Queue, set.Head, oldVersion).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.Update(context.Background(), set, oldVersion, newVersion))
	require.NoError(t, mock.ExpectationsWereMet())
}
