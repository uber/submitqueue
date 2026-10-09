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
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestQueuePolicyStoreConditionalUpdate(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, tt := range []struct {
		name string
		rows int64
		err  error
		want error
	}{
		{"writes supplied version", 1, nil, nil}, {"stale guard", 0, nil, storage.ErrVersionMismatch}, {"dependency error", 0, failure, failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			store := NewQueuePolicyStore(db, tally.NoopScope, "repo/main")
			query := mock.ExpectExec(regexp.QuoteMeta("UPDATE queue_policy SET transition_id = ?, revision = ?, version = ? WHERE queue = ? AND version = ?")).WithArgs("enable", 2, 17, "repo/main", 8)
			if tt.err != nil {
				query.WillReturnError(tt.err)
			} else {
				query.WillReturnResult(sqlmock.NewResult(0, tt.rows))
			}
			err = store.UpdateCurrent(context.Background(), entity.QueuePolicyHead{Queue: "repo/main", TransitionID: "enable", Revision: 2, Version: 8}, 8, 17)
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.want)
			}
			require.NoError(t, mock.ExpectationsWereMet())
			mock.ExpectClose()
		})
	}
}

func TestQueuePolicyStoreReadFailures(t *testing.T) {
	failure := errors.New("database unavailable")
	for _, tt := range []struct {
		name string
		err  error
		want error
	}{{"missing", sql.ErrNoRows, storage.ErrNotFound}, {"dependency", failure, failure}} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewQueuePolicyStore(db, tally.NoopScope, "repo/main")
			mock.ExpectQuery("SELECT queue, transition_id, revision, version FROM queue_policy").WithArgs("repo/main").WillReturnError(tt.err)
			_, err = store.GetCurrent(context.Background())
			require.ErrorIs(t, err, tt.want)
			mock.ExpectQuery("SELECT queue, id, previous_id, revision").WithArgs("repo/main", "enable").WillReturnError(tt.err)
			_, err = store.GetTransition(context.Background(), "enable")
			require.ErrorIs(t, err, tt.want)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestQueuePolicyStoreDuplicateAndForeignWrites(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewQueuePolicyStore(db, tally.NoopScope, "repo/main")
	ctx := context.Background()
	head := entity.QueuePolicyHead{Queue: "repo/main", TransitionID: "initial", Revision: 1, Version: 1}
	transition := entity.QueuePolicyTransition{ID: "initial", Queue: "repo/main", Policy: entity.QueuePolicy{Revision: 1, State: entity.QueuePolicyStateDisabled, ChangedAtMs: 1000}}
	mock.ExpectExec("INSERT INTO queue_policy ").WithArgs("repo/main", "initial", 1, 1).WillReturnError(&driver.MySQLError{Number: mysqlErrDuplicateEntry})
	require.ErrorIs(t, store.CreateCurrent(ctx, head), storage.ErrAlreadyExists)
	mock.ExpectExec("INSERT INTO queue_policy_transition ").WithArgs("repo/main", "initial", "", 1, entity.QueuePolicyStateDisabled, "", 1000).WillReturnError(&driver.MySQLError{Number: mysqlErrDuplicateEntry})
	require.ErrorIs(t, store.CreateTransition(ctx, transition), storage.ErrAlreadyExists)
	head.Queue = "other/main"
	transition.Queue = "other/main"
	require.Error(t, store.CreateCurrent(ctx, head))
	require.Error(t, store.UpdateCurrent(ctx, head, 1, 2))
	require.Error(t, store.CreateTransition(ctx, transition))
	require.NoError(t, mock.ExpectationsWereMet())
}
