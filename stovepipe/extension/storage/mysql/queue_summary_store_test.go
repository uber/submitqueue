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
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func queueSummaryFixture() entity.QueueSummary {
	return entity.QueueSummary{Queue: "repo/main", State: entity.QueueEnablementEnabled, EffectiveFromCommitURI: "A", StateTimestampMs: 1000, Version: 2, QueueStateVersion: 7}
}

func TestQueueSummaryStoreConditionalUpdate(t *testing.T) {
	failure := errors.New("storage failure")
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"supplied versions", func(m sqlmock.Sqlmock) {
			m.ExpectExec("UPDATE queue_summary").WithArgs("enabled", "A", int64(1000), int64(7), int64(9), "repo/main", int64(2)).WillReturnResult(sqlmock.NewResult(0, 1))
		}, nil},
		{"stale version", func(m sqlmock.Sqlmock) {
			m.ExpectExec("UPDATE queue_summary").WillReturnResult(sqlmock.NewResult(0, 0))
		}, storage.ErrVersionMismatch},
		{"dependency failure", func(m sqlmock.Sqlmock) { m.ExpectExec("UPDATE queue_summary").WillReturnError(failure) }, failure},
		{"rows affected failure", func(m sqlmock.Sqlmock) {
			m.ExpectExec("UPDATE queue_summary").WillReturnResult(sqlmock.NewErrorResult(failure))
		}, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			tc.setup(m)
			state := queueSummaryFixture()
			err = NewQueueSummaryStore(db, tally.NoopScope, state.Queue).Update(context.Background(), state, 2, 9)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.Equal(t, int64(2), state.Version)
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestQueueSummaryStoreCreateAndGet(t *testing.T) {
	state := queueSummaryFixture()
	failure := errors.New("storage failure")
	for _, tc := range []struct {
		name       string
		createErr  error
		getErr     error
		wantCreate error
		wantGet    error
	}{
		{"round trip", nil, nil, nil, nil},
		{"duplicate", &drivermysql.MySQLError{Number: mysqlErrDuplicateEntry}, nil, storage.ErrAlreadyExists, nil},
		{"missing", nil, sql.ErrNoRows, nil, storage.ErrNotFound},
		{"dependency", failure, failure, failure, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			exec := m.ExpectExec("INSERT INTO queue_summary").WithArgs(state.Queue, "enabled", "A", int64(1000), int64(7), int64(2))
			if tc.createErr != nil {
				exec.WillReturnError(tc.createErr)
			} else {
				exec.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			query := m.ExpectQuery("SELECT .* FROM queue_summary WHERE queue").WithArgs(state.Queue)
			if tc.getErr != nil {
				query.WillReturnError(tc.getErr)
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"queue", "state", "uri", "time", "state_version", "version"}).AddRow(state.Queue, state.State, "A", 1000, 7, 2))
			}
			store := NewQueueSummaryStore(db, tally.NoopScope, state.Queue)
			err = store.Create(context.Background(), state)
			if tc.wantCreate != nil {
				require.ErrorIs(t, err, tc.wantCreate)
			} else {
				require.NoError(t, err)
			}
			got, err := store.Get(context.Background())
			if tc.wantGet != nil {
				require.ErrorIs(t, err, tc.wantGet)
			} else {
				require.NoError(t, err)
				require.Equal(t, state, got)
			}
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestQueueLogStoreImmutableOccurrence(t *testing.T) {
	log := entity.QueueLog{ID: entity.QueueLogID(2), Queue: "repo/main", TimestampMs: 1000, State: entity.QueueEnablementEnabled, QueueStateVersion: 2, EffectiveFromCommitURI: "A"}
	failure := errors.New("storage failure")
	for _, tc := range []struct {
		name       string
		createErr  error
		getErr     error
		wantCreate error
		wantGet    error
	}{
		{"round trip", nil, nil, nil, nil},
		{"immutable duplicate", &drivermysql.MySQLError{Number: mysqlErrDuplicateEntry}, nil, storage.ErrAlreadyExists, nil},
		{"missing", nil, sql.ErrNoRows, nil, storage.ErrNotFound},
		{"dependency", failure, failure, failure, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			exec := m.ExpectExec("INSERT INTO queue_log").WithArgs(log.Queue, log.ID, int64(1000), "enabled", int64(2), "A")
			if tc.createErr != nil {
				exec.WillReturnError(tc.createErr)
			} else {
				exec.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			query := m.ExpectQuery("SELECT .* FROM queue_log WHERE queue").WithArgs(log.Queue, log.ID)
			if tc.getErr != nil {
				query.WillReturnError(tc.getErr)
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"queue", "id", "time", "state", "version", "uri"}).AddRow(log.Queue, log.ID, 1000, "enabled", 2, "A"))
			}
			store := NewQueueLogStore(db, tally.NoopScope, log.Queue)
			err = store.Create(context.Background(), log)
			if tc.wantCreate != nil {
				require.ErrorIs(t, err, tc.wantCreate)
			} else {
				require.NoError(t, err)
			}
			got, err := store.Get(context.Background(), log.ID)
			if tc.wantGet != nil {
				require.ErrorIs(t, err, tc.wantGet)
			} else {
				require.NoError(t, err)
				require.Equal(t, log, got)
			}
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestQueueSummaryStoresRejectInvalidWrites(t *testing.T) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	state := queueSummaryFixture()
	state.Queue = "other"
	store := NewQueueSummaryStore(db, tally.NoopScope, "repo/main")
	require.Error(t, store.Create(context.Background(), state))
	require.Error(t, store.Update(context.Background(), state, 1, 2))
	state.Queue = "repo/main"
	state.State = entity.QueueEnablementUnknown
	require.Error(t, store.Create(context.Background(), state))
	require.Error(t, store.Update(context.Background(), state, 1, 2))
	logs := NewQueueLogStore(db, tally.NoopScope, "repo/main")
	log := entity.QueueLog{Queue: "other"}
	require.Error(t, logs.Create(context.Background(), log))
	log.Queue = "repo/main"
	require.Error(t, logs.Create(context.Background(), log))
	require.NoError(t, m.ExpectationsWereMet())
}
