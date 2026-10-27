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
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func issueLogFixture() entity.IssueLog {
	return entity.IssueLog{TaskID: "task", UnitID: "unit",
		Event: entity.IssueEventResolved, BuildID: "build/2", TimestampMs: 2000}
}

func TestIssueLogCreate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		dbErr   error
		wantErr error
	}{
		{name: "created"},
		{name: "duplicate", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "database error", dbErr: assert.AnError, wantErr: assert.AnError},
		{name: "cancelled", dbErr: context.Canceled, wantErr: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueLogStore(db, testMetrics(), "queue")
			log := issueLogFixture()
			query := mock.ExpectExec(regexp.QuoteMeta(
				`INSERT INTO issue_log (queue, task_id, unit_id, build_id, event, timestamp_ms)
                 VALUES (?, ?, ?, ?, ?, ?)`)).
				WithArgs("queue", log.TaskID, log.UnitID, log.BuildID, log.Event, log.TimestampMs)
			if tt.dbErr != nil {
				query.WillReturnError(tt.dbErr)
			} else {
				query.WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err = store.Create(context.Background(), log)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueLogGet(t *testing.T) {
	for _, kind := range []string{"found", "missing", "database error", "scan error"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueLogStore(db, testMetrics(), "queue")
			log := issueLogFixture()
			query := mock.ExpectQuery(regexp.QuoteMeta(
				`SELECT task_id, unit_id, build_id, event, timestamp_ms
                 FROM issue_log WHERE queue = ? AND task_id = ? AND unit_id = ? AND build_id = ? AND event = ?`)).
				WithArgs("queue", log.TaskID, log.UnitID, log.BuildID, log.Event)
			switch kind {
			case "missing":
				query.WillReturnError(sql.ErrNoRows)
			case "database error":
				query.WillReturnError(assert.AnError)
			default:
				var timestamp any = log.TimestampMs
				if kind == "scan error" {
					timestamp = "invalid"
				}
				query.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "build_id", "event", "timestamp_ms"}).
					AddRow(log.TaskID, log.UnitID, log.BuildID, log.Event, timestamp))
			}

			got, err := store.Get(context.Background(), log.TaskID, log.UnitID, log.BuildID, log.Event)
			switch kind {
			case "found":
				require.NoError(t, err)
				require.Equal(t, log, got)
			case "missing":
				require.ErrorIs(t, err, storage.ErrNotFound)
			case "database error":
				require.ErrorIs(t, err, assert.AnError)
			default:
				require.Error(t, err)
			}
			if kind != "found" {
				require.Equal(t, entity.IssueLog{}, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueLogRejectsInvalidKeys(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*entity.IssueLog, *string)
	}{
		{name: "oversized queue", change: func(_ *entity.IssueLog, q *string) { *q = strings.Repeat("q", 256) }},
		{name: "oversized task", change: func(l *entity.IssueLog, _ *string) { l.TaskID = strings.Repeat("t", 256) }},
		{name: "oversized unit", change: func(l *entity.IssueLog, _ *string) { l.UnitID = strings.Repeat("😀", 257) }},
		{name: "oversized build", change: func(l *entity.IssueLog, _ *string) { l.BuildID = strings.Repeat("b", 1021) }},
		{name: "empty build", change: func(l *entity.IssueLog, _ *string) { l.BuildID = "" }},
		{name: "unknown event", change: func(l *entity.IssueLog, _ *string) { l.Event = entity.IssueEventUnknown }},
		{name: "unsupported event", change: func(l *entity.IssueLog, _ *string) { l.Event = "other" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			log, queue := issueLogFixture(), "queue"
			tt.change(&log, &queue)
			store := NewIssueLogStore(db, testMetrics(), queue)

			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT INTO issue_log").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(
				[]string{"task_id", "unit_id", "build_id", "event", "timestamp_ms"}).
				AddRow(log.TaskID, log.UnitID, log.BuildID, log.Event, log.TimestampMs))

			require.Error(t, store.Create(context.Background(), log))
			_, err = store.Get(context.Background(), log.TaskID, log.UnitID, log.BuildID, log.Event)
			require.Error(t, err)
		})
	}
}

func TestIssueLogRejectsInvalidValues(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*entity.IssueLog)
	}{
		{name: "zero timestamp", change: func(l *entity.IssueLog) { l.TimestampMs = 0 }},
		{name: "negative timestamp", change: func(l *entity.IssueLog) { l.TimestampMs = -1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			log := issueLogFixture()
			tt.change(&log)
			store := NewIssueLogStore(db, testMetrics(), "queue")

			mock.ExpectExec("INSERT INTO issue_log").WillReturnResult(sqlmock.NewResult(0, 1))
			require.Error(t, store.Create(context.Background(), log))
		})
	}
}
