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

func trackingFixture() entity.IssueTrackingState {
	return entity.IssueTrackingState{RequestID: "10", BuildID: "build/10", NotificationID: "notification/10", CursorTaskID: "task", CursorUnitID: "unit", Phase: entity.IssueTrackingPhaseFinalizing, Finalize: true, Version: 1}
}

func TestIssueTrackingStoreGet(t *testing.T) {
	for _, kind := range []string{"found", "missing", "database error", "scan error"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			state := trackingFixture()
			store := NewIssueTrackingStore(db, testMetrics(), "queue")
			query := mock.ExpectQuery(regexp.QuoteMeta(`SELECT request_id, build_id, notification_id, cursor_task_id, cursor_unit_id, finalize, has_task_wide_issues, phase, version FROM issue_tracking WHERE queue = ?`)).WithArgs("queue")
			switch kind {
			case "missing":
				query.WillReturnError(sql.ErrNoRows)
			case "database error":
				query.WillReturnError(assert.AnError)
			default:
				var request any = state.RequestID
				if kind == "scan error" {
					request = nil
				}
				query.WillReturnRows(sqlmock.NewRows([]string{"request_id", "build_id", "notification_id", "cursor_task_id", "cursor_unit_id", "finalize", "has_task_wide_issues", "phase", "version"}).AddRow(request, state.BuildID, state.NotificationID, state.CursorTaskID, state.CursorUnitID, state.Finalize, state.HasTaskWideIssues, state.Phase, state.Version))
			}

			got, err := store.Get(context.Background())
			switch kind {
			case "found":
				require.NoError(t, err)
				require.Equal(t, state, got)
			case "missing":
				require.ErrorIs(t, err, storage.ErrNotFound)
			case "database error":
				require.ErrorIs(t, err, assert.AnError)
			default:
				require.Error(t, err)
			}
			if kind != "found" {
				require.Equal(t, entity.IssueTrackingState{}, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueTrackingStoreCreate(t *testing.T) {
	for _, tt := range []struct {
		name           string
		dbErr, errorIs error
	}{
		{name: "created"},
		{name: "duplicate", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, errorIs: storage.ErrAlreadyExists},
		{name: "database error", dbErr: assert.AnError, errorIs: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			state := trackingFixture()
			store := NewIssueTrackingStore(db, testMetrics(), "queue")
			query := mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO issue_tracking (queue, request_id, build_id, notification_id, cursor_task_id, cursor_unit_id, finalize, has_task_wide_issues, phase, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)).
				WithArgs("queue", state.RequestID, state.BuildID, state.NotificationID, state.CursorTaskID, state.CursorUnitID, state.Finalize, state.HasTaskWideIssues, state.Phase, state.Version)
			if tt.dbErr != nil {
				query.WillReturnError(tt.dbErr)
			} else {
				query.WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err = store.Create(context.Background(), state)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueTrackingStoreUpdate(t *testing.T) {
	for _, tt := range []struct {
		name           string
		rows           int64
		dbErr, errorIs error
		resultError    bool
	}{
		{name: "updated", rows: 1},
		{name: "stale guard", errorIs: storage.ErrVersionMismatch},
		{name: "database error", dbErr: assert.AnError, errorIs: assert.AnError},
		{name: "affected rows error", resultError: true, errorIs: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			state := trackingFixture()
			store := NewIssueTrackingStore(db, testMetrics(), "queue")
			query := mock.ExpectExec(regexp.QuoteMeta(`UPDATE issue_tracking SET request_id = ?, build_id = ?, notification_id = ?, cursor_task_id = ?, cursor_unit_id = ?, finalize = ?, has_task_wide_issues = ?, phase = ?, version = ? WHERE queue = ? AND version = ?`)).
				WithArgs(state.RequestID, state.BuildID, state.NotificationID, state.CursorTaskID, state.CursorUnitID, state.Finalize, state.HasTaskWideIssues, state.Phase, int32(7), "queue", int32(1))
			switch {
			case tt.dbErr != nil:
				query.WillReturnError(tt.dbErr)
			case tt.resultError:
				query.WillReturnResult(sqlmock.NewErrorResult(assert.AnError))
			default:
				query.WillReturnResult(sqlmock.NewResult(0, tt.rows))
			}

			err = store.Update(context.Background(), state, 1, 7)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(1), state.Version)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueTrackingStoreRejectsInvalidState(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*entity.IssueTrackingState, *string)
	}{
		{"invalid request", func(s *entity.IssueTrackingState, _ *string) { s.RequestID = "invalid" }},
		{"empty notification", func(s *entity.IssueTrackingState, _ *string) { s.NotificationID = "" }},
		{"oversized notification", func(s *entity.IssueTrackingState, _ *string) { s.NotificationID = strings.Repeat("n", 1021) }},
		{"oversized task cursor", func(s *entity.IssueTrackingState, _ *string) { s.CursorTaskID = strings.Repeat("t", 256) }},
		{"oversized unit cursor", func(s *entity.IssueTrackingState, _ *string) {
			s.CursorTaskID = "task"
			s.CursorUnitID = strings.Repeat("u", 1025)
		}},

		{"empty build", func(s *entity.IssueTrackingState, _ *string) { s.BuildID = "" }},
		{"oversized build", func(s *entity.IssueTrackingState, _ *string) { s.BuildID = strings.Repeat("b", 1021) }},
		{"unknown phase", func(s *entity.IssueTrackingState, _ *string) { s.Phase = entity.IssueTrackingPhaseUnknown }},
		{"oversized queue", func(_ *entity.IssueTrackingState, q *string) { *q = strings.Repeat("q", 256) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			state, queue := trackingFixture(), "queue"
			tt.change(&state, &queue)
			store := NewIssueTrackingStore(db, testMetrics(), queue)
			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			require.Error(t, store.Create(context.Background(), state))
			require.Error(t, store.Update(context.Background(), state, 1, 2))
		})
	}
	for _, versions := range [][2]int32{{0, 1}, {1, 0}, {1, -1}} {
		t.Run(fmt.Sprintf("versions %d/%d", versions[0], versions[1]), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueTrackingStore(db, testMetrics(), "queue")
			mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
			require.Error(t, store.Update(context.Background(), trackingFixture(), versions[0], versions[1]))
		})
	}
}
