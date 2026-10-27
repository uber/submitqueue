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

func issueStateFixture() entity.IssueState {
	return entity.IssueState{
		TaskID: "task", UnitID: "unit",
		FirstSeenBuildID: "build/1", Status: entity.IssueStatusOpen, Version: 1,
		LastProcessedBuildID: "build/1", LastChangedBuildID: "build/1", ChangedAtMs: 1000}
}

func TestIssueStateCreate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		dbErr   error
		wantErr error
	}{
		{name: "created"},
		{name: "duplicate", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "database error", dbErr: assert.AnError, wantErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := issueStateFixture()
			store := NewIssueStateStore(db, testMetrics(), "queue")

			q := mock.ExpectExec(regexp.QuoteMeta(
				`INSERT INTO issue_state (queue, task_id, unit_id, status, first_seen_build_id, last_processed_build_id, last_changed_build_id, changed_at_ms, version)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)).
				WithArgs("queue", issue.TaskID, issue.UnitID, issue.Status, issue.FirstSeenBuildID, issue.LastProcessedBuildID, issue.LastChangedBuildID, issue.ChangedAtMs, issue.Version)
			if tt.dbErr != nil {
				q.WillReturnError(tt.dbErr)
			} else {
				q.WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err = store.Create(context.Background(), issue)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueStateGet(t *testing.T) {
	for _, kind := range []string{"found", "missing", "database error", "scan error"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := issueStateFixture()
			store := NewIssueStateStore(db, testMetrics(), "queue")

			q := mock.ExpectQuery(regexp.QuoteMeta(
				`SELECT task_id, unit_id, status, first_seen_build_id, last_processed_build_id, last_changed_build_id, changed_at_ms, version
                 FROM issue_state WHERE queue = ? AND task_id = ? AND unit_id = ?`)).
				WithArgs("queue", issue.TaskID, issue.UnitID)
			switch kind {
			case "missing":
				q.WillReturnError(sql.ErrNoRows)
			case "database error":
				q.WillReturnError(assert.AnError)
			default:
				var firstBuild any = issue.FirstSeenBuildID
				if kind == "scan error" {
					firstBuild = nil
				}

				q.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "status", "first_seen_build_id", "last_processed_build_id", "last_changed_build_id", "changed_at_ms", "version"}).
					AddRow(issue.TaskID, issue.UnitID, issue.Status, firstBuild, issue.LastProcessedBuildID, issue.LastChangedBuildID, issue.ChangedAtMs, issue.Version))
			}

			got, err := store.Get(context.Background(), issue.TaskID, issue.UnitID)
			switch kind {
			case "found":
				require.NoError(t, err)
				assert.Equal(t, issue, got)
			case "missing":
				require.ErrorIs(t, err, storage.ErrNotFound)
			case "database error":
				require.ErrorIs(t, err, assert.AnError)
			default:
				require.Error(t, err)
			}

			if kind != "found" {
				assert.Equal(t, entity.IssueState{}, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueStateUpdate(t *testing.T) {
	for _, tt := range []struct {
		name           string
		rows           int64
		dbErr, wantErr error
		resultError    bool
	}{
		{name: "updated", rows: 1},
		{name: "stale version", wantErr: storage.ErrVersionMismatch},
		{name: "database error", dbErr: assert.AnError, wantErr: assert.AnError},
		{name: "result error", resultError: true, wantErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := issueStateFixture()
			issue.Status = entity.IssueStatusClosed
			expectation := mock.ExpectExec(regexp.QuoteMeta(`UPDATE issue_state SET status = ?, first_seen_build_id = ?, last_processed_build_id = ?, last_changed_build_id = ?, changed_at_ms = ?, version = ? WHERE queue = ? AND task_id = ? AND unit_id = ? AND version = ?`)).
				WithArgs(issue.Status, issue.FirstSeenBuildID, issue.LastProcessedBuildID, issue.LastChangedBuildID, issue.ChangedAtMs, int32(2), "queue", issue.TaskID, issue.UnitID, int32(1))
			switch {
			case tt.dbErr != nil:
				expectation.WillReturnError(tt.dbErr)
			case tt.resultError:
				expectation.WillReturnResult(sqlmock.NewErrorResult(assert.AnError))
			default:
				expectation.WillReturnResult(sqlmock.NewResult(0, tt.rows))
			}
			before := issue
			err = NewIssueStateStore(db, testMetrics(), "queue").Update(context.Background(), issue, 1, 2)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, before, issue)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueStateRejectsInvalidIdentifiers(t *testing.T) {
	for _, kind := range []string{"queue", "task", "empty task", "unit", "first build", "empty first build", "unknown status", "zero version", "processed build", "changed build", "changed timestamp"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			queue := "queue"
			issue := issueStateFixture()

			switch kind {
			case "queue":
				queue = strings.Repeat("q", 256)
			case "task":
				issue.TaskID = strings.Repeat("t", 256)
			case "empty task":
				issue.TaskID = ""
			case "unit":
				issue.UnitID = strings.Repeat("😀", 257)
			case "first build":
				issue.FirstSeenBuildID = strings.Repeat("b", 1021)
			case "empty first build":
				issue.FirstSeenBuildID = ""
			case "unknown status":
				issue.Status = entity.IssueStatusUnknown
			case "processed build":
				issue.LastProcessedBuildID = strings.Repeat("b", 1021)
			case "changed build":
				issue.LastChangedBuildID = strings.Repeat("b", 1021)
			case "changed timestamp":
				issue.ChangedAtMs = 0
			case "zero version":
				issue.Version = 0
			}

			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT INTO issue_state").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("UPDATE issue_state").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT task_id, unit_id").WillReturnRows(
				sqlmock.NewRows([]string{"task_id", "unit_id", "status", "first_seen_build_id", "last_processed_build_id", "last_changed_build_id", "changed_at_ms", "version"}).
					AddRow(issue.TaskID, issue.UnitID, issue.Status, issue.FirstSeenBuildID, issue.LastProcessedBuildID, issue.LastChangedBuildID, issue.ChangedAtMs, issue.Version),
			)
			store := NewIssueStateStore(db, testMetrics(), queue)

			require.Error(t, store.Create(context.Background(), issue))
			require.Error(t, store.Update(context.Background(), issue, 1, issue.Version))

			switch kind {
			case "queue", "task", "empty task", "unit":
				_, err = store.Get(context.Background(), issue.TaskID, issue.UnitID)
				require.Error(t, err)
			}
		})
	}
}
