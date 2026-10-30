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
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	insertIssueSummarySQL = `INSERT INTO issue_summary (queue, task_id, unit_id, status, first_seen_build_id, request_id, build_id, changed_at_ms, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	getIssueSummarySQL    = `SELECT task_id, unit_id, status, first_seen_build_id, request_id, build_id, changed_at_ms, version FROM issue_summary WHERE queue = ? AND task_id = ? AND unit_id = ?`
	updateIssueSummarySQL = `UPDATE issue_summary SET status = ?, first_seen_build_id = ?, request_id = ?, build_id = ?, changed_at_ms = ?, version = ? WHERE queue = ? AND task_id = ? AND unit_id = ? AND version = ?`
)

func testIssueSummary() entity.IssueSummary {
	return entity.IssueSummary{TaskID: "validation", UnitID: "unit", Status: entity.IssueStatusOpen,
		FirstSeenBuildID: "build/1", RequestID: "1", BuildID: "build/1", ChangedAtMs: 1000, Version: 1}
}

func newIssueSummaryTestStore(t *testing.T) (sqlmock.Sqlmock, storage.IssueSummaryStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	return mock, NewIssueSummaryStore(db, testMetrics(), "queue/main")
}

func TestIssueSummaryStoreCreate(t *testing.T) {
	dependencyErr := errors.New("database unavailable")
	for _, tt := range []struct {
		name    string
		dbErr   error
		wantErr error
	}{
		{name: "created"},
		{"duplicate", &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, storage.ErrAlreadyExists},
		{"database error", dependencyErr, dependencyErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newIssueSummaryTestStore(t)
			summary := testIssueSummary()
			expected := mock.ExpectExec(insertIssueSummarySQL).WithArgs("queue/main", summary.TaskID, summary.UnitID,
				summary.Status, summary.FirstSeenBuildID, summary.RequestID, summary.BuildID, summary.ChangedAtMs, summary.Version)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err := store.Create(context.Background(), summary)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIssueSummaryStoreGet(t *testing.T) {
	dependencyErr := errors.New("database unavailable")
	for _, tt := range []struct {
		name    string
		status  entity.IssueStatus
		dbErr   error
		wantErr error
	}{
		{name: "open", status: entity.IssueStatusOpen},
		{name: "closed", status: entity.IssueStatusClosed},
		{name: "not found", dbErr: sql.ErrNoRows, wantErr: storage.ErrNotFound},
		{name: "database error", dbErr: dependencyErr, wantErr: dependencyErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newIssueSummaryTestStore(t)
			want := testIssueSummary()
			want.Status = tt.status
			expected := mock.ExpectQuery(getIssueSummarySQL).WithArgs("queue/main", want.TaskID, want.UnitID)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "status", "first_seen_build_id", "request_id", "build_id", "changed_at_ms", "version"}).
					AddRow(want.TaskID, want.UnitID, want.Status, want.FirstSeenBuildID, want.RequestID, want.BuildID, want.ChangedAtMs, want.Version))
			}
			got, err := store.Get(context.Background(), want.TaskID, want.UnitID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Empty(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		})
	}
}

func TestIssueSummaryStoreUpdate(t *testing.T) {
	dependencyErr := errors.New("database unavailable")
	for _, tt := range []struct {
		name    string
		dbErr   error
		result  sql.Result
		wantErr error
	}{
		{name: "replaced", result: sqlmock.NewResult(0, 1)},
		{name: "stale or missing", result: sqlmock.NewResult(0, 0), wantErr: storage.ErrVersionMismatch},
		{name: "database error", dbErr: dependencyErr, wantErr: dependencyErr},
		{name: "row count error", result: sqlmock.NewErrorResult(dependencyErr), wantErr: dependencyErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newIssueSummaryTestStore(t)
			summary := testIssueSummary()
			summary.Status, summary.RequestID, summary.BuildID = entity.IssueStatusClosed, "2", "build/2"
			summary.ChangedAtMs = 2000
			original := summary
			expected := mock.ExpectExec(updateIssueSummarySQL).WithArgs(summary.Status, summary.FirstSeenBuildID,
				summary.RequestID, summary.BuildID, summary.ChangedAtMs, 2, "queue/main", summary.TaskID, summary.UnitID, 1)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnResult(tt.result)
			}
			err := store.Update(context.Background(), summary, 1, 2)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, original, summary)
		})
	}
}

func TestIssueSummaryStoreRejectsInvalidWrites(t *testing.T) {
	for _, tt := range []struct {
		name       string
		queue      string
		change     func(*entity.IssueSummary)
		oldVersion int32
		newVersion int32
	}{
		{"empty task", "queue", func(s *entity.IssueSummary) { s.TaskID = "" }, 1, 2},
		{"invalid status", "queue", func(s *entity.IssueSummary) { s.Status = "invalid" }, 1, 2},
		{"long queue", strings.Repeat("q", 256), nil, 1, 2},
		{"long task", "queue", func(s *entity.IssueSummary) { s.TaskID = strings.Repeat("t", 256) }, 1, 2},
		{"long unit", "queue", func(s *entity.IssueSummary) { s.UnitID = strings.Repeat("u", 1025) }, 1, 2},
		{"long opening build", "queue", func(s *entity.IssueSummary) { s.FirstSeenBuildID = strings.Repeat("b", 1021) }, 1, 2},
		{"long source build", "queue", func(s *entity.IssueSummary) { s.BuildID = strings.Repeat("b", 1021) }, 1, 2},
		{"long source request", "queue", func(s *entity.IssueSummary) { s.RequestID = strings.Repeat("1", 256) }, 1, 2},
		{"invalid guard", "queue", nil, 0, 2},
		{"invalid replacement version", "queue", nil, 1, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newIssueSummaryTestStore(t)
			concrete := store.(*issueSummaryStore)
			concrete.queue = tt.queue
			summary := testIssueSummary()
			if tt.change != nil {
				tt.change(&summary)
			}
			if tt.oldVersion == 1 && tt.newVersion == 2 {
				require.Error(t, store.Create(context.Background(), summary))
			}
			require.Error(t, store.Update(context.Background(), summary, tt.oldVersion, tt.newVersion))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueSummaryStoreRejectsInvalidReadKeys(t *testing.T) {
	for _, tt := range []struct{ name, task, unit string }{
		{"empty task", "", "unit"},
		{"long task", strings.Repeat("t", 256), "unit"},
		{"long unit", "task", strings.Repeat("u", 1025)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, store := newIssueSummaryTestStore(t)
			_, err := store.Get(context.Background(), tt.task, tt.unit)
			require.Error(t, err)
		})
	}
}
