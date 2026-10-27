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
	"database/sql/driver"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestIssueStateList(t *testing.T) {
	for _, tt := range []struct {
		name      string
		after     storage.IssueStateCursor
		empty     bool
		dbErr     error
		scanError bool
		rowError  bool
	}{
		{name: "first page"},
		{name: "after task-wide issue", after: storage.IssueStateCursor{TaskID: "task"}},
		{name: "empty page", empty: true},
		{name: "query error", dbErr: assert.AnError},
		{name: "canceled", dbErr: context.Canceled},
		{name: "scan error", scanError: true},
		{name: "iteration error", rowError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewIssueStateStore(db, testMetrics(), "queue")
			issue := issueStateFixture()
			query := listIssueStatesQuery
			args := []driver.Value{"queue", int64(2)}
			if tt.after.TaskID != "" {
				query = listIssueStatesAfterCursorQuery
				args = []driver.Value{"queue", tt.after.TaskID, tt.after.TaskID, tt.after.UnitID, int64(2)}
			}
			expectation := mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(args...)
			if tt.dbErr != nil {
				expectation.WillReturnError(tt.dbErr)
			} else {
				rows := sqlmock.NewRows([]string{"task_id", "unit_id", "status", "first_seen_build_id", "last_processed_build_id", "last_changed_build_id", "changed_at_ms", "version"})
				if !tt.empty {
					var firstBuild any = issue.FirstSeenBuildID
					if tt.scanError {
						firstBuild = nil
					}
					rows.AddRow(issue.TaskID, issue.UnitID, issue.Status, firstBuild, issue.LastProcessedBuildID, issue.LastChangedBuildID, issue.ChangedAtMs, issue.Version)
					if tt.rowError {
						rows.AddRow("task", "unit-2", entity.IssueStatusClosed, "build/2", "build/2", "build/2", int64(1000), 2).RowError(1, assert.AnError)
					}
				}
				expectation.WillReturnRows(rows).RowsWillBeClosed()
			}

			got, err := store.List(context.Background(), storage.IssueStateRange{After: tt.after, Limit: 2})
			switch {
			case tt.dbErr != nil:
				require.ErrorIs(t, err, tt.dbErr)
				require.Nil(t, got)
			case tt.scanError:
				require.Error(t, err)
				require.Nil(t, got)
			case tt.rowError:
				require.ErrorIs(t, err, assert.AnError)
				require.Nil(t, got)
			case tt.empty:
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Empty(t, got)
			default:
				require.NoError(t, err)
				require.Equal(t, []entity.IssueState{issue}, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIssueStateListRejectsInvalidRange(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queue  string
		bounds storage.IssueStateRange
	}{
		{name: "zero limit", bounds: storage.IssueStateRange{}},
		{name: "negative limit", bounds: storage.IssueStateRange{Limit: -1}},
		{name: "unit without task", bounds: storage.IssueStateRange{Limit: 1,
			After: storage.IssueStateCursor{UnitID: "unit"}}},
		{name: "oversized task", bounds: storage.IssueStateRange{Limit: 1,
			After: storage.IssueStateCursor{TaskID: strings.Repeat("t", 256)}}},
		{name: "oversized unit", bounds: storage.IssueStateRange{Limit: 1,
			After: storage.IssueStateCursor{TaskID: "task", UnitID: strings.Repeat("😀", 257)}}},
		{name: "oversized queue", queue: strings.Repeat("q", 256), bounds: storage.IssueStateRange{Limit: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			queue := tt.queue
			if queue == "" {
				queue = "queue"
			}
			store := NewIssueStateStore(db, testMetrics(), queue)
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "status", "first_seen_build_id", "last_processed_build_id", "last_changed_build_id", "changed_at_ms", "version"}))

			got, err := store.List(context.Background(), tt.bounds)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
