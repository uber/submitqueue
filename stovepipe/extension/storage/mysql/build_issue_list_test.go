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

func TestBuildIssueList(t *testing.T) {
	for _, tt := range []struct {
		name      string
		after     storage.BuildIssueCursor
		empty     bool
		dbErr     error
		scanError bool
		rowError  bool
	}{
		{name: "first page"},
		{name: "after task-wide issue", after: storage.BuildIssueCursor{TaskID: "task"}},
		{name: "empty page", empty: true},
		{name: "query error", dbErr: assert.AnError},
		{name: "cancelled", dbErr: context.Canceled},
		{name: "scan error", scanError: true},
		{name: "iteration error", rowError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewBuildIssueStore(db, testMetrics(), "queue")
			issue := entity.BuildIssue{TaskID: "task", UnitID: "unit", FixID: "Fix/1 ", Version: 3}
			query := listBuildIssuesQuery
			args := []driver.Value{"queue", "build/1", int64(2)}
			if tt.after.TaskID != "" {
				query = listBuildIssuesAfterCursorQuery
				args = []driver.Value{"queue", "build/1", tt.after.TaskID, tt.after.TaskID, tt.after.UnitID, int64(2)}
			}
			expectation := mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(args...)
			if tt.dbErr != nil {
				expectation.WillReturnError(tt.dbErr)
			} else {
				rows := sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"})
				if !tt.empty {
					var task any = issue.TaskID
					if tt.scanError {
						task = nil
					}
					rows.AddRow(task, issue.UnitID, issue.FixID, issue.Version)
					if tt.rowError {
						rows.AddRow("task", "unit-2", "", 0).RowError(1, assert.AnError)
					}
				}
				expectation.WillReturnRows(rows).RowsWillBeClosed()
			}

			got, err := store.List(context.Background(), "build/1", storage.BuildIssueRange{After: tt.after, Limit: 2})
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
				require.Equal(t, []entity.BuildIssue{issue}, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBuildIssueListRejectsInvalidRange(t *testing.T) {
	for _, tt := range []struct {
		name    string
		queue   string
		buildID string
		bounds  storage.BuildIssueRange
	}{
		{name: "empty build", buildID: "", bounds: storage.BuildIssueRange{Limit: 1}},
		{name: "oversized build", buildID: strings.Repeat("b", 1021), bounds: storage.BuildIssueRange{Limit: 1}},
		{name: "zero limit", buildID: "build/1", bounds: storage.BuildIssueRange{}},
		{name: "negative limit", buildID: "build/1", bounds: storage.BuildIssueRange{Limit: -1}},
		{name: "unit without task", buildID: "build/1", bounds: storage.BuildIssueRange{Limit: 1,
			After: storage.BuildIssueCursor{UnitID: "unit"}}},
		{name: "oversized task", buildID: "build/1", bounds: storage.BuildIssueRange{Limit: 1,
			After: storage.BuildIssueCursor{TaskID: strings.Repeat("t", 256)}}},
		{name: "oversized unit", buildID: "build/1", bounds: storage.BuildIssueRange{Limit: 1,
			After: storage.BuildIssueCursor{TaskID: "task", UnitID: strings.Repeat("😀", 257)}}},
		{name: "oversized queue", buildID: "build/1", queue: strings.Repeat("q", 256), bounds: storage.BuildIssueRange{Limit: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			queue := tt.queue
			if queue == "" {
				queue = "queue"
			}
			buildID := tt.buildID
			store := NewBuildIssueStore(db, testMetrics(), queue)
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"}))

			got, err := store.List(context.Background(), buildID, tt.bounds)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
