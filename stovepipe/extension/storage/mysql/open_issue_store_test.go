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
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const (
	insertOpenIssueSQL  = `INSERT INTO open_issue (queue, task_id, unit_id, first_seen_build_id) VALUES (?, ?, ?, ?)`
	getOpenIssueSQL     = `SELECT task_id, unit_id, first_seen_build_id FROM open_issue WHERE queue = ? AND task_id = ? AND unit_id = ?`
	replaceOpenIssueSQL = `UPDATE open_issue SET first_seen_build_id = ? WHERE queue = ? AND task_id = ? AND unit_id = ? AND first_seen_build_id = ?`
	deleteOpenIssueSQL  = `DELETE FROM open_issue WHERE queue = ? AND task_id = ? AND unit_id = ? AND first_seen_build_id = ?`
)

func newOpenIssueTestStore(t *testing.T) (sqlmock.Sqlmock, storage.OpenIssueStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	})
	return mock, NewOpenIssueStore(db, testMetrics(), "queue/main")
}

func TestOpenIssueStoreCreate(t *testing.T) {
	for _, tt := range []struct {
		name           string
		dbErr, wantErr error
	}{
		{name: "created"},
		{"duplicate", &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, storage.ErrAlreadyExists},
		{"database error", assert.AnError, assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newOpenIssueTestStore(t)
			issue := entity.OpenIssue{TaskID: "task", UnitID: "unit", FirstSeenBuildID: "build/1"}
			expected := mock.ExpectExec(insertOpenIssueSQL).WithArgs("queue/main", issue.TaskID, issue.UnitID, issue.FirstSeenBuildID)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err := store.Create(context.Background(), issue)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOpenIssueStoreGet(t *testing.T) {
	for _, tt := range []struct {
		name, unit     string
		dbErr, wantErr error
	}{
		{name: "found", unit: "unit"},
		{name: "task wide"},
		{name: "missing", dbErr: sql.ErrNoRows, wantErr: storage.ErrNotFound},
		{name: "database error", dbErr: assert.AnError, wantErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newOpenIssueTestStore(t)
			want := entity.OpenIssue{TaskID: "task", UnitID: tt.unit, FirstSeenBuildID: "build/1"}
			expected := mock.ExpectQuery(getOpenIssueSQL).WithArgs("queue/main", want.TaskID, want.UnitID)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				expected.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "first_seen_build_id"}).AddRow(want.TaskID, want.UnitID, want.FirstSeenBuildID))
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

func TestOpenIssueStoreConditionalWrites(t *testing.T) {
	issue := entity.OpenIssue{TaskID: "task", UnitID: "unit", FirstSeenBuildID: "build/2"}
	for _, operation := range []struct {
		name, query string
		args        []driver.Value
		call        func(storage.OpenIssueStore) (bool, error)
	}{
		{"replace", replaceOpenIssueSQL, []driver.Value{issue.FirstSeenBuildID, "queue/main", issue.TaskID, issue.UnitID, "build/1"},
			func(s storage.OpenIssueStore) (bool, error) { return s.Replace(context.Background(), issue, "build/1") }},
		{"delete", deleteOpenIssueSQL, []driver.Value{"queue/main", issue.TaskID, issue.UnitID, issue.FirstSeenBuildID},
			func(s storage.OpenIssueStore) (bool, error) { return s.Delete(context.Background(), issue) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				result  sql.Result
				dbErr   error
				changed bool
				wantErr error
			}{
				{name: "matched", result: sqlmock.NewResult(0, 1), changed: true},
				{name: "missing or different opening", result: sqlmock.NewResult(0, 0)},
				{name: "database error", dbErr: assert.AnError, wantErr: assert.AnError},
				{name: "row count error", result: sqlmock.NewErrorResult(assert.AnError), wantErr: assert.AnError},
			} {
				t.Run(tt.name, func(t *testing.T) {
					mock, store := newOpenIssueTestStore(t)
					expected := mock.ExpectExec(operation.query).WithArgs(operation.args...)
					if tt.dbErr != nil {
						expected.WillReturnError(tt.dbErr)
					} else {
						expected.WillReturnResult(tt.result)
					}
					changed, err := operation.call(store)
					if tt.wantErr != nil {
						require.ErrorIs(t, err, tt.wantErr)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, tt.changed, changed)
				})
			}
		})
	}
}

func TestOpenIssueStoreUnchangedReplacement(t *testing.T) {
	_, store := newOpenIssueTestStore(t)
	issue := entity.OpenIssue{TaskID: "task", UnitID: "unit", FirstSeenBuildID: "build/1"}
	replaced, err := store.Replace(context.Background(), issue, issue.FirstSeenBuildID)
	require.NoError(t, err)
	require.False(t, replaced)
}

func TestOpenIssueStoreList(t *testing.T) {
	for _, tt := range []struct {
		name                       string
		after                      storage.OpenIssueCursor
		empty, scanError, rowError bool
		dbErr                      error
	}{
		{name: "start"},
		{name: "after task wide", after: storage.OpenIssueCursor{TaskID: "task"}},
		{name: "after unit", after: storage.OpenIssueCursor{TaskID: "task", UnitID: "earlier"}},
		{name: "empty", empty: true},
		{name: "query error", dbErr: assert.AnError},
		{name: "scan error", scanError: true},
		{name: "iteration error", rowError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newOpenIssueTestStore(t)
			want := entity.OpenIssue{TaskID: "task", UnitID: "unit", FirstSeenBuildID: "build/1"}
			query := `SELECT task_id, unit_id, first_seen_build_id FROM open_issue WHERE queue = ? ORDER BY task_id ASC, unit_id ASC LIMIT ?`
			args := []driver.Value{"queue/main", int64(2)}
			if tt.after.TaskID != "" {
				query = `SELECT task_id, unit_id, first_seen_build_id FROM open_issue WHERE queue = ? AND (task_id > ? OR (task_id = ? AND unit_id > ?)) ORDER BY task_id ASC, unit_id ASC LIMIT ?`
				args = []driver.Value{"queue/main", tt.after.TaskID, tt.after.TaskID, tt.after.UnitID, int64(2)}
			}
			expected := mock.ExpectQuery(query).WithArgs(args...)
			if tt.dbErr != nil {
				expected.WillReturnError(tt.dbErr)
			} else {
				rows := sqlmock.NewRows([]string{"task_id", "unit_id", "first_seen_build_id"})
				if !tt.empty {
					var opening any = want.FirstSeenBuildID
					if tt.scanError {
						opening = nil
					}
					rows.AddRow(want.TaskID, want.UnitID, opening)
					if tt.rowError {
						rows.AddRow("task", "unit-2", "build/2").RowError(1, assert.AnError)
					}
				}
				expected.WillReturnRows(rows).RowsWillBeClosed()
			}
			got, err := store.List(context.Background(), storage.OpenIssueRange{After: tt.after, Limit: 2})
			switch {
			case tt.dbErr != nil:
				require.ErrorIs(t, err, tt.dbErr)
				require.Nil(t, got)
			case tt.rowError:
				require.ErrorIs(t, err, assert.AnError)
				require.Nil(t, got)
			case tt.scanError:
				require.Error(t, err)
				require.Nil(t, got)
			case tt.empty:
				require.NoError(t, err)
				require.NotNil(t, got)
				require.Empty(t, got)
			default:
				require.NoError(t, err)
				require.Equal(t, []entity.OpenIssue{want}, got)
			}
		})
	}
}

func TestOpenIssueStoreRejectsInvalidOpenIssues(t *testing.T) {
	for _, tt := range []struct {
		name  string
		issue entity.OpenIssue
	}{
		{"missing task", entity.OpenIssue{FirstSeenBuildID: "build/1"}},
		{"missing opening", entity.OpenIssue{TaskID: "task"}},
		{"long task", entity.OpenIssue{TaskID: strings.Repeat("t", 256), FirstSeenBuildID: "build/1"}},
		{"long unit", entity.OpenIssue{TaskID: "task", UnitID: strings.Repeat("u", 1025), FirstSeenBuildID: "build/1"}},
		{"long opening", entity.OpenIssue{TaskID: "task", FirstSeenBuildID: strings.Repeat("b", 1021)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, store := newOpenIssueTestStore(t)
			require.Error(t, store.Create(context.Background(), tt.issue))
			replaced, err := store.Replace(context.Background(), tt.issue, "build/previous")
			require.Error(t, err)
			require.False(t, replaced)
			deleted, err := store.Delete(context.Background(), tt.issue)
			require.Error(t, err)
			require.False(t, deleted)
		})
	}
	for _, expected := range []string{"", strings.Repeat("b", 1021)} {
		t.Run("invalid replacement guard", func(t *testing.T) {
			_, store := newOpenIssueTestStore(t)
			replaced, err := store.Replace(context.Background(), entity.OpenIssue{TaskID: "task", FirstSeenBuildID: "build/1"}, expected)
			require.Error(t, err)
			require.False(t, replaced)
		})
	}
}

func TestOpenIssueStoreRejectsInvalidReads(t *testing.T) {
	for _, cursor := range []storage.OpenIssueCursor{
		{UnitID: "unit"},
		{TaskID: strings.Repeat("t", 256)},
		{TaskID: "task", UnitID: strings.Repeat("u", 1025)},
	} {
		_, store := newOpenIssueTestStore(t)
		_, err := store.Get(context.Background(), cursor.TaskID, cursor.UnitID)
		require.Error(t, err)
		_, err = store.List(context.Background(), storage.OpenIssueRange{After: cursor, Limit: 1})
		require.Error(t, err)
	}
	for _, limit := range []int{0, -1} {
		_, store := newOpenIssueTestStore(t)
		_, err := store.List(context.Background(), storage.OpenIssueRange{Limit: limit})
		require.Error(t, err)
	}
}
