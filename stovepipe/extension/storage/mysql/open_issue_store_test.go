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

func openIssueFixture() entity.OpenIssue {
	return entity.OpenIssue{
		TaskID: "task", UnitID: "unit",
		FirstSeenBuildID: "build/1",
	}
}

func TestOpenIssueCreate(t *testing.T) {
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
			issue := openIssueFixture()
			store := NewOpenIssueStore(db, testMetrics(), "queue")

			q := mock.ExpectExec(regexp.QuoteMeta(
				`INSERT INTO open_issue (queue, task_id, unit_id, first_seen_build_id)
                 VALUES (?, ?, ?, ?)`)).
				WithArgs("queue", issue.TaskID, issue.UnitID, issue.FirstSeenBuildID)
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

func TestOpenIssueGet(t *testing.T) {
	for _, kind := range []string{"found", "missing", "database error", "scan error"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := openIssueFixture()
			store := NewOpenIssueStore(db, testMetrics(), "queue")

			q := mock.ExpectQuery(regexp.QuoteMeta(
				`SELECT task_id, unit_id, first_seen_build_id
                 FROM open_issue WHERE queue = ? AND task_id = ? AND unit_id = ?`)).
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

				q.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "first_seen_build_id"}).
					AddRow(issue.TaskID, issue.UnitID, firstBuild))
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
				assert.Equal(t, entity.OpenIssue{}, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestOpenIssueDelete(t *testing.T) {
	for _, tt := range []struct {
		name  string
		rows  int64
		dbErr error
	}{
		{name: "matching opening", rows: 1},
		{name: "already absent"},
		{name: "different opening"},
		{name: "database error", dbErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := openIssueFixture()
			store := NewOpenIssueStore(db, testMetrics(), "queue")

			q := mock.ExpectExec(regexp.QuoteMeta(
				`DELETE FROM open_issue
                 WHERE queue = ? AND task_id = ? AND unit_id = ? AND first_seen_build_id = ?`)).
				WithArgs("queue", issue.TaskID, issue.UnitID, issue.FirstSeenBuildID)
			if tt.dbErr != nil {
				q.WillReturnError(tt.dbErr)
			} else {
				q.WillReturnResult(sqlmock.NewResult(0, tt.rows))
			}

			err = store.Delete(context.Background(), issue)
			if tt.dbErr != nil {
				require.ErrorIs(t, err, tt.dbErr)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestOpenIssueRejectsInvalidIdentifiers(t *testing.T) {
	for _, kind := range []string{"queue", "task", "empty task", "unit", "first build", "empty first build"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			queue := "queue"
			issue := openIssueFixture()

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
			}

			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT INTO open_issue").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("DELETE FROM open_issue").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT task_id, unit_id").WillReturnRows(
				sqlmock.NewRows([]string{"task_id", "unit_id", "first_seen_build_id"}).
					AddRow(issue.TaskID, issue.UnitID, issue.FirstSeenBuildID),
			)
			store := NewOpenIssueStore(db, testMetrics(), queue)

			require.Error(t, store.Create(context.Background(), issue))
			require.Error(t, store.Delete(context.Background(), issue))

			switch kind {
			case "queue", "task", "empty task", "unit":
				_, err = store.Get(context.Background(), issue.TaskID, issue.UnitID)
				require.Error(t, err)
			}
		})
	}
}
