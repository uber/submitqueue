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
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestBuildIssueStoreCreate(t *testing.T) {
	dbErr := errors.New("database unavailable")

	for _, tt := range []struct {
		name    string
		unitID  string
		dbErr   error
		wantErr error
	}{
		{name: "unit observation", unitID: "unit"},
		{name: "task-wide observation"},
		{name: "long unit identifier", unitID: strings.Repeat("u", 1024)},
		{name: "duplicate observation", unitID: "unit", dbErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "dependency error", unitID: "unit", dbErr: dbErr, wantErr: dbErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			store := NewBuildIssueStore(db, testMetrics(), "queue")
			issue := entity.BuildIssue{TaskID: "task", UnitID: tt.unitID}

			expectation := mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO build_issue (queue, build_id, task_id, unit_id, fix_id, version) VALUES (?, ?, ?, ?, ?, ?)`)).
				WithArgs("queue", "build", issue.TaskID, issue.UnitID, issue.FixID, issue.Version)
			if tt.dbErr != nil {
				expectation.WillReturnError(tt.dbErr)
			} else {
				expectation.WillReturnResult(sqlmock.NewResult(0, 1))
			}

			err = store.Create(context.Background(), "build", issue)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBuildIssueStoreGet(t *testing.T) {
	dbErr := errors.New("database unavailable")

	for _, tt := range []struct {
		name     string
		unitID   string
		queryErr error
		scanErr  bool
		wantErr  error
	}{
		{name: "unit observation", unitID: "unit"},
		{name: "task-wide observation"},
		{name: "missing observation", unitID: "unit", queryErr: sql.ErrNoRows, wantErr: storage.ErrNotFound},
		{name: "dependency error", unitID: "unit", queryErr: dbErr, wantErr: dbErr},
		{name: "scan error", unitID: "unit", scanErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			store := NewBuildIssueStore(db, testMetrics(), "queue")
			issue := entity.BuildIssue{TaskID: "task", UnitID: tt.unitID}

			expectation := mock.ExpectQuery(regexp.QuoteMeta(`SELECT task_id, unit_id, fix_id, version FROM build_issue WHERE queue = ? AND build_id = ? AND task_id = ? AND unit_id = ?`)).
				WithArgs("queue", "build", issue.TaskID, issue.UnitID)
			if tt.queryErr != nil {
				expectation.WillReturnError(tt.queryErr)
			} else if tt.scanErr {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"}).AddRow(nil, issue.UnitID, "", 0))
			} else {
				expectation.WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"}).AddRow(issue.TaskID, issue.UnitID, issue.FixID, issue.Version))
			}

			got, err := store.Get(context.Background(), "build", issue.TaskID, issue.UnitID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else if tt.scanErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, issue, got)
			}

			if tt.wantErr != nil || tt.scanErr {
				require.Equal(t, entity.BuildIssue{}, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBuildIssueStoreRejectsOversizedIdentifiers(t *testing.T) {
	for _, tt := range []struct {
		name    string
		queue   string
		buildID string
		issue   entity.BuildIssue
	}{
		{name: "queue", queue: strings.Repeat("q", 256), buildID: "build", issue: entity.BuildIssue{TaskID: "task"}},
		{name: "build ID", queue: "queue", buildID: strings.Repeat("b", 1021), issue: entity.BuildIssue{TaskID: "task"}},
		{name: "task ID", queue: "queue", buildID: "build", issue: entity.BuildIssue{TaskID: strings.Repeat("t", 256)}},
		{name: "unit ID", queue: "queue", buildID: "build", issue: entity.BuildIssue{TaskID: "task", UnitID: strings.Repeat("u", 1025)}},
		{name: "multibyte task ID", queue: "queue", buildID: "build", issue: entity.BuildIssue{TaskID: strings.Repeat("é", 128)}},
		{name: "multibyte unit ID", queue: "queue", buildID: "build", issue: entity.BuildIssue{TaskID: "task", UnitID: strings.Repeat("😀", 257)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			store := NewBuildIssueStore(db, testMetrics(), tt.queue)

			// SQL would succeed, so rejection must come from identifier validation.
			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT INTO build_issue").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT").WillReturnRows(
				sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"}).AddRow(tt.issue.TaskID, tt.issue.UnitID, tt.issue.FixID, tt.issue.Version),
			)

			require.Error(t, store.Create(context.Background(), tt.buildID, tt.issue))

			got, err := store.Get(context.Background(), tt.buildID, tt.issue.TaskID, tt.issue.UnitID)
			require.Error(t, err)
			require.Equal(t, entity.BuildIssue{}, got)
			require.Error(t, store.Update(context.Background(), tt.buildID, tt.issue, 0, 1))
		})
	}
}

func TestBuildIssueStoreRejectsMissingIdentifiers(t *testing.T) {
	for _, tt := range []struct {
		name    string
		buildID string
		taskID  string
	}{
		{name: "missing build", taskID: "task"},
		{name: "missing task", buildID: "build/1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := NewBuildIssueStore(db, testMetrics(), "queue")
			mock.MatchExpectationsInOrder(false)
			mock.ExpectExec("INSERT INTO build_issue").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"task_id", "unit_id", "fix_id", "version"}).AddRow(tt.taskID, "unit", "", 0))

			require.Error(t, store.Create(context.Background(), tt.buildID, entity.BuildIssue{TaskID: tt.taskID, UnitID: "unit"}))
			_, err = store.Get(context.Background(), tt.buildID, tt.taskID, "unit")
			require.Error(t, err)
		})
	}
}
