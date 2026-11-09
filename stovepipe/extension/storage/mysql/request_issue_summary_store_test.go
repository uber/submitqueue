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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestRequestIssueCreate(t *testing.T) {
	for _, dbErr := range []error{nil, &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, assert.AnError} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		store := NewRequestIssueSummaryStore(db, testMetrics(), "queue")
		issue := entity.RequestIssueSummary{RequestID: "12", TaskID: "test", Kind: entity.RequestIssueKindNew, BuildID: "b/12", FirstSeenBuildID: "b/12"}
		query := mock.ExpectExec("INSERT INTO request_issue_summary").WithArgs("queue", "12", "test", "", "new", "b/12", "b/12", int64(0))
		if dbErr == nil {
			query.WillReturnResult(sqlmock.NewResult(0, 1))
		} else {
			query.WillReturnError(dbErr)
		}
		err = store.Create(context.Background(), issue)
		switch dbErr {
		case nil:
			require.NoError(t, err)
		case assert.AnError:
			require.ErrorIs(t, err, assert.AnError)
		default:
			require.ErrorIs(t, err, storage.ErrAlreadyExists)
		}
		require.NoError(t, mock.ExpectationsWereMet())
		db.Close()
	}
}

func TestRequestIssueGet(t *testing.T) {
	for _, dbErr := range []error{nil, sql.ErrNoRows, assert.AnError} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		store := NewRequestIssueSummaryStore(db, testMetrics(), "queue")
		query := mock.ExpectQuery("SELECT .* FROM request_issue_summary WHERE queue = \\? AND request_id = \\? AND task_id = \\? AND unit_id = \\?").WithArgs("queue", "12", "test", "")
		if dbErr == nil {
			query.WillReturnRows(sqlmock.NewRows([]string{"request_id", "task_id", "unit_id", "kind", "build_id", "first_seen_build_id", "version"}).AddRow("12", "test", "", "existing", "b/12", "b/10", int64(0)))
		} else {
			query.WillReturnError(dbErr)
		}
		got, err := store.Get(context.Background(), "12", "test", "")
		switch dbErr {
		case nil:
			require.NoError(t, err)
			require.Equal(t, entity.RequestIssueKindExisting, got.Kind)
		case sql.ErrNoRows:
			require.ErrorIs(t, err, storage.ErrNotFound)
		default:
			require.ErrorIs(t, err, dbErr)
		}
		require.NoError(t, mock.ExpectationsWereMet())
		db.Close()
	}
}

func TestRequestIssueList(t *testing.T) {
	for _, after := range []storage.RequestIssueSummaryCursor{{}, {TaskID: "test", UnitID: "unit"}} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		store := NewRequestIssueSummaryStore(db, testMetrics(), "queue")
		var query *sqlmock.ExpectedQuery
		if after.TaskID == "" {
			query = mock.ExpectQuery(regexp.QuoteMeta(listRequestIssueSummariesQuery)).WithArgs("queue", "12", 2)
		} else {
			query = mock.ExpectQuery(regexp.QuoteMeta(listRequestIssueSummariesAfterCursorQuery)).WithArgs("queue", "12", "test", "test", "unit", 2)
		}
		query.WillReturnRows(sqlmock.NewRows([]string{"request_id", "task_id", "unit_id", "kind", "build_id", "first_seen_build_id", "version"}).AddRow("12", "test", "z", "closed", "b/12", "b/10", int64(0)))
		got, err := store.List(context.Background(), "12", storage.RequestIssueSummaryRange{After: after, Limit: 2})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, entity.RequestIssueKindClosed, got[0].Kind)
		require.NoError(t, mock.ExpectationsWereMet())
		db.Close()
	}
}

func TestRequestIssueInvalidBounds(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := NewRequestIssueSummaryStore(db, testMetrics(), "queue")
	for _, bounds := range []storage.RequestIssueSummaryRange{{}, {Limit: -1}, {Limit: 1, After: storage.RequestIssueSummaryCursor{UnitID: "u"}}} {
		_, err := store.List(context.Background(), "12", bounds)
		require.Error(t, err)
	}
	_, err = store.List(context.Background(), "not-a-request", storage.RequestIssueSummaryRange{Limit: 1})
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRequestIssueSummaryUpdate(t *testing.T) {
	for _, affected := range []int64{0, 1} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		summary := entity.RequestIssueSummary{RequestID: "12", TaskID: "test", Kind: entity.RequestIssueKindClosed, BuildID: "b/12", FirstSeenBuildID: "b/10", Version: 3}
		mock.ExpectExec("UPDATE request_issue_summary").WithArgs("closed", "b/12", "b/10", int64(4), "queue", "12", "test", "", int64(3)).WillReturnResult(sqlmock.NewResult(0, affected))
		err = NewRequestIssueSummaryStore(db, testMetrics(), "queue").Update(context.Background(), summary, 3, 4)
		if affected == 0 {
			require.ErrorIs(t, err, storage.ErrVersionMismatch)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, int32(3), summary.Version)
		require.NoError(t, mock.ExpectationsWereMet())
		db.Close()
	}
}
