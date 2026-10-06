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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func newRequestAcceptanceStoreTest(t *testing.T) (sqlmock.Sqlmock, storage.RequestAcceptanceStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return mock, NewRequestAcceptanceStore(db, testMetrics(), "monorepo/main")
}

func TestRequestAcceptanceStoreCreate(t *testing.T) {
	acceptance := entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: 1000, RequestID: "request/monorepo/main/1"}
	writeErr := errors.New("write failed")
	for _, tt := range []struct {
		name string
		err  error
		want error
	}{
		{name: "created"},
		{name: "duplicate", err: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, want: storage.ErrAlreadyExists},
		{name: "write failure", err: writeErr, want: writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestAcceptanceStoreTest(t)
			write := mock.ExpectExec("INSERT INTO request_acceptance (queue, accepted_at_ms, request_id) VALUES (?, ?, ?)").
				WithArgs(acceptance.Queue, acceptance.AcceptedAtMs, acceptance.RequestID)
			if tt.err != nil {
				write.WillReturnError(tt.err)
			} else {
				write.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err := store.Create(context.Background(), acceptance)
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestAcceptanceStoreRejectsInvalidMappings(t *testing.T) {
	for _, tt := range []struct {
		name       string
		acceptance entity.RequestAcceptance
	}{
		{"wrong queue", entity.RequestAcceptance{Queue: "other", AcceptedAtMs: 1000, RequestID: "request/1"}},
		{"unknown time", entity.RequestAcceptance{Queue: "monorepo/main", RequestID: "request/1"}},
		{"negative time", entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: -1, RequestID: "request/1"}},
		{"missing ID", entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: 1000}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestAcceptanceStoreTest(t)
			require.Error(t, store.Create(context.Background(), tt.acceptance))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestAcceptanceStoreList(t *testing.T) {
	const rangeQuery = `SELECT queue, accepted_at_ms, request_id FROM request_acceptance
		WHERE queue = ? AND accepted_at_ms >= ? AND accepted_at_ms < ?`
	const cursorCondition = ` AND (accepted_at_ms < ? OR (accepted_at_ms = ? AND request_id < ?))`
	const orderAndLimit = ` ORDER BY accepted_at_ms DESC, request_id DESC LIMIT ?`
	queryErr := errors.New("query failed")
	rowErr := errors.New("iteration failed")
	first := entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: 2000, RequestID: "request/9"}
	second := entity.RequestAcceptance{Queue: "monorepo/main", AcceptedAtMs: 2000, RequestID: "request/10"}
	for _, tt := range []struct {
		name   string
		cursor storage.RequestAcceptanceCursor
		rows   *sqlmock.Rows
		err    error
		want   []entity.RequestAcceptance
		fails  bool
	}{
		{"first page", storage.RequestAcceptanceCursor{}, acceptanceRows(first, second), nil, []entity.RequestAcceptance{first, second}, false},
		{"continuation", storage.RequestAcceptanceCursor{AcceptedAtMs: first.AcceptedAtMs, RequestID: first.RequestID}, acceptanceRows(second), nil, []entity.RequestAcceptance{second}, false},
		{"empty", storage.RequestAcceptanceCursor{}, acceptanceRows(), nil, []entity.RequestAcceptance{}, false},
		{"query failure", storage.RequestAcceptanceCursor{}, nil, queryErr, nil, true},
		{"scan failure", storage.RequestAcceptanceCursor{}, acceptanceRows().AddRow("monorepo/main", "not a timestamp", "request/1"), nil, nil, true},
		{"iteration failure", storage.RequestAcceptanceCursor{}, acceptanceRows(first, second).RowError(1, rowErr), nil, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestAcceptanceStoreTest(t)
			bounds := storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: 1000, AcceptedBeforeMs: 3000, Before: tt.cursor, Limit: 2}
			query := rangeQuery
			args := []driver.Value{"monorepo/main", int64(1000), int64(3000)}
			if tt.cursor.AcceptedAtMs != 0 {
				query += cursorCondition
				args = append(args, tt.cursor.AcceptedAtMs, tt.cursor.AcceptedAtMs, tt.cursor.RequestID)
			}
			query += orderAndLimit
			args = append(args, 2)
			read := mock.ExpectQuery(query).WithArgs(args...)
			if tt.err != nil {
				read.WillReturnError(tt.err)
			} else {
				read.WillReturnRows(tt.rows).RowsWillBeClosed()
			}
			got, err := store.List(context.Background(), bounds)
			if tt.fails {
				require.Error(t, err)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func acceptanceRows(acceptances ...entity.RequestAcceptance) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"queue", "accepted_at_ms", "request_id"})
	for _, acceptance := range acceptances {
		rows.AddRow(acceptance.Queue, acceptance.AcceptedAtMs, acceptance.RequestID)
	}
	return rows
}

func TestRequestAcceptanceStoreRejectsInvalidRanges(t *testing.T) {
	for _, tt := range []struct {
		name   string
		bounds storage.RequestAcceptanceRange
	}{
		{"negative lower", storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: -1, AcceptedBeforeMs: 2000, Limit: 1}},
		{"empty range", storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: 1000, AcceptedBeforeMs: 1000, Limit: 1}},
		{"inverted range", storage.RequestAcceptanceRange{AcceptedAtOrAfterMs: 2000, AcceptedBeforeMs: 1000, Limit: 1}},
		{"zero limit", storage.RequestAcceptanceRange{AcceptedBeforeMs: 2000}},
		{"negative limit", storage.RequestAcceptanceRange{AcceptedBeforeMs: 2000, Limit: -1}},
		{"cursor missing ID", storage.RequestAcceptanceRange{AcceptedBeforeMs: 2000, Limit: 1, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: 1000}}},
		{"cursor missing time", storage.RequestAcceptanceRange{AcceptedBeforeMs: 2000, Limit: 1, Before: storage.RequestAcceptanceCursor{RequestID: "request/1"}}},
		{"cursor negative time", storage.RequestAcceptanceRange{AcceptedBeforeMs: 2000, Limit: 1, Before: storage.RequestAcceptanceCursor{AcceptedAtMs: -1, RequestID: "request/1"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestAcceptanceStoreTest(t)
			_, err := store.List(context.Background(), tt.bounds)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
