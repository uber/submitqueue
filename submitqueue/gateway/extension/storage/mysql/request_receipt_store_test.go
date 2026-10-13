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
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func newRequestReceiptStoreTest(t *testing.T) (sqlmock.Sqlmock, storage.RequestReceiptStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return mock, NewRequestReceiptStore(db, testMetrics(), "monorepo/main")
}

func TestRequestReceiptStoreCreate(t *testing.T) {
	receipt := entity.RequestReceipt{Queue: "monorepo/main", ReceivedAtMs: 1000, RequestID: "request/monorepo/main/1"}
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
			mock, store := newRequestReceiptStoreTest(t)
			write := mock.ExpectExec("INSERT INTO request_receipt (queue, received_at_ms, request_id) VALUES (?, ?, ?)").
				WithArgs(receipt.Queue, receipt.ReceivedAtMs, receipt.RequestID)
			if tt.err != nil {
				write.WillReturnError(tt.err)
			} else {
				write.WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err := store.Create(context.Background(), receipt)
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestReceiptStoreRejectsInvalidMappings(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receipt entity.RequestReceipt
	}{
		{"wrong queue", entity.RequestReceipt{Queue: "other", ReceivedAtMs: 1000, RequestID: "request/1"}},
		{"unknown time", entity.RequestReceipt{Queue: "monorepo/main", RequestID: "request/1"}},
		{"negative time", entity.RequestReceipt{Queue: "monorepo/main", ReceivedAtMs: -1, RequestID: "request/1"}},
		{"missing ID", entity.RequestReceipt{Queue: "monorepo/main", ReceivedAtMs: 1000}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestReceiptStoreTest(t)
			require.Error(t, store.Create(context.Background(), tt.receipt))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRequestReceiptStoreList(t *testing.T) {
	const rangeQuery = `SELECT queue, received_at_ms, request_id FROM request_receipt
		WHERE queue = ? AND received_at_ms >= ? AND received_at_ms < ?`
	const cursorCondition = ` AND (received_at_ms < ? OR (received_at_ms = ? AND request_id < ?))`
	const orderAndLimit = ` ORDER BY received_at_ms DESC, request_id DESC LIMIT ?`
	queryErr := errors.New("query failed")
	rowErr := errors.New("iteration failed")
	first := entity.RequestReceipt{Queue: "monorepo/main", ReceivedAtMs: 2000, RequestID: "request/9"}
	second := entity.RequestReceipt{Queue: "monorepo/main", ReceivedAtMs: 2000, RequestID: "request/10"}
	for _, tt := range []struct {
		name   string
		cursor storage.RequestReceiptCursor
		rows   *sqlmock.Rows
		err    error
		want   []entity.RequestReceipt
		fails  bool
	}{
		{"first page", storage.RequestReceiptCursor{}, receiptRows(first, second), nil, []entity.RequestReceipt{first, second}, false},
		{"continuation", storage.RequestReceiptCursor{ReceivedAtMs: first.ReceivedAtMs, RequestID: first.RequestID}, receiptRows(second), nil, []entity.RequestReceipt{second}, false},
		{"empty", storage.RequestReceiptCursor{}, receiptRows(), nil, []entity.RequestReceipt{}, false},
		{"query failure", storage.RequestReceiptCursor{}, nil, queryErr, nil, true},
		{"scan failure", storage.RequestReceiptCursor{}, receiptRows().AddRow("monorepo/main", "not a timestamp", "request/1"), nil, nil, true},
		{"iteration failure", storage.RequestReceiptCursor{}, receiptRows(first, second).RowError(1, rowErr), nil, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestReceiptStoreTest(t)
			bounds := storage.RequestReceiptRange{ReceivedAtOrAfterMs: 1000, ReceivedBeforeMs: 3000, Before: tt.cursor, Limit: 2}
			query := rangeQuery
			args := []driver.Value{"monorepo/main", int64(1000), int64(3000)}
			if tt.cursor.ReceivedAtMs != 0 {
				query += cursorCondition
				args = append(args, tt.cursor.ReceivedAtMs, tt.cursor.ReceivedAtMs, tt.cursor.RequestID)
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

func receiptRows(receipts ...entity.RequestReceipt) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"queue", "received_at_ms", "request_id"})
	for _, receipt := range receipts {
		rows.AddRow(receipt.Queue, receipt.ReceivedAtMs, receipt.RequestID)
	}
	return rows
}

func TestRequestReceiptStoreRejectsInvalidRanges(t *testing.T) {
	for _, tt := range []struct {
		name   string
		bounds storage.RequestReceiptRange
	}{
		{"empty range", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 1000, ReceivedBeforeMs: 1000, Limit: 1}},
		{"inverted range", storage.RequestReceiptRange{ReceivedAtOrAfterMs: 2000, ReceivedBeforeMs: 1000, Limit: 1}},
		{"zero limit", storage.RequestReceiptRange{ReceivedBeforeMs: 2000}},
		{"negative limit", storage.RequestReceiptRange{ReceivedBeforeMs: 2000, Limit: -1}},
		{"cursor missing ID", storage.RequestReceiptRange{ReceivedBeforeMs: 2000, Limit: 1, Before: storage.RequestReceiptCursor{ReceivedAtMs: 1000}}},
		{"cursor missing time", storage.RequestReceiptRange{ReceivedBeforeMs: 2000, Limit: 1, Before: storage.RequestReceiptCursor{RequestID: "request/1"}}},
		{"cursor negative time", storage.RequestReceiptRange{ReceivedBeforeMs: 2000, Limit: 1, Before: storage.RequestReceiptCursor{ReceivedAtMs: -1, RequestID: "request/1"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock, store := newRequestReceiptStoreTest(t)
			_, err := store.List(context.Background(), tt.bounds)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
