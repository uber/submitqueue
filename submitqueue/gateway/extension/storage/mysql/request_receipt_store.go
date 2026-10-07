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
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

const listRequestReceiptsQuery = `
	SELECT queue, received_at_ms, request_id
	FROM request_receipt
	WHERE queue = ? AND received_at_ms >= ? AND received_at_ms < ?
	ORDER BY received_at_ms DESC, request_id DESC LIMIT ?`

const listRequestReceiptsBeforeCursorQuery = `
	SELECT queue, received_at_ms, request_id
	FROM request_receipt
	WHERE queue = ? AND received_at_ms >= ? AND received_at_ms < ?
		AND (received_at_ms < ? OR (received_at_ms = ? AND request_id < ?))
	ORDER BY received_at_ms DESC, request_id DESC LIMIT ?`

type requestReceiptStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestReceiptStore creates a MySQL-backed, queue-scoped RequestReceiptStore.
func NewRequestReceiptStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestReceiptStore {
	return &requestReceiptStore{db: db, scope: scope, queue: queue}
}

func (r *requestReceiptStore) Create(ctx context.Context, receipt entity.RequestReceipt) (retErr error) {
	op := metrics.Begin(r.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if receipt.Queue != r.queue {
		return fmt.Errorf("request receipt queue %q does not match the store's bound queue %q", receipt.Queue, r.queue)
	}
	if receipt.ReceivedAtMs <= 0 || receipt.RequestID == "" {
		return fmt.Errorf("request receipt requires a positive timestamp and nonempty request ID")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO request_receipt (queue, received_at_ms, request_id)
		VALUES (?, ?, ?)`, receipt.Queue, receipt.ReceivedAtMs, receipt.RequestID)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request receipt request_id=%q: %w", receipt.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request receipt request_id=%q: %w", receipt.RequestID, err)
	}
	return nil
}

func (r *requestReceiptStore) List(ctx context.Context, bounds storage.RequestReceiptRange) (ret []entity.RequestReceipt, retErr error) {
	op := metrics.Begin(r.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateRequestReceiptRange(bounds); err != nil {
		return nil, err
	}
	return r.queryRequestReceipts(ctx, bounds)
}

func validateRequestReceiptRange(bounds storage.RequestReceiptRange) error {
	if bounds.ReceivedBeforeMs <= bounds.ReceivedAtOrAfterMs || bounds.Limit <= 0 {
		return fmt.Errorf("request receipt range requires ordered bounds and a positive limit")
	}
	cursor := bounds.Before
	if cursor.ReceivedAtMs < 0 || (cursor.ReceivedAtMs == 0) != (cursor.RequestID == "") {
		return fmt.Errorf("request receipt cursor requires a positive timestamp and nonempty request ID, or the zero value")
	}
	return nil
}

func (r *requestReceiptStore) queryRequestReceipts(ctx context.Context, bounds storage.RequestReceiptRange) ([]entity.RequestReceipt, error) {
	var (
		rows *sql.Rows
		err  error
	)
	cursor := bounds.Before
	if cursor.ReceivedAtMs == 0 {
		rows, err = r.db.QueryContext(ctx, listRequestReceiptsQuery,
			r.queue, bounds.ReceivedAtOrAfterMs, bounds.ReceivedBeforeMs, bounds.Limit,
		)
	} else {
		rows, err = r.db.QueryContext(ctx, listRequestReceiptsBeforeCursorQuery,
			r.queue, bounds.ReceivedAtOrAfterMs, bounds.ReceivedBeforeMs,
			cursor.ReceivedAtMs, cursor.ReceivedAtMs, cursor.RequestID, bounds.Limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list request receipts queue=%q: %w", r.queue, err)
	}
	defer rows.Close()
	return scanRequestReceiptRows(rows, r.queue)
}

func scanRequestReceiptRows(rows *sql.Rows, queue string) ([]entity.RequestReceipt, error) {
	receipts := make([]entity.RequestReceipt, 0)
	for rows.Next() {
		var receipt entity.RequestReceipt
		if err := rows.Scan(&receipt.Queue, &receipt.ReceivedAtMs, &receipt.RequestID); err != nil {
			return nil, fmt.Errorf("failed to scan request receipt queue=%q: %w", queue, err)
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request receipts queue=%q: %w", queue, err)
	}
	return receipts, nil
}
