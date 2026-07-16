// Copyright (c) 2025 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestQueueSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestQueueSummaryStore creates a MySQL-backed RequestQueueSummaryStore.
func NewRequestQueueSummaryStore(db *sql.DB, scope tally.Scope) storage.RequestQueueSummaryStore {
	return &requestQueueSummaryStore{db: db, scope: scope}
}

func (s *requestQueueSummaryStore) Create(ctx context.Context, summary entity.RequestQueueSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(summary.ChangeURIs, summary.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal queue summary request_id=%s: %w", summary.RequestID, err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO request_summary_by_queue (
			queue, received_at_ms, request_id, change_uris, status,
			version, last_error, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		summary.Queue, summary.ReceivedAtMs, summary.RequestID, changeURIsJSON,
		summary.Status, summary.Version, summary.LastError, metadataJSON,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("queue summary queue=%s received_at_ms=%d request_id=%s: %w", summary.Queue, summary.ReceivedAtMs, summary.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert queue summary request_id=%s: %w", summary.RequestID, err)
	}
	return nil
}

func (s *requestQueueSummaryStore) Get(ctx context.Context, queue string, receivedAtMs int64, requestID string) (ret entity.RequestQueueSummary, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	var changeURIsJSON []byte
	var metadataJSON []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT queue, received_at_ms, request_id, change_uris, status,
			version, last_error, metadata
		FROM request_summary_by_queue
		WHERE queue = ? AND received_at_ms = ? AND request_id = ?`, queue, receivedAtMs, requestID,
	).Scan(&ret.Queue, &ret.ReceivedAtMs, &ret.RequestID, &changeURIsJSON, &ret.Status, &ret.Version, &ret.LastError, &metadataJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestQueueSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestQueueSummary{}, fmt.Errorf("failed to get queue summary queue=%s received_at_ms=%d request_id=%s: %w", queue, receivedAtMs, requestID, err)
	}
	if err := unmarshalSummaryJSON(changeURIsJSON, metadataJSON, &ret.ChangeURIs, &ret.Metadata); err != nil {
		return entity.RequestQueueSummary{}, fmt.Errorf("failed to decode queue summary request_id=%s: %w", requestID, err)
	}
	return ret, nil
}

func (s *requestQueueSummaryStore) Update(ctx context.Context, summary entity.RequestQueueSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	metadataJSON, err := json.Marshal(normalizeMetadata(summary.Metadata))
	if err != nil {
		return fmt.Errorf("failed to marshal queue summary metadata request_id=%s: %w", summary.RequestID, err)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE request_summary_by_queue
		SET status = ?, version = ?, last_error = ?, metadata = ?
		WHERE queue = ? AND received_at_ms = ? AND request_id = ? AND version = ?`,
		summary.Status, newVersion, summary.LastError, metadataJSON,
		summary.Queue, summary.ReceivedAtMs, summary.RequestID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update queue summary request_id=%s old_version=%d new_version=%d: %w", summary.RequestID, oldVersion, newVersion, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get queue summary update rows request_id=%s: %w", summary.RequestID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("queue summary request_id=%s expected_version=%d: %w", summary.RequestID, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}

func (s *requestQueueSummaryStore) List(ctx context.Context, query storage.RequestQueueSummaryQuery) (ret []entity.RequestQueueSummary, retErr error) {
	op := metrics.Begin(s.scope, "list")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	statement := `
		SELECT queue, received_at_ms, request_id, change_uris, status,
			version, last_error, metadata
		FROM request_summary_by_queue
		WHERE queue = ? AND received_at_ms >= ? AND received_at_ms < ?`
	args := []any{query.Queue, query.ReceivedAtOrAfterMs, query.ReceivedBeforeMs}
	if query.HasCursor {
		statement += " AND (received_at_ms < ? OR (received_at_ms = ? AND request_id < ?))"
		args = append(args, query.Cursor.ReceivedAtMs, query.Cursor.ReceivedAtMs, query.Cursor.RequestID)
	}
	statement += " ORDER BY received_at_ms DESC, request_id DESC LIMIT ?"
	args = append(args, query.Limit)

	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list queue summaries queue=%s: %w", query.Queue, err)
	}
	defer rows.Close()

	results := make([]entity.RequestQueueSummary, 0)
	for rows.Next() {
		var summary entity.RequestQueueSummary
		var changeURIsJSON []byte
		var metadataJSON []byte
		if err := rows.Scan(&summary.Queue, &summary.ReceivedAtMs, &summary.RequestID, &changeURIsJSON, &summary.Status, &summary.Version, &summary.LastError, &metadataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan queue summary queue=%s: %w", query.Queue, err)
		}
		if err := unmarshalSummaryJSON(changeURIsJSON, metadataJSON, &summary.ChangeURIs, &summary.Metadata); err != nil {
			return nil, fmt.Errorf("failed to decode queue summary request_id=%s: %w", summary.RequestID, err)
		}
		results = append(results, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue summaries queue=%s: %w", query.Queue, err)
	}
	return results, nil
}
