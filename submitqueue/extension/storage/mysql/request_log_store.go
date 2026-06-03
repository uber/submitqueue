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
	"fmt"
	"math/rand/v2"

	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestLogStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestLogStore creates a new MySQL-backed RequestLogStore.
func NewRequestLogStore(db *sql.DB, scope tally.Scope) storage.RequestLogStore {
	return &requestLogStore{db: db, scope: scope}
}

// Insert appends a new request log record. The primary key is (request_id, timestamp_ms, salt).
// Multiple log entries for the same request can share a timestamp (e.g. concurrent writers or
// millisecond-precision collisions), so a random salt is generated to guarantee uniqueness
// without requiring the caller to manage deduplication.
func (r *requestLogStore) Insert(ctx context.Context, log entity.RequestLog) (retErr error) {
	op := metrics.Begin(r.scope, "insert")
	defer func() { op.Complete(retErr) }()

	metadataJSON, err := json.Marshal(log.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata for request log request_id=%s: %w", log.RequestID, err)
	}

	// Generate a random salt to break primary key ties when two inserts share the same
	// (request_id, timestamp_ms). The salt is part of the composite primary key in MySQL
	// but is never exposed through the storage interface or returned to callers.
	salt := rand.Int64()

	_, err = r.db.ExecContext(ctx,
		"INSERT INTO request_log (request_id, timestamp_ms, salt, status, request_version, last_error, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
		log.RequestID, log.TimestampMs, salt, log.Status, log.RequestVersion, log.LastError, metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to insert request log for request_id=%s timestamp_ms=%d: %w", log.RequestID, log.TimestampMs, err)
	}

	return nil
}

// List retrieves all request log records for a given request ID, ordered by timestamp ascending.
// Salt is used as a secondary sort key to provide stable ordering for entries that share a
// timestamp, but it is not included in the SELECT columns and never returned to callers.
func (r *requestLogStore) List(ctx context.Context, requestID string) (ret []entity.RequestLog, retErr error) {
	op := metrics.Begin(r.scope, "list")
	defer func() { op.Complete(retErr) }()

	rows, err := r.db.QueryContext(ctx,
		"SELECT request_id, timestamp_ms, status, request_version, last_error, metadata FROM request_log WHERE request_id = ? ORDER BY timestamp_ms ASC, salt ASC",
		requestID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list request logs for request_id=%s: %w", requestID, err)
	}
	defer rows.Close()

	var logs []entity.RequestLog
	for rows.Next() {
		var log entity.RequestLog
		var metadataJSON []byte

		err := rows.Scan(&log.RequestID, &log.TimestampMs, &log.Status, &log.RequestVersion, &log.LastError, &metadataJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to scan request log row for request_id=%s: %w", requestID, err)
		}

		if err := json.Unmarshal(metadataJSON, &log.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata for request log request_id=%s: %w", requestID, err)
		}

		logs = append(logs, log)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request log rows for request_id=%s: %w", requestID, err)
	}

	if len(logs) == 0 {
		return nil, fmt.Errorf("no request log records for request_id=%s: %w", requestID, storage.ErrNotFound)
	}

	return logs, nil
}
