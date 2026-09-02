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

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listRequestLogQuery = `
	SELECT queue, request_id, log_id, timestamp_ms, state, event,
		request_version, outcome_reason, metadata
	FROM request_log
	WHERE queue = ? AND request_id = ?
	ORDER BY timestamp_ms ASC, log_id ASC`

type requestLogStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestLogStore creates a MySQL-backed RequestLogStore.
func NewRequestLogStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestLogStore {
	return &requestLogStore{db: db, scope: scope, queue: queue}
}

func (r *requestLogStore) Create(ctx context.Context, log entity.RequestLog) (retErr error) {
	op := metrics.Begin(r.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := log.Validate(); err != nil {
		return fmt.Errorf("invalid request log id=%q: %w", log.ID, err)
	}
	if log.Queue != r.queue {
		return fmt.Errorf("request log %q queue %q does not match the store's bound queue %q", log.ID, log.Queue, r.queue)
	}
	metadata := log.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal request log metadata request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO request_log (
			queue, request_id, log_id, timestamp_ms, state, event, request_version,
			outcome_reason, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		log.Queue,
		log.RequestID,
		log.ID,
		log.TimestampMs,
		log.State,
		log.Event,
		log.RequestVersion,
		log.OutcomeReason,
		metadataJSON,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request log request_id=%q log_id=%q: %w", log.RequestID, log.ID, err)
	}
	return nil
}

func (r *requestLogStore) Get(ctx context.Context, requestID, logID string) (ret entity.RequestLog, retErr error) {
	op := metrics.Begin(r.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	log, err := scanRequestLog(r.db.QueryRowContext(ctx, `
		SELECT queue, request_id, log_id, timestamp_ms, state, event,
			request_version, outcome_reason, metadata
		FROM request_log
		WHERE queue = ? AND request_id = ? AND log_id = ?`,
		r.queue, requestID, logID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestLog{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestLog{}, fmt.Errorf("failed to get request log request_id=%q log_id=%q: %w", requestID, logID, err)
	}
	return log, nil
}

func (r *requestLogStore) List(ctx context.Context, requestID string) (ret []entity.RequestLog, retErr error) {
	op := metrics.Begin(r.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if requestID == "" {
		return nil, fmt.Errorf("request log request ID must not be empty")
	}

	rows, err := r.db.QueryContext(ctx, listRequestLogQuery, r.queue, requestID)
	if err != nil {
		return nil, fmt.Errorf("failed to list request log request_id=%q: %w", requestID, err)
	}
	defer rows.Close()

	logs := make([]entity.RequestLog, 0)
	for rows.Next() {
		log, err := scanRequestLog(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan request log request_id=%q: %w", requestID, err)
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request log request_id=%q: %w", requestID, err)
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("no request log records for queue=%q request_id=%q: %w", r.queue, requestID, storage.ErrNotFound)
	}
	return logs, nil
}

type requestLogScanner interface {
	Scan(dest ...any) error
}

func scanRequestLog(scanner requestLogScanner) (entity.RequestLog, error) {
	var log entity.RequestLog
	var metadataJSON []byte
	err := scanner.Scan(
		&log.Queue,
		&log.RequestID,
		&log.ID,
		&log.TimestampMs,
		&log.State,
		&log.Event,
		&log.RequestVersion,
		&log.OutcomeReason,
		&metadataJSON,
	)
	if err != nil {
		return entity.RequestLog{}, err
	}
	if err := json.Unmarshal(metadataJSON, &log.Metadata); err != nil {
		return entity.RequestLog{}, fmt.Errorf("failed to unmarshal request log metadata: %w", err)
	}
	if log.Metadata == nil {
		log.Metadata = map[string]string{}
	}
	return log, nil
}
