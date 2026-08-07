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

type requestSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewRequestSummaryStore creates a MySQL-backed RequestSummaryStore.
func NewRequestSummaryStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestSummaryStore {
	return &requestSummaryStore{db: db, scope: scope, queue: queue}
}

func (s *requestSummaryStore) Create(ctx context.Context, summary entity.RequestSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if summary.Queue != s.queue {
		return fmt.Errorf("request summary request_id=%s queue %q does not match the store's bound queue %q", summary.RequestID, summary.Queue, s.queue)
	}

	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(summary.ChangeURIs, summary.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal request summary metadata request_id=%s: %w", summary.RequestID, err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO request_summary (
			queue, request_id, change_uris, received_at_ms, status, request_version,
			status_timestamp_ms, version,
			last_error, metadata
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		summary.Queue, summary.RequestID, changeURIsJSON, summary.ReceivedAtMs, summary.Status,
		summary.RequestVersion, summary.StatusTimestampMs, summary.Version,
		summary.LastError, metadataJSON,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request summary queue=%s request_id=%s: %w", summary.Queue, summary.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request summary request_id=%s: %w", summary.RequestID, err)
	}

	return nil
}

func (s *requestSummaryStore) Get(ctx context.Context, requestID string) (ret entity.RequestSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var changeURIsJSON []byte
	var metadataJSON []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT queue, request_id, change_uris, received_at_ms, status, request_version,
			status_timestamp_ms, version,
			last_error, metadata
		FROM request_summary
		WHERE queue = ? AND request_id = ?`, s.queue, requestID,
	).Scan(
		&ret.Queue, &ret.RequestID, &changeURIsJSON, &ret.ReceivedAtMs, &ret.Status,
		&ret.RequestVersion, &ret.StatusTimestampMs, &ret.Version,
		&ret.LastError, &metadataJSON,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to get request summary queue=%s request_id=%s: %w", s.queue, requestID, err)
	}
	if err := unmarshalSummaryJSON(changeURIsJSON, metadataJSON, &ret.ChangeURIs, &ret.Metadata); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to decode request summary request_id=%s: %w", requestID, err)
	}

	return ret, nil
}

func (s *requestSummaryStore) Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if summary.Queue != s.queue {
		return fmt.Errorf("request summary request_id=%s queue %q does not match the store's bound queue %q", summary.RequestID, summary.Queue, s.queue)
	}

	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(summary.ChangeURIs, summary.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal request summary request_id=%s: %w", summary.RequestID, err)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE request_summary
		SET change_uris = ?, received_at_ms = ?, status = ?,
			request_version = ?, status_timestamp_ms = ?, version = ?,
			last_error = ?, metadata = ?
		WHERE queue = ? AND request_id = ? AND version = ?`,
		changeURIsJSON, summary.ReceivedAtMs, summary.Status,
		summary.RequestVersion, summary.StatusTimestampMs, newVersion,
		summary.LastError, metadataJSON,
		summary.Queue, summary.RequestID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update request summary request_id=%s old_version=%d new_version=%d: %w", summary.RequestID, oldVersion, newVersion, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get request summary update rows request_id=%s: %w", summary.RequestID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("request summary request_id=%s expected_version=%d: %w", summary.RequestID, oldVersion, storage.ErrVersionMismatch)
	}

	return nil
}

func marshalSummaryJSON(changeURIs []string, metadata map[string]string) ([]byte, []byte, error) {
	changeURIsJSON, err := json.Marshal(normalizeChangeURIs(changeURIs))
	if err != nil {
		return nil, nil, err
	}
	metadataJSON, err := json.Marshal(normalizeMetadata(metadata))
	if err != nil {
		return nil, nil, err
	}
	return changeURIsJSON, metadataJSON, nil
}

func unmarshalSummaryJSON(changeURIsJSON, metadataJSON []byte, changeURIs *[]string, metadata *map[string]string) error {
	if err := json.Unmarshal(changeURIsJSON, changeURIs); err != nil {
		return err
	}
	if err := json.Unmarshal(metadataJSON, metadata); err != nil {
		return err
	}
	*changeURIs = normalizeChangeURIs(*changeURIs)
	*metadata = normalizeMetadata(*metadata)
	return nil
}

func normalizeChangeURIs(changeURIs []string) []string {
	if changeURIs == nil {
		return []string{}
	}
	return changeURIs
}

func normalizeMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	return metadata
}
