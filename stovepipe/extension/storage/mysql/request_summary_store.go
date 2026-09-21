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

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type requestSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
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
		return fmt.Errorf("request summary %q queue %q does not match the store's bound queue %q", summary.RequestID, summary.Queue, s.queue)
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO request_summary (
			queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		summary.Queue,
		summary.RequestID,
		summary.URI,
		summary.BaseURI,
		summary.State,
		summary.RequestVersion,
		summary.StateTimestampMs,
		summary.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("request summary request_id=%q: %w", summary.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request summary request_id=%q: %w", summary.RequestID, err)
	}
	return nil
}

func (s *requestSummaryStore) Get(ctx context.Context, requestID string) (ret entity.RequestSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	err := s.db.QueryRowContext(ctx, `
		SELECT queue, request_id, uri, base_uri, state, request_version, state_timestamp_ms, version
		FROM request_summary
		WHERE queue = ? AND request_id = ?`,
		s.queue, requestID,
	).Scan(
		&ret.Queue,
		&ret.RequestID,
		&ret.URI,
		&ret.BaseURI,
		&ret.State,
		&ret.RequestVersion,
		&ret.StateTimestampMs,
		&ret.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to get request summary request_id=%q: %w", requestID, err)
	}
	return ret, nil
}

func (s *requestSummaryStore) Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if summary.Queue != s.queue {
		return fmt.Errorf("request summary %q queue %q does not match the store's bound queue %q", summary.RequestID, summary.Queue, s.queue)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE request_summary
		SET base_uri = ?, state = ?, request_version = ?, state_timestamp_ms = ?, version = ?
		WHERE queue = ? AND request_id = ? AND version = ?`,
		summary.BaseURI,
		summary.State,
		summary.RequestVersion,
		summary.StateTimestampMs,
		newVersion,
		summary.Queue,
		summary.RequestID,
		oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update request summary request_id=%q old_version=%d new_version=%d: %w", summary.RequestID, oldVersion, newVersion, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get request summary update rows request_id=%q: %w", summary.RequestID, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("request summary request_id=%q expected_version=%d: %w", summary.RequestID, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}
