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

type queueSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewQueueSummaryStore binds a MySQL queue summary store to one queue.
func NewQueueSummaryStore(db *sql.DB, scope tally.Scope, queue string) storage.QueueSummaryStore {
	return &queueSummaryStore{db: db, scope: scope, queue: queue}
}

func (s *queueSummaryStore) Get(ctx context.Context) (summary entity.QueueSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx,
		"SELECT queue, state, effective_from_commit_uri, state_timestamp_ms, queue_state_version, version FROM queue_summary WHERE queue = ?", s.queue).
		Scan(&summary.Queue, &summary.State, &summary.EffectiveFromCommitURI, &summary.StateTimestampMs, &summary.QueueStateVersion, &summary.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.QueueSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.QueueSummary{}, fmt.Errorf("read queue summary %q: %w", s.queue, err)
	}
	return summary, nil
}

func (s *queueSummaryStore) Create(ctx context.Context, summary entity.QueueSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := s.validateSummary(summary); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO queue_summary (queue, state, effective_from_commit_uri, state_timestamp_ms, queue_state_version, version) VALUES (?, ?, ?, ?, ?, ?)",
		summary.Queue, summary.State, summary.EffectiveFromCommitURI, summary.StateTimestampMs, summary.QueueStateVersion, summary.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create queue summary %q: %w", s.queue, err)
	}
	return nil
}

func (s *queueSummaryStore) Update(ctx context.Context, summary entity.QueueSummary, oldVersion, newVersion int64) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	stored := summary
	stored.Version = newVersion
	if err := s.validateSummary(stored); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		"UPDATE queue_summary SET state = ?, effective_from_commit_uri = ?, state_timestamp_ms = ?, queue_state_version = ?, version = ? WHERE queue = ? AND version = ?",
		summary.State, summary.EffectiveFromCommitURI, summary.StateTimestampMs, summary.QueueStateVersion, newVersion, s.queue, oldVersion)
	if err != nil {
		return fmt.Errorf("update queue summary %q: %w", s.queue, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read queue summary update count: %w", err)
	}
	if count != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *queueSummaryStore) validateSummary(summary entity.QueueSummary) error {
	if summary.Queue != s.queue {
		return fmt.Errorf("summary queue differs from bound queue %q", s.queue)
	}
	return summary.Validate()
}
