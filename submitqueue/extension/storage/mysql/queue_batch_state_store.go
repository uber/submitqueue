// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type queueBatchStateStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewQueueBatchStateStore creates a new MySQL-backed QueueBatchStateStore.
func NewQueueBatchStateStore(db *sql.DB, scope tally.Scope, queue string) storage.QueueBatchStateStore {
	return &queueBatchStateStore{db: db, scope: scope, queue: queue}
}

// List returns every record filed under the bound queue's state bucket. The
// WHERE clause is a prefix of the (queue, state, batch_id) PK, so this is a
// PK-prefix scan.
func (s *queueBatchStateStore) List(ctx context.Context, state entity.BatchState) (ret []entity.QueueBatchState, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	queue := s.queue
	const query = "SELECT queue, state, batch_id FROM queue_batch_state WHERE queue = ? AND state = ?"
	rows, err := s.db.QueryContext(ctx, query, queue, string(state))
	if err != nil {
		return nil, fmt.Errorf("failed to query queue batch state records for queue=%s state=%s: %w", queue, state, err)
	}
	defer rows.Close()

	var results []entity.QueueBatchState
	for rows.Next() {
		var rec entity.QueueBatchState
		if err := rows.Scan(&rec.Queue, &rec.State, &rec.BatchID); err != nil {
			return nil, fmt.Errorf("failed to scan queue batch state record for queue=%s state=%s: %w", queue, state, err)
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue batch state records for queue=%s state=%s: %w", queue, state, err)
	}
	return results, nil
}

// Put inserts a record. A primary-key conflict on (queue, state, batch_id) is
// silently ignored via INSERT IGNORE — the record carries no data beyond its
// identity, so re-putting it is a no-op success.
func (s *queueBatchStateStore) Put(ctx context.Context, record entity.QueueBatchState) (retErr error) {
	op := metrics.Begin(s.scope, "put", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if record.Queue != s.queue {
		return fmt.Errorf("queue batch state record batch_id=%s queue %q does not match the store's bound queue %q", record.BatchID, record.Queue, s.queue)
	}

	const query = "INSERT IGNORE INTO queue_batch_state (queue, state, batch_id) VALUES (?, ?, ?)"
	if _, err := s.db.ExecContext(ctx, query, record.Queue, string(record.State), record.BatchID); err != nil {
		return fmt.Errorf("failed to put queue batch state record queue=%s state=%s batch_id=%s: %w", record.Queue, record.State, record.BatchID, err)
	}
	return nil
}

// Delete removes the bound queue's record identified by (state, batchID).
// Deleting an absent record is a no-op success — rows-affected is intentionally
// not checked.
func (s *queueBatchStateStore) Delete(ctx context.Context, state entity.BatchState, batchID string) (retErr error) {
	op := metrics.Begin(s.scope, "delete", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	queue := s.queue
	const query = "DELETE FROM queue_batch_state WHERE queue = ? AND state = ? AND batch_id = ?"
	if _, err := s.db.ExecContext(ctx, query, queue, string(state), batchID); err != nil {
		return fmt.Errorf("failed to delete queue batch state record queue=%s state=%s batch_id=%s: %w", queue, state, batchID, err)
	}
	return nil
}
