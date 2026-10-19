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
	"errors"
	"fmt"
	"strings"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// errQueueRequestStateRecordsDisabled is returned by the store a Storage hands out
// when it was not built WithQueueRequestStateRecords: its request writes keep no
// records, so an empty listing would misreport every queue as idle.
var errQueueRequestStateRecordsDisabled = errors.New("queue request state records are not enabled for this storage")

type queueRequestStateStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read is scoped to it.
	queue string
}

// NewQueueRequestStateStore creates a new MySQL-backed QueueRequestStateStore.
func NewQueueRequestStateStore(db *sql.DB, scope tally.Scope, queue string) storage.QueueRequestStateStore {
	return &queueRequestStateStore{db: db, scope: scope, queue: queue}
}

// List returns every record filed under the bound queue's state. The WHERE clause
// is a prefix of the (queue, state, request_id) PK, so this is a PK-prefix scan.
func (s *queueRequestStateStore) List(ctx context.Context, state entity.RequestState) (ret []entity.QueueRequestState, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { storage.CompleteMetricsOp(op, retErr) }()

	const query = "SELECT queue, state, request_id, entered_at_ms FROM queue_request_state WHERE queue = ? AND state = ?"
	rows, err := s.db.QueryContext(ctx, query, s.queue, string(state))
	if err != nil {
		return nil, fmt.Errorf("failed to query queue request state records for queue=%s state=%s: %w", s.queue, state, err)
	}
	defer rows.Close()

	var results []entity.QueueRequestState
	for rows.Next() {
		var rec entity.QueueRequestState
		if err := rows.Scan(&rec.Queue, &rec.State, &rec.RequestID, &rec.EnteredAtMs); err != nil {
			return nil, fmt.Errorf("failed to scan queue request state record for queue=%s state=%s: %w", s.queue, state, err)
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue request state records for queue=%s state=%s: %w", s.queue, state, err)
	}
	return results, nil
}

type disabledQueueRequestStateStore struct{}

func (disabledQueueRequestStateStore) List(context.Context, entity.RequestState) ([]entity.QueueRequestState, error) {
	return nil, errQueueRequestStateRecordsDisabled
}

// fileQueueRequestStateRecord makes the request's membership record match state, within
// the caller's transaction: it removes the record from every other in-queue state and,
// unless state is terminal, files it under state. INSERT IGNORE keeps the original
// EnteredAtMs when a version-only update leaves the state unchanged.
func fileQueueRequestStateRecord(ctx context.Context, tx *sql.Tx, queue, requestID string, state entity.RequestState, nowMs int64) error {
	others := make([]any, 0, len(entity.InQueueRequestStates))
	for _, s := range entity.InQueueRequestStates {
		if s != state {
			others = append(others, string(s))
		}
	}
	deleteQuery := "DELETE FROM queue_request_state WHERE queue = ? AND state IN (?" + strings.Repeat(", ?", len(others)-1) + ") AND request_id = ?"
	args := append(append([]any{queue}, others...), requestID)
	if _, err := tx.ExecContext(ctx, deleteQuery, args...); err != nil {
		return fmt.Errorf("failed to remove stale queue request state records for queue=%s request_id=%s: %w", queue, requestID, err)
	}
	if entity.IsRequestStateTerminal(state) {
		return nil
	}
	const insertQuery = "INSERT IGNORE INTO queue_request_state (queue, state, request_id, entered_at_ms) VALUES (?, ?, ?, ?)"
	if _, err := tx.ExecContext(ctx, insertQuery, queue, string(state), requestID, nowMs); err != nil {
		return fmt.Errorf("failed to file queue request state record for queue=%s request_id=%s state=%s: %w", queue, requestID, state, err)
	}
	return nil
}
