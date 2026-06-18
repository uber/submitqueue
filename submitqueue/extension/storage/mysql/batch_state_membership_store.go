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
	"fmt"

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type batchStateMembershipStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBatchStateMembershipStore creates a new MySQL-backed BatchStateMembershipStore.
func NewBatchStateMembershipStore(db *sql.DB, scope tally.Scope) storage.BatchStateMembershipStore {
	return &batchStateMembershipStore{db: db, scope: scope}
}

// Add records a batch's state membership. Duplicate membership is a retry-safe no-op.
func (s *batchStateMembershipStore) Add(ctx context.Context, queue string, state entity.BatchState, batchID string) (retErr error) {
	op := metrics.Begin(s.scope, "add")
	defer func() { op.Complete(retErr) }()

	const query = "INSERT INTO batch_state_membership (queue, state, batch_id) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE batch_id = batch_id"
	if _, err := s.db.ExecContext(ctx, query, queue, state, batchID); err != nil {
		return fmt.Errorf("failed to add batch state membership queue=%q state=%q batch_id=%q: %w", queue, state, batchID, err)
	}
	return nil
}

// Remove deletes a batch's state membership. Missing rows are treated as already removed.
func (s *batchStateMembershipStore) Remove(ctx context.Context, queue string, state entity.BatchState, batchID string) (retErr error) {
	op := metrics.Begin(s.scope, "remove")
	defer func() { op.Complete(retErr) }()

	const query = "DELETE FROM batch_state_membership WHERE queue = ? AND state = ? AND batch_id = ?"
	if _, err := s.db.ExecContext(ctx, query, queue, state, batchID); err != nil {
		return fmt.Errorf("failed to remove batch state membership queue=%q state=%q batch_id=%q: %w", queue, state, batchID, err)
	}
	return nil
}

// ListIDs returns batch IDs recorded for a queue and state.
func (s *batchStateMembershipStore) ListIDs(ctx context.Context, queue string, state entity.BatchState) (ret []string, retErr error) {
	op := metrics.Begin(s.scope, "list_ids")
	defer func() { op.Complete(retErr) }()

	const query = "SELECT batch_id FROM batch_state_membership WHERE queue = ? AND state = ?"
	rows, err := s.db.QueryContext(ctx, query, queue, state)
	if err != nil {
		return nil, fmt.Errorf("failed to list batch state memberships queue=%q state=%q: %w", queue, state, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan batch state membership queue=%q state=%q: %w", queue, state, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate batch state memberships queue=%q state=%q: %w", queue, state, err)
	}
	return ids, nil
}
