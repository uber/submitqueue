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
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type activeRequestStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewActiveRequestStore binds active-request identities to one queue.
func NewActiveRequestStore(db *sql.DB, scope tally.Scope, queue string) storage.ActiveRequestStore {
	return &activeRequestStore{db: db, scope: scope, queue: queue}
}

func (s *activeRequestStore) Create(ctx context.Context, requestID string) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if requestID == "" {
		return fmt.Errorf("active request identity is empty")
	}
	_, err := s.db.ExecContext(ctx, "INSERT INTO active_request (queue, request_id) VALUES (?, ?)", s.queue, requestID)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create active request: %w", err)
	}
	return nil
}

func (s *activeRequestStore) List(ctx context.Context, after string, limit int) (ids []string, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if limit <= 0 {
		return nil, fmt.Errorf("active request limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT request_id FROM active_request WHERE queue = ? AND request_id > ? ORDER BY request_id ASC LIMIT ?",
		s.queue, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list active requests: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan active request: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active requests: %w", err)
	}
	return ids, nil
}

func (s *activeRequestStore) Delete(ctx context.Context, requestID string) (retErr error) {
	op := metrics.Begin(s.scope, "delete", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if requestID == "" {
		return fmt.Errorf("active request identity is empty")
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM active_request WHERE queue = ? AND request_id = ?", s.queue, requestID)
	if err != nil {
		return fmt.Errorf("delete active request: %w", err)
	}
	return nil
}
