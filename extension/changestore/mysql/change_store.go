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
	"strings"

	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/changestore"
)

type changeStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewChangeStore creates a new MySQL-backed ChangeStore.
func NewChangeStore(db *sql.DB, scope tally.Scope) changestore.ChangeStore {
	return &changeStore{db: db, scope: scope}
}

// Create inserts a batch of ChangeRecords as a single multi-row INSERT IGNORE.
// Primary-key conflicts on (queue, uri, request_id) are silently ignored so
// queue-redelivery of the same request is a no-op. The whole batch is one
// statement, so partial success is not observable.
func (s *changeStore) Create(ctx context.Context, records []entity.ChangeRecord) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	if len(records) == 0 {
		return nil
	}

	const cols = 6
	placeholders := strings.Repeat("(?, ?, ?, ?, ?, ?), ", len(records))
	placeholders = placeholders[:len(placeholders)-2] // trim trailing ", "

	args := make([]any, 0, len(records)*cols)
	for _, r := range records {
		// Use the empty JSON object as the canonical "no metadata yet" value.
		// metadata is NOT NULL in the schema, and an empty Go string would be
		// rejected by the JSON column type.
		metadata := r.Metadata
		if metadata == "" {
			metadata = "{}"
		}
		args = append(args, r.URI, r.RequestID, r.Queue, metadata, r.CreatedAt, r.UpdatedAt)
	}

	query := "INSERT IGNORE INTO `change` (uri, request_id, queue, metadata, created_at, updated_at) VALUES " + placeholders
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to insert change records (count=%d): %w", len(records), err)
	}
	return nil
}

// FindOverlapping returns ChangeRecords whose uri is in the given set, scoped to queue.
// The store does not filter by request_id; callers that want to skip self should do so
// after the call. Liveness checks against the request store are also the caller's job.
func (s *changeStore) FindOverlapping(
	ctx context.Context,
	queue string,
	uris []string,
) (ret []entity.ChangeRecord, retErr error) {
	op := metrics.Begin(s.scope, "find_overlapping")
	defer func() { op.Complete(retErr) }()

	if len(uris) == 0 {
		return nil, nil
	}

	uriPlaceholders := "?" + strings.Repeat(", ?", len(uris)-1)
	// queue leads the WHERE clause to align with the (queue, uri, request_id) PK,
	// so this is a PK-prefix scan.
	query := "SELECT uri, request_id, queue, metadata, created_at, updated_at FROM `change` " +
		"WHERE queue = ? AND uri IN (" + uriPlaceholders + ")"

	args := make([]any, 0, 1+len(uris))
	args = append(args, queue)
	for _, u := range uris {
		args = append(args, u)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query overlapping changes for queue=%s: %w", queue, err)
	}
	defer rows.Close()

	var results []entity.ChangeRecord
	for rows.Next() {
		var rec entity.ChangeRecord
		if err := rows.Scan(&rec.URI, &rec.RequestID, &rec.Queue, &rec.Metadata, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan change record for queue=%s: %w", queue, err)
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate change records for queue=%s: %w", queue, err)
	}
	return results, nil
}
