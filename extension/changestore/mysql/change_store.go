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

// Create inserts a batch of ChangeRecords. Primary-key conflicts on (uri, request_id)
// are silently ignored via INSERT IGNORE so queue-redelivery of the same request is a no-op.
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
		// Pass empty Metadata as NULL — JSON column rejects empty string but accepts NULL.
		var metadata any
		if r.Metadata != "" {
			metadata = r.Metadata
		}
		args = append(args, r.URI, r.RequestID, r.Queue, metadata, r.CreatedAt, r.UpdatedAt)
	}

	query := "INSERT IGNORE INTO `change` (uri, request_id, queue, metadata, created_at, updated_at) VALUES " + placeholders
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to insert change records (count=%d): %w", len(records), err)
	}
	return nil
}

// FindOverlapping returns ChangeRecords whose uri is in the given set, scoped to queue,
// excluding any belonging to excludeRequestID.
func (s *changeStore) FindOverlapping(
	ctx context.Context,
	queue string,
	uris []string,
	excludeRequestID string,
) (ret []entity.ChangeRecord, retErr error) {
	op := metrics.Begin(s.scope, "find_overlapping")
	defer func() { op.Complete(retErr) }()

	if len(uris) == 0 {
		return nil, nil
	}

	uriPlaceholders := "?" + strings.Repeat(", ?", len(uris)-1)
	query := "SELECT uri, request_id, queue, metadata, created_at, updated_at FROM `change` " +
		"WHERE uri IN (" + uriPlaceholders + ") AND queue = ? AND request_id != ?"

	args := make([]any, 0, len(uris)+2)
	for _, u := range uris {
		args = append(args, u)
	}
	args = append(args, queue, excludeRequestID)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query overlapping changes for queue=%s: %w", queue, err)
	}
	defer rows.Close()

	var results []entity.ChangeRecord
	for rows.Next() {
		var rec entity.ChangeRecord
		var metadata sql.NullString
		if err := rows.Scan(&rec.URI, &rec.RequestID, &rec.Queue, &metadata, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan change record for queue=%s: %w", queue, err)
		}
		if metadata.Valid {
			rec.Metadata = metadata.String
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate change records for queue=%s: %w", queue, err)
	}
	return results, nil
}
