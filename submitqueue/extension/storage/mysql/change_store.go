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

	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type changeStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewChangeStore creates a new MySQL-backed ChangeStore.
func NewChangeStore(db *sql.DB, scope tally.Scope) storage.ChangeStore {
	return &changeStore{db: db, scope: scope}
}

// Create inserts a single ChangeRecord. A primary-key conflict on
// (queue, uri, request_id) is silently ignored via INSERT IGNORE, so
// queue-redelivery of the same request is a no-op.
func (s *changeStore) Create(ctx context.Context, record entity.ChangeRecord) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	// Use the empty JSON object as the canonical "no metadata yet" value.
	// metadata is NOT NULL in the schema and the JSON column type rejects an empty string.
	metadata := record.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	const query = "INSERT IGNORE INTO `change` (uri, request_id, queue, metadata, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?, ?, ?)"
	if _, err := s.db.ExecContext(ctx, query,
		record.URI, record.RequestID, record.Queue, metadata, record.CreatedAt, record.UpdatedAt, record.Version,
	); err != nil {
		return fmt.Errorf("failed to insert change record uri=%s request_id=%s: %w", record.URI, record.RequestID, err)
	}
	return nil
}

// GetByURI returns every ChangeRecord for (queue, uri). queue leads the WHERE
// clause to align with the (queue, uri, request_id) PK so this is a PK-prefix scan.
func (s *changeStore) GetByURI(ctx context.Context, queue string, uri string) (ret []entity.ChangeRecord, retErr error) {
	op := metrics.Begin(s.scope, "get_by_uri")
	defer func() { op.Complete(retErr) }()

	const query = "SELECT uri, request_id, queue, metadata, created_at, updated_at, version FROM `change` WHERE queue = ? AND uri = ?"
	rows, err := s.db.QueryContext(ctx, query, queue, uri)
	if err != nil {
		return nil, fmt.Errorf("failed to query change records for queue=%s uri=%s: %w", queue, uri, err)
	}
	defer rows.Close()

	var results []entity.ChangeRecord
	for rows.Next() {
		var rec entity.ChangeRecord
		if err := rows.Scan(&rec.URI, &rec.RequestID, &rec.Queue, &rec.Metadata, &rec.CreatedAt, &rec.UpdatedAt, &rec.Version); err != nil {
			return nil, fmt.Errorf("failed to scan change record for queue=%s uri=%s: %w", queue, uri, err)
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate change records for queue=%s uri=%s: %w", queue, uri, err)
	}
	return results, nil
}
