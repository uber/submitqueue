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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type fixStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewFixStore creates a MySQL-backed, queue-scoped FixStore.
func NewFixStore(db *sql.DB, scope tally.Scope, queue string) storage.FixStore {
	return &fixStore{db: db, scope: scope, queue: queue}
}

func (s *fixStore) Create(ctx context.Context, fix entity.Fix) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if fix.Queue != s.queue || fix.ID == "" {
		return fmt.Errorf("repair requires a nonempty ID and queue matching %q", s.queue)
	}
	issuesJSON, metadataJSON, err := marshalFixJSON(fix)
	if err != nil {
		return fmt.Errorf("failed to marshal repair data: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO fix (queue, id, build_id, state, issues, execution_ref, metadata, created_at, updated_at, version)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.queue,
		fix.ID,
		fix.BuildID,
		fix.State,
		issuesJSON,
		fix.ExecutionRef,
		metadataJSON,
		fix.CreatedAt,
		fix.UpdatedAt,
		fix.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("fix queue=%q: %w", s.queue, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert fix queue=%q: %w", s.queue, err)
	}
	return nil
}

func (s *fixStore) Get(ctx context.Context, id string) (ret entity.Fix, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var fix entity.Fix
	fix.Queue = s.queue
	var issuesJSON []byte
	var metadataJSON []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT id, build_id, state, issues, execution_ref, metadata, created_at, updated_at, version FROM fix
		 WHERE queue = ? AND id = ?`,
		s.queue,
		id,
	).Scan(
		&fix.ID,
		&fix.BuildID,
		&fix.State,
		&issuesJSON,
		&fix.ExecutionRef,
		&metadataJSON,
		&fix.CreatedAt,
		&fix.UpdatedAt,
		&fix.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.Fix{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Fix{}, fmt.Errorf("failed to get fix queue=%q: %w", s.queue, err)
	}
	if err := json.Unmarshal(issuesJSON, &fix.Issues); err != nil {
		return entity.Fix{}, fmt.Errorf("failed to unmarshal repair issues: %w", err)
	}
	if err := json.Unmarshal(metadataJSON, &fix.Metadata); err != nil {
		return entity.Fix{}, fmt.Errorf("failed to unmarshal repair metadata: %w", err)
	}
	if fix.Metadata == nil {
		fix.Metadata = map[string]string{}
	}
	return fix, nil
}

func (s *fixStore) Update(ctx context.Context, fix entity.Fix, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if fix.Queue != s.queue || fix.ID == "" {
		return fmt.Errorf("repair requires a nonempty ID and queue matching %q", s.queue)
	}
	issuesJSON, metadataJSON, err := marshalFixJSON(fix)
	if err != nil {
		return fmt.Errorf("failed to marshal repair data: %w", err)
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE fix SET build_id = ?, state = ?, issues = ?, execution_ref = ?, metadata = ?, created_at = ?, updated_at = ?, version = ?
		 WHERE queue = ? AND id = ? AND version = ?`,
		fix.BuildID,
		fix.State,
		issuesJSON,
		fix.ExecutionRef,
		metadataJSON,
		fix.CreatedAt,
		fix.UpdatedAt,
		newVersion,
		s.queue,
		fix.ID,
		oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update fix queue=%q: %w", s.queue, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for fix queue=%q: %w", s.queue, err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("version mismatch for fix queue=%q expected_version=%d: %w", s.queue, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}

func marshalFixJSON(fix entity.Fix) ([]byte, []byte, error) {
	issuesJSON, err := json.Marshal(fix.Issues)
	if err != nil {
		return nil, nil, err
	}
	metadata := fix.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, nil, err
	}
	return issuesJSON, metadataJSON, nil
}
