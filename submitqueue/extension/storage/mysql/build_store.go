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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type buildStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBuildStore creates a new MySQL-backed BuildStore.
func NewBuildStore(db *sql.DB, scope tally.Scope) storage.BuildStore {
	return &buildStore{db: db, scope: scope}
}

// Get retrieves a build by ID. Returns ErrNotFound if the build is not found.
func (s *buildStore) Get(ctx context.Context, id string) (ret entity.Build, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	return s.scanBuild(ctx,
		"SELECT id, batch_id, speculation_path, score, status FROM build WHERE id = ?",
		"id="+id, id)
}

// GetByBatchID retrieves the single build scheduled for the given batch.
// Returns ErrNotFound if no build exists for the batch.
func (s *buildStore) GetByBatchID(ctx context.Context, batchID string) (ret entity.Build, retErr error) {
	op := metrics.Begin(s.scope, "get_by_batch_id")
	defer func() { op.Complete(retErr) }()

	return s.scanBuild(ctx,
		"SELECT id, batch_id, speculation_path, score, status FROM build WHERE batch_id = ?",
		"batch_id="+batchID, batchID)
}

// scanBuild runs a single-row build query and decodes the result, including the
// speculation_path JSON column. label is used only for error context (e.g.
// "id=…" or "batch_id=…"). Returns ErrNotFound when the query matches no row.
func (s *buildStore) scanBuild(ctx context.Context, query, label string, args ...any) (entity.Build, error) {
	var build entity.Build
	var speculationPathJSON []byte

	err := s.db.QueryRowContext(ctx, query, args...).
		Scan(&build.ID, &build.BatchID, &speculationPathJSON, &build.Score, &build.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.Build{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Build{}, fmt.Errorf("failed to get build entity %s from the database: %w", label, err)
	}

	if err := json.Unmarshal(speculationPathJSON, &build.SpeculationPath); err != nil {
		return entity.Build{}, fmt.Errorf("failed to unmarshal speculation_path for build entity %s from the database: %w", label, err)
	}

	return build, nil
}

// Create creates a new build. The build must have a unique ID and batch ID.
// Returns ErrAlreadyExists if either uniqueness constraint is violated.
func (s *buildStore) Create(ctx context.Context, build entity.Build) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	speculationPathJSON, err := json.Marshal(build.SpeculationPath)
	if err != nil {
		return fmt.Errorf("failed to marshal speculation_path id=%s for Create build entity: %w", build.ID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO build (id, batch_id, speculation_path, score, status) VALUES (?, ?, ?, ?, ?)",
		build.ID, build.BatchID, speculationPathJSON, build.Score, build.Status,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("build entity id=%s: %w", build.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert build entity id=%s: %w", build.ID, err)
	}

	return nil
}

// UpdateStatus updates the status of a build. Returns ErrNotFound if the build is not found.
func (s *buildStore) UpdateStatus(ctx context.Context, id string, newStatus entity.BuildStatus) (retErr error) {
	op := metrics.Begin(s.scope, "update_status")
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx,
		"UPDATE build SET status = ? WHERE id = ?",
		newStatus, id,
	)
	if err != nil {
		return fmt.Errorf("failed to update build status for id=%q newStatus=%v: %w", id, newStatus, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected from update for id=%q newStatus=%v: %w", id, newStatus, err)
	}

	if rowsAffected != 1 {
		return storage.WrapNotFound(fmt.Errorf("build entity id=%s", id))
	}

	return nil
}
