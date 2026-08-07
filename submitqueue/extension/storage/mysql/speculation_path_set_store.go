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

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type speculationPathSetStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewSpeculationPathSetStore creates a new MySQL-backed SpeculationPathSetStore.
func NewSpeculationPathSetStore(db *sql.DB, scope tally.Scope) storage.SpeculationPathSetStore {
	return &speculationPathSetStore{db: db, scope: scope}
}

// Get retrieves a head's path set, where head is the head batch's ID.
// Returns ErrNotFound if the head has no set.
func (s *speculationPathSetStore) Get(ctx context.Context, head string) (ret entity.SpeculationPathSet, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var set entity.SpeculationPathSet
	var pathsJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT head, paths, version FROM speculation_path_set WHERE head = ?",
		head,
	).Scan(&set.Head, &pathsJSON, &set.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.SpeculationPathSet{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.SpeculationPathSet{}, fmt.Errorf("failed to get speculation path set entity head=%s from the database: %w", head, err)
	}

	if err := json.Unmarshal(pathsJSON, &set.Paths); err != nil {
		return entity.SpeculationPathSet{}, fmt.Errorf("failed to unmarshal paths for speculation path set entity head=%s from the database: %w", head, err)
	}

	return set, nil
}

// Create stores a head's first path set. Returns ErrAlreadyExists if the head already has one.
func (s *speculationPathSetStore) Create(ctx context.Context, set entity.SpeculationPathSet) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	pathsJSON, err := json.Marshal(set.Paths)
	if err != nil {
		return fmt.Errorf("failed to marshal paths head=%s for Create speculation path set entity: %w", set.Head, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO speculation_path_set (head, paths, version) VALUES (?, ?, ?)",
		set.Head, pathsJSON, set.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("speculation path set entity head=%s: %w", set.Head, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert speculation path set entity head=%s: %w", set.Head, err)
	}

	return nil
}

// Update replaces the stored set and writes newVersion if the persisted version matches
// oldVersion. If versions do not match, returns ErrVersionMismatch. set.Version is ignored:
// version arithmetic is owned by the caller and this is a pure conditional write.
func (s *speculationPathSetStore) Update(ctx context.Context, set entity.SpeculationPathSet, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	pathsJSON, err := json.Marshal(set.Paths)
	if err != nil {
		return fmt.Errorf("failed to marshal paths head=%s for Update speculation path set entity: %w", set.Head, err)
	}

	result, err := s.db.ExecContext(ctx,
		"UPDATE speculation_path_set SET paths = ?, version = ? WHERE head = ? AND version = ?",
		pathsJSON, newVersion, set.Head, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update speculation path set for head=%q oldVersion=%d newVersion=%d: %w",
			set.Head, oldVersion, newVersion, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for head=%q oldVersion=%d newVersion=%d: %w",
			set.Head, oldVersion, newVersion, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for speculation path set update: head=%q expected_version=%d: %w",
			set.Head, oldVersion, storage.ErrVersionMismatch,
		)
	}

	return nil
}
