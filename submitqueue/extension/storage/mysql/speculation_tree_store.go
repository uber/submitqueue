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

type speculationTreeStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewSpeculationTreeStore creates a new MySQL-backed SpeculationTreeStore.
func NewSpeculationTreeStore(db *sql.DB, scope tally.Scope) storage.SpeculationTreeStore {
	return &speculationTreeStore{db: db, scope: scope}
}

// Get retrieves the speculation tree by batch ID. Returns ErrNotFound if the speculation tree is not found.
func (s *speculationTreeStore) Get(ctx context.Context, batchID string) (ret entity.SpeculationTree, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	var st entity.SpeculationTree
	var pathsJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT batch_id, paths FROM speculation_tree WHERE batch_id = ?",
		batchID,
	).Scan(&st.BatchID, &pathsJSON)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.SpeculationTree{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("failed to get speculation tree entity batchID=%s from the database: %w", batchID, err)
	}

	if err := json.Unmarshal(pathsJSON, &st.Paths); err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("failed to unmarshal paths for speculation tree entity batchID=%s from the database: %w", batchID, err)
	}

	return st, nil
}

// Create creates a new speculation tree. Returns ErrAlreadyExists if the entry already exists.
func (s *speculationTreeStore) Create(ctx context.Context, speculationTree entity.SpeculationTree) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	pathsJSON, err := json.Marshal(speculationTree.Paths)
	if err != nil {
		return fmt.Errorf("failed to marshal paths batchID=%s for Create speculation tree entity: %w", speculationTree.BatchID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO speculation_tree (batch_id, paths) VALUES (?, ?)",
		speculationTree.BatchID, pathsJSON,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("speculation tree entity batchID=%s: %w", speculationTree.BatchID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert speculation tree entity batchID=%s: %w", speculationTree.BatchID, err)
	}

	return nil
}

// Update overwrites the paths of an existing speculation tree, identified by
// speculationTree.BatchID. Returns ErrNotFound if the speculation tree is not found.
func (s *speculationTreeStore) Update(ctx context.Context, speculationTree entity.SpeculationTree) (retErr error) {
	op := metrics.Begin(s.scope, "update")
	defer func() { op.Complete(retErr) }()

	pathsJSON, err := json.Marshal(speculationTree.Paths)
	if err != nil {
		return fmt.Errorf("failed to marshal paths batchID=%s for Update: %w", speculationTree.BatchID, err)
	}

	result, err := s.db.ExecContext(ctx,
		"UPDATE speculation_tree SET paths = ? WHERE batch_id = ?",
		pathsJSON, speculationTree.BatchID,
	)
	if err != nil {
		return fmt.Errorf("failed to update speculation tree for batchID=%q: %w", speculationTree.BatchID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected from update for batchID=%q: %w", speculationTree.BatchID, err)
	}

	if rowsAffected != 1 {
		return storage.WrapNotFound(fmt.Errorf("speculation tree entity batchID=%s", speculationTree.BatchID))
	}

	return nil
}
