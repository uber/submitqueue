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
	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
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
	var speculationsJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT batch_id, speculations FROM speculation_tree WHERE batch_id = ?",
		batchID,
	).Scan(&st.BatchID, &speculationsJSON)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.SpeculationTree{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("failed to get speculation tree entity batchID=%s from the database: %w", batchID, err)
	}

	if err := json.Unmarshal(speculationsJSON, &st.Speculations); err != nil {
		return entity.SpeculationTree{}, fmt.Errorf("failed to unmarshal speculations for speculation tree entity batchID=%s from the database: %w", batchID, err)
	}

	return st, nil
}

// Create creates a new speculation tree. Returns ErrAlreadyExists if the entry already exists.
func (s *speculationTreeStore) Create(ctx context.Context, speculationTree entity.SpeculationTree) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	speculationsJSON, err := json.Marshal(speculationTree.Speculations)
	if err != nil {
		return fmt.Errorf("failed to marshal speculations batchID=%s for Create speculation tree entity: %w", speculationTree.BatchID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO speculation_tree (batch_id, speculations) VALUES (?, ?)",
		speculationTree.BatchID, speculationsJSON,
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

// UpdateSpeculations updates the speculations of a speculation tree. Returns ErrNotFound if the speculation tree is not found.
func (s *speculationTreeStore) UpdateSpeculations(ctx context.Context, batchID string, speculations []entity.SpeculationInfo) (retErr error) {
	op := metrics.Begin(s.scope, "update_speculations")
	defer func() { op.Complete(retErr) }()

	speculationsJSON, err := json.Marshal(speculations)
	if err != nil {
		return fmt.Errorf("failed to marshal speculations batchID=%s for UpdateSpeculations: %w", batchID, err)
	}

	result, err := s.db.ExecContext(ctx,
		"UPDATE speculation_tree SET speculations = ? WHERE batch_id = ?",
		speculationsJSON, batchID,
	)
	if err != nil {
		return fmt.Errorf("failed to update speculations for batchID=%q: %w", batchID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected from update for batchID=%q: %w", batchID, err)
	}

	if rowsAffected != 1 {
		return storage.WrapNotFound(fmt.Errorf("speculation tree entity batchID=%s", batchID))
	}

	return nil
}
