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
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
)

type batchDependentStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBatchDependentStore creates a new MySQL-backed BatchDependentStore.
func NewBatchDependentStore(db *sql.DB, scope tally.Scope) storage.BatchDependentStore {
	return &batchDependentStore{db: db, scope: scope}
}

// Get retrieves the batch dependent by batch ID. Returns ErrNotFound if the batch dependent is not found.
func (s *batchDependentStore) Get(ctx context.Context, batchID string) (ret entity.BatchDependent, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	var bd entity.BatchDependent
	var dependentsJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT batch_id, dependents, version FROM batch_dependent WHERE batch_id = ?",
		batchID,
	).Scan(&bd.BatchID, &dependentsJSON, &bd.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.BatchDependent{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.BatchDependent{}, fmt.Errorf("failed to get batch dependent entity batchID=%s from the database: %w", batchID, err)
	}

	if err := json.Unmarshal(dependentsJSON, &bd.Dependents); err != nil {
		return entity.BatchDependent{}, fmt.Errorf("failed to unmarshal dependents for batch dependent entity batchID=%s from the database: %w", batchID, err)
	}

	return bd, nil
}

// Create creates a new batch dependent. Returns ErrAlreadyExists if the entry already exists.
func (s *batchDependentStore) Create(ctx context.Context, batchDependent entity.BatchDependent) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	dependentsJSON, err := json.Marshal(batchDependent.Dependents)
	if err != nil {
		return fmt.Errorf("failed to marshal dependents batchID=%s for Create batch dependent entity: %w", batchDependent.BatchID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO batch_dependent (batch_id, dependents, version) VALUES (?, ?, ?)",
		batchDependent.BatchID, dependentsJSON, batchDependent.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("batch dependent entity batchID=%s: %w", batchDependent.BatchID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert batch dependent entity batchID=%s: %w", batchDependent.BatchID, err)
	}

	return nil
}

// UpdateDependents updates the dependents of a batch dependent if the current version matches the expected version.
// If versions do not match, returns ErrVersionMismatch.
// The implementation increments the version by 1 atomically with the dependents update.
func (s *batchDependentStore) UpdateDependents(ctx context.Context, batchID string, version int32, dependents []string) (retErr error) {
	op := metrics.Begin(s.scope, "update_dependents")
	defer func() { op.Complete(retErr) }()

	dependentsJSON, err := json.Marshal(dependents)
	if err != nil {
		return fmt.Errorf("failed to marshal dependents batchID=%s for UpdateDependents batch dependent entity: %w", batchID, err)
	}

	result, err := s.db.ExecContext(ctx,
		"UPDATE batch_dependent SET dependents = ?, version = version + 1 WHERE batch_id = ? AND version = ?",
		dependentsJSON, batchID, version,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update batch dependent dependents for batchID=%q version=%d: %w",
			batchID, version, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for batchID=%q version=%d: %w",
			batchID, version, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for batch dependent update: batchID=%q expected_version=%d: %w",
			batchID, version, storage.ErrVersionMismatch,
		)
	}

	return nil
}
