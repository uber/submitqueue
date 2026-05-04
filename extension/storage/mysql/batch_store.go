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
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
)

type batchStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBatchStore creates a new MySQL-backed BatchStore.
func NewBatchStore(db *sql.DB, scope tally.Scope) storage.BatchStore {
	return &batchStore{db: db, scope: scope}
}

// Get retrieves a batch by ID. Returns ErrNotFound if the batch is not found.
func (s *batchStore) Get(ctx context.Context, id string) (ret entity.Batch, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	var batch entity.Batch
	var containsJSON []byte
	var dependenciesJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT id, queue, contains, dependencies, score, state, version FROM batch WHERE id = ?",
		id,
	).Scan(&batch.ID, &batch.Queue, &containsJSON, &dependenciesJSON, &batch.Score, &batch.State, &batch.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Batch{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Batch{}, fmt.Errorf("failed to get batch entity id=%s from the database: %w", id, err)
	}

	if err := json.Unmarshal(containsJSON, &batch.Contains); err != nil {
		return entity.Batch{}, fmt.Errorf("failed to unmarshal contains for batch entity id=%s from the database: %w", id, err)
	}

	if err := json.Unmarshal(dependenciesJSON, &batch.Dependencies); err != nil {
		return entity.Batch{}, fmt.Errorf("failed to unmarshal dependencies for batch entity id=%s from the database: %w", id, err)
	}

	return batch, nil
}

// Create creates a new batch. The batch must have a unique ID already assigned. Returns ErrAlreadyExists if the batch ID already exists.
func (s *batchStore) Create(ctx context.Context, batch entity.Batch) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	containsJSON, err := json.Marshal(batch.Contains)
	if err != nil {
		return fmt.Errorf("failed to marshal contains=%v id=%s for Create batch entity: %w", batch.Contains, batch.ID, err)
	}

	dependenciesJSON, err := json.Marshal(batch.Dependencies)
	if err != nil {
		return fmt.Errorf("failed to marshal dependencies=%v id=%s for Create batch entity: %w", batch.Dependencies, batch.ID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO batch (id, queue, contains, dependencies, score, state, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
		batch.ID, batch.Queue, containsJSON, dependenciesJSON, batch.Score, batch.State, batch.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("batch entity id=%s: %w", batch.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert batch entity id=%s: %w", batch.ID, err)
	}

	return nil
}

// UpdateState updates the state of a batch to newState and the version to newVersion
// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
// Version arithmetic is owned by the caller; this is a pure conditional write.
func (s *batchStore) UpdateState(ctx context.Context, id string, oldVersion, newVersion int32, newState entity.BatchState) (retErr error) {
	op := metrics.Begin(s.scope, "update_state")
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx,
		"UPDATE batch SET state = ?, version = ? WHERE id = ? AND version = ?",
		newState, newVersion, id, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update batch state for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			id, oldVersion, newVersion, newState, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			id, oldVersion, newVersion, newState, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for batch update: id=%q expected_version=%d newState=%v: %w",
			id, oldVersion, newState, storage.ErrVersionMismatch,
		)
	}

	return nil
}

// UpdateScoreAndState atomically updates the score and state of a batch and the version to newVersion
// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
// Version arithmetic is owned by the caller; this is a pure conditional write.
func (s *batchStore) UpdateScoreAndState(ctx context.Context, id string, oldVersion, newVersion int32, score float64, newState entity.BatchState) (retErr error) {
	op := metrics.Begin(s.scope, "update_score_and_state")
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx,
		"UPDATE batch SET score = ?, state = ?, version = ? WHERE id = ? AND version = ?",
		score, newState, newVersion, id, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update batch score and state for id=%q oldVersion=%d newVersion=%d score=%f newState=%v: %w",
			id, oldVersion, newVersion, score, newState, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update score and state for id=%q oldVersion=%d newVersion=%d score=%f newState=%v: %w",
			id, oldVersion, newVersion, score, newState, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for batch update score and state: id=%q expected_version=%d score=%f newState=%v: %w",
			id, oldVersion, score, newState, storage.ErrVersionMismatch,
		)
	}

	return nil
}

// GetByQueueAndStates retrieves all batches that belong to the given queue and are in the given states.
func (s *batchStore) GetByQueueAndStates(ctx context.Context, queue string, states []entity.BatchState) (ret []entity.Batch, retErr error) {
	op := metrics.Begin(s.scope, "get_by_queue_and_states")
	defer func() { op.Complete(retErr) }()

	if len(states) == 0 {
		return nil, nil
	}

	query := "SELECT id, queue, contains, dependencies, score, state, version FROM batch WHERE queue = ? AND state IN (?" + strings.Repeat(", ?", len(states)-1) + ")"

	args := make([]any, 1+len(states))
	args[0] = queue
	for i, state := range states {
		args[i+1] = state
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query batches by queue=%q states=%v from the database: %w", queue, states, err)
	}
	defer rows.Close()

	var results []entity.Batch
	for rows.Next() {
		var batch entity.Batch
		var containsJSON []byte
		var dependenciesJSON []byte

		if err := rows.Scan(&batch.ID, &batch.Queue, &containsJSON, &dependenciesJSON, &batch.Score, &batch.State, &batch.Version); err != nil {
			return nil, fmt.Errorf("failed to scan batch entity by queue=%q states=%v from the database: %w", queue, states, err)
		}

		if err := json.Unmarshal(containsJSON, &batch.Contains); err != nil {
			return nil, fmt.Errorf("failed to unmarshal contains for batch entity id=%s from the database: %w", batch.ID, err)
		}

		if err := json.Unmarshal(dependenciesJSON, &batch.Dependencies); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dependencies for batch entity id=%s from the database: %w", batch.ID, err)
		}

		results = append(results, batch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate batches by queue=%q states=%v from the database: %w", queue, states, err)
	}

	return results, nil
}
