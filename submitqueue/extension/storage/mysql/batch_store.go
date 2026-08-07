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

type batchStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewBatchStore creates a new MySQL-backed BatchStore.
func NewBatchStore(db *sql.DB, scope tally.Scope, queue string) storage.BatchStore {
	return &batchStore{db: db, scope: scope, queue: queue}
}

// Get retrieves a batch by ID. Returns ErrNotFound if the batch is not found.
func (s *batchStore) Get(ctx context.Context, id string) (ret entity.Batch, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var batch entity.Batch
	var containsJSON []byte
	var dependenciesJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT id, queue, contains, dependencies, state, version FROM batch WHERE queue = ? AND id = ?",
		s.queue, id,
	).Scan(&batch.ID, &batch.Queue, &containsJSON, &dependenciesJSON, &batch.State, &batch.Version)

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
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if batch.Queue != s.queue {
		return fmt.Errorf("batch %s queue %q does not match the store's bound queue %q", batch.ID, batch.Queue, s.queue)
	}

	containsJSON, err := json.Marshal(batch.Contains)
	if err != nil {
		return fmt.Errorf("failed to marshal contains=%v id=%s for Create batch entity: %w", batch.Contains, batch.ID, err)
	}

	dependenciesJSON, err := json.Marshal(batch.Dependencies)
	if err != nil {
		return fmt.Errorf("failed to marshal dependencies=%v id=%s for Create batch entity: %w", batch.Dependencies, batch.ID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO batch (id, queue, contains, dependencies, state, version) VALUES (?, ?, ?, ?, ?, ?)",
		batch.ID, batch.Queue, containsJSON, dependenciesJSON, batch.State, batch.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("batch entity id=%s: %w", batch.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert batch entity id=%s: %w", batch.ID, err)
	}

	return nil
}

// Update replaces every non-key field of a batch and writes newVersion
// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
// Version arithmetic is owned by the caller; this is a pure conditional write.
func (s *batchStore) Update(ctx context.Context, batch entity.Batch, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update_state", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if batch.Queue != s.queue {
		return fmt.Errorf("batch %s queue %q does not match the store's bound queue %q", batch.ID, batch.Queue, s.queue)
	}

	containsJSON, err := json.Marshal(batch.Contains)
	if err != nil {
		return fmt.Errorf("failed to marshal contains=%v id=%s for Update batch entity: %w", batch.Contains, batch.ID, err)
	}

	dependenciesJSON, err := json.Marshal(batch.Dependencies)
	if err != nil {
		return fmt.Errorf("failed to marshal dependencies=%v id=%s for Update batch entity: %w", batch.Dependencies, batch.ID, err)
	}

	result, err := s.db.ExecContext(ctx,
		"UPDATE batch SET contains = ?, dependencies = ?, state = ?, version = ? WHERE queue = ? AND id = ? AND version = ?",
		containsJSON, dependenciesJSON, batch.State, newVersion, batch.Queue, batch.ID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update batch for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			batch.ID, oldVersion, newVersion, batch.State, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			batch.ID, oldVersion, newVersion, batch.State, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for batch update: id=%q expected_version=%d newState=%v: %w",
			batch.ID, oldVersion, batch.State, storage.ErrVersionMismatch,
		)
	}

	return nil
}
