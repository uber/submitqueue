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

	// Write membership before the batch row (no transaction) so a batch row is
	// never visible to ListActive without its membership. ON DUPLICATE KEY UPDATE is
	// an idempotent no-op on PK conflict (retry-safe) while still surfacing real
	// errors, unlike INSERT IGNORE which would swallow them and let Create proceed
	// without a valid membership. A create that fails after this point leaves a
	// dangling row, which ListActive skips and the reconcile job reclaims.
	if _, err = s.db.ExecContext(ctx,
		"INSERT INTO active_batch (queue, batch_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE batch_id = batch_id",
		batch.Queue, batch.ID,
	); err != nil {
		return fmt.Errorf("failed to insert active_batch membership for batch entity id=%s queue=%s: %w", batch.ID, batch.Queue, err)
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
		// Leave the membership row in place: a returned error doesn't prove the
		// batch row was not written (an ambiguous failure can commit it and still
		// error), so deleting could permanently hide a live batch from ListActive.
		// A dangling row is the safe direction.
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

// ListActive returns all active (non-terminal) batches in the given queue.
//
// Membership is tracked in active_batch (queue leads the PK), so listing is a
// PK-prefix scan that ports cleanly to a key-value store. Each member is fetched
// by primary key: a terminal batch's membership is best-effort removed (race-free,
// its id is never reused), while a missing batch is skipped but NOT removed (it
// may belong to an in-flight Create that hasn't written its batch row yet).
func (s *batchStore) ListActive(ctx context.Context, queue string) (ret []entity.Batch, retErr error) {
	op := metrics.Begin(s.scope, "list_active")
	defer func() { op.Complete(retErr) }()

	// Read all membership rows and release the connection before resolving each
	// batch, since Get issues its own query.
	ids, err := s.activeBatchIDs(ctx, queue)
	if err != nil {
		return nil, err
	}

	var results []entity.Batch
	for _, id := range ids {
		batch, err := s.Get(ctx, id)
		if err != nil {
			if storage.IsNotFound(err) {
				// Missing batch: either an in-flight Create or a dangling row. We
				// can't tell them apart, so skip without deleting.
				continue
			}
			return nil, fmt.Errorf("failed to get active batch id=%q queue=%q: %w", id, queue, err)
		}
		if batch.State.IsTerminal() {
			// Stale membership: the batch has finished. Race-free to remove since
			// its id is never reused.
			s.removeActive(ctx, queue, id)
			continue
		}
		results = append(results, batch)
	}

	return results, nil
}

// activeBatchIDs reads the batch IDs recorded as active for the queue, owning the
// result set's lifecycle so the caller can resolve each batch after it's closed.
func (s *batchStore) activeBatchIDs(ctx context.Context, queue string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT batch_id FROM active_batch WHERE queue = ?",
		queue,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query active batch membership for queue=%q: %w", queue, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan active batch membership for queue=%q: %w", queue, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate active batch membership for queue=%q: %w", queue, err)
	}
	return ids, nil
}

// removeActive best-effort deletes a single active_batch membership row, used by
// ListActive to reclaim terminal batches' memberships. Failures are counted and
// ignored — the row is harmless and the next read retries.
func (s *batchStore) removeActive(ctx context.Context, queue, batchID string) {
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM active_batch WHERE queue = ? AND batch_id = ?",
		queue, batchID,
	); err != nil {
		metrics.NamedCounter(s.scope, "list_active", "self_heal_errors", 1)
	}
}
