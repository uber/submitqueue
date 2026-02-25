package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
)

type batchStore struct {
	db *sql.DB
}

// NewBatchStore creates a new MySQL-backed BatchStore.
func NewBatchStore(db *sql.DB) storage.BatchStore {
	return &batchStore{db: db}
}

// Get retrieves a batch by ID. Returns ErrNotFound if the batch is not found.
func (s *batchStore) Get(ctx context.Context, id string) (entity.Batch, error) {
	var batch entity.Batch
	var containsJSON []byte
	var dependenciesJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT id, queue, contains, dependencies, state, version FROM batch WHERE id = ?",
		id,
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
func (s *batchStore) Create(ctx context.Context, batch entity.Batch) error {
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
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("batch entity id=%s: %w", batch.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert batch entity id=%s: %w", batch.ID, err)
	}

	return nil
}

// UpdateState updates the state of a batch if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
// The implementation increments the version by 1 atomically with the state update.
func (s *batchStore) UpdateState(ctx context.Context, id string, version int32, newState entity.BatchState) error {
	result, err := s.db.ExecContext(ctx,
		"UPDATE batch SET state = ?, version = version + 1 WHERE id = ? AND version = ?",
		newState, id, version,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update batch state for id=%q version=%d newState=%v: %w",
			id, version, newState, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q version=%d newState=%v: %w",
			id, version, newState, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for batch update: id=%q expected_version=%d newState=%v: %w",
			id, version, newState, storage.ErrVersionMismatch,
		)
	}

	return nil
}

// GetByStates retrieves all batches that are in the given states.
func (s *batchStore) GetByStates(ctx context.Context, states []entity.BatchState) ([]entity.Batch, error) {
	if len(states) == 0 {
		return nil, nil
	}

	query := "SELECT id, queue, contains, dependencies, state, version FROM batch WHERE state IN (?" + strings.Repeat(", ?", len(states)-1) + ")"

	args := make([]any, len(states))
	for i, state := range states {
		args[i] = state
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query batches by states=%v from the database: %w", states, err)
	}
	defer rows.Close()

	var results []entity.Batch
	for rows.Next() {
		var batch entity.Batch
		var containsJSON []byte
		var dependenciesJSON []byte

		if err := rows.Scan(&batch.ID, &batch.Queue, &containsJSON, &dependenciesJSON, &batch.State, &batch.Version); err != nil {
			return nil, fmt.Errorf("failed to scan batch entity by states=%v from the database: %w", states, err)
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
		return nil, fmt.Errorf("failed to iterate batches by states=%v from the database: %w", states, err)
	}

	return results, nil
}
