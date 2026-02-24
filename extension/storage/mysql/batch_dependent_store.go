package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
)

type batchDependentStore struct {
	db *sql.DB
}

// NewBatchDependentStore creates a new MySQL-backed BatchDependentStore.
func NewBatchDependentStore(db *sql.DB) storage.BatchDependentStore {
	return &batchDependentStore{db: db}
}

// Get retrieves the batch dependent by batch ID. Returns ErrNotFound if the batch dependent is not found.
func (s *batchDependentStore) Get(ctx context.Context, batchID string) (entity.BatchDependent, error) {
	var bd entity.BatchDependent
	var dependentsJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT batch_id, dependents FROM batch_dependent WHERE batch_id = ?",
		batchID,
	).Scan(&bd.BatchID, &dependentsJSON)

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
func (s *batchDependentStore) Create(ctx context.Context, batchDependent entity.BatchDependent) error {
	dependentsJSON, err := json.Marshal(batchDependent.Dependents)
	if err != nil {
		return fmt.Errorf("failed to marshal dependents batchID=%s for Create batch dependent entity: %w", batchDependent.BatchID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO batch_dependent (batch_id, dependents) VALUES (?, ?)",
		batchDependent.BatchID, dependentsJSON,
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
