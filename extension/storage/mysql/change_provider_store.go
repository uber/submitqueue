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

type changeProviderStore struct {
	db *sql.DB
}

// NewChangeProviderStore creates a new MySQL-backed ChangeProviderStore.
func NewChangeProviderStore(db *sql.DB) storage.ChangeProviderStore {
	return &changeProviderStore{db: db}
}

// Get retrieves a change provider by ID. Returns ErrNotFound if the change provider is not found.
func (s *changeProviderStore) Get(ctx context.Context, id string) (entity.ChangeProvider, error) {
	var cp entity.ChangeProvider
	var metadataJSON []byte

	err := s.db.QueryRowContext(ctx,
		"SELECT id, queue, change_provider_src, change_provider_id, metadata, version FROM change_provider WHERE id = ?",
		id,
	).Scan(&cp.ID, &cp.Queue, &cp.ChangeProviderSrc, &cp.ChangeProviderID, &metadataJSON, &cp.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.ChangeProvider{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.ChangeProvider{}, fmt.Errorf("failed to get change provider entity id=%s from the database: %w", id, err)
	}

	if err := json.Unmarshal(metadataJSON, &cp.Metadata); err != nil {
		return entity.ChangeProvider{}, fmt.Errorf("failed to unmarshal metadata for change provider entity id=%s from the database: %w", id, err)
	}

	return cp, nil
}

// Create creates a new change provider. Returns ErrAlreadyExists if the entry already exists.
func (s *changeProviderStore) Create(ctx context.Context, changeProvider entity.ChangeProvider) error {
	metadataJSON, err := json.Marshal(changeProvider.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata id=%s for Create change provider entity: %w", changeProvider.ID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO change_provider (id, queue, change_provider_src, change_provider_id, metadata, version) VALUES (?, ?, ?, ?, ?, ?)",
		changeProvider.ID, changeProvider.Queue, changeProvider.ChangeProviderSrc, changeProvider.ChangeProviderID, metadataJSON, changeProvider.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("change provider entity id=%s: %w", changeProvider.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert change provider entity id=%s: %w", changeProvider.ID, err)
	}

	return nil
}
