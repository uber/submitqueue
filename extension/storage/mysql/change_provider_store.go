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

// Get retrieves change provider(s) by request ID. Returns ErrNotFound if the change provider is not found.
func (s *changeProviderStore) Get(ctx context.Context, requestID string) ([]entity.ChangeProvider, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT request_id, change_provider_src, change_provider_id, metadata FROM change_provider WHERE request_id = ?",
		requestID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get change provider entities requestID=%s from the database: %w", requestID, err)
	}
	defer rows.Close()

	var results []entity.ChangeProvider
	for rows.Next() {
		var cp entity.ChangeProvider
		var metadataJSON []byte

		if err := rows.Scan(&cp.RequestID, &cp.ChangeProviderSrc, &cp.ChangeProviderID, &metadataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan change provider entity requestID=%s from the database: %w", requestID, err)
		}

		if err := json.Unmarshal(metadataJSON, &cp.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata for change provider entity requestID=%s from the database: %w", requestID, err)
		}

		results = append(results, cp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate change provider entities requestID=%s from the database: %w", requestID, err)
	}

	if len(results) == 0 {
		return nil, storage.WrapNotFound(fmt.Errorf("change provider entity requestID=%s", requestID))
	}

	return results, nil
}

// Create creates a new change provider. Returns ErrAlreadyExists if the entry already exists.
func (s *changeProviderStore) Create(ctx context.Context, changeProvider entity.ChangeProvider) error {
	metadataJSON, err := json.Marshal(changeProvider.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata id=%s for Create change provider entity: %w", changeProvider.RequestID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO change_provider (request_id, change_provider_src, change_provider_id, metadata) VALUES (?, ?, ?, ?)",
		changeProvider.RequestID, changeProvider.ChangeProviderSrc, changeProvider.ChangeProviderID, metadataJSON,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("change provider entity id=%s: %w", changeProvider.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert change provider entity id=%s: %w", changeProvider.RequestID, err)
	}

	return nil
}
