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

type changeProviderStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewChangeProviderStore creates a new MySQL-backed ChangeProviderStore.
func NewChangeProviderStore(db *sql.DB, scope tally.Scope) storage.ChangeProviderStore {
	return &changeProviderStore{db: db, scope: scope}
}

// Get retrieves change provider(s) by request ID. Returns ErrNotFound if the change provider is not found.
//
// Note: The order of ChangeProvider entities returned here is not guaranteed
// to be the same as the request to which it belongs. The caller is repsonsible
// for inspecting and mapping the result of this function to the
// order of changes within the original request.
func (s *changeProviderStore) Get(ctx context.Context, requestID string) (ret []entity.ChangeProvider, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

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
func (s *changeProviderStore) Create(ctx context.Context, changeProvider entity.ChangeProvider) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

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
