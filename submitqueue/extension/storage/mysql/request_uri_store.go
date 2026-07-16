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
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestURIStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestURIStore creates a MySQL-backed RequestURIStore.
func NewRequestURIStore(db *sql.DB, scope tally.Scope) storage.RequestURIStore {
	return &requestURIStore{db: db, scope: scope}
}

func (s *requestURIStore) Create(ctx context.Context, mapping entity.RequestURI) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO change_uri_request_mapping (change_uri, received_at_ms, request_id) VALUES (?, ?, ?)",
		mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request URI change_uri=%s received_at_ms=%d request_id=%s: %w", mapping.ChangeURI, mapping.ReceivedAtMs, mapping.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request URI request_id=%s change_uri=%s: %w", mapping.RequestID, mapping.ChangeURI, err)
	}
	return nil
}

func (s *requestURIStore) ListByURI(ctx context.Context, changeURI string, limit int) (ret []entity.RequestURI, retErr error) {
	op := metrics.Begin(s.scope, "list_by_uri")
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, `
		SELECT change_uri, received_at_ms, request_id
		FROM change_uri_request_mapping
		WHERE change_uri = ?
		ORDER BY received_at_ms DESC, request_id DESC
		LIMIT ?`, changeURI, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list request URIs change_uri=%s: %w", changeURI, err)
	}
	defer rows.Close()

	results := make([]entity.RequestURI, 0)
	for rows.Next() {
		var mapping entity.RequestURI
		if err := rows.Scan(&mapping.ChangeURI, &mapping.ReceivedAtMs, &mapping.RequestID); err != nil {
			return nil, fmt.Errorf("failed to scan request URI change_uri=%s: %w", changeURI, err)
		}
		results = append(results, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request URIs change_uri=%s: %w", changeURI, err)
	}
	return results, nil
}
