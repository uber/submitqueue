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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestContextStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestContextStore creates a MySQL-backed RequestContextStore.
func NewRequestContextStore(db *sql.DB, scope tally.Scope) storage.RequestContextStore {
	return &requestContextStore{db: db, scope: scope}
}

// Create persists immutable request admission data.
func (s *requestContextStore) Create(ctx context.Context, requestContext entity.RequestContext) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	changeURIs := requestContext.ChangeURIs
	if changeURIs == nil {
		changeURIs = []string{}
	}
	changeURIsJSON, err := json.Marshal(changeURIs)
	if err != nil {
		return fmt.Errorf("failed to marshal request context change URIs for request_id=%s: %w", requestContext.RequestID, err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO request_context (request_id, queue, change_uri, admitted_at_ms) VALUES (?, ?, ?, ?)",
		requestContext.RequestID, requestContext.Queue, changeURIsJSON, requestContext.AdmittedAtMs,
	)
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("request context request_id=%s: %w", requestContext.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to create request context request_id=%s: %w", requestContext.RequestID, err)
	}
	return nil
}

// Get returns immutable request admission data.
func (s *requestContextStore) Get(ctx context.Context, requestID string) (ret entity.RequestContext, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	var changeURIsJSON []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT request_id, queue, change_uri, admitted_at_ms FROM request_context WHERE request_id = ?",
		requestID,
	).Scan(&ret.RequestID, &ret.Queue, &changeURIsJSON, &ret.AdmittedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestContext{}, fmt.Errorf("request context request_id=%s: %w", requestID, storage.ErrNotFound)
	}
	if err != nil {
		return entity.RequestContext{}, fmt.Errorf("failed to get request context request_id=%s: %w", requestID, err)
	}
	if err := json.Unmarshal(changeURIsJSON, &ret.ChangeURIs); err != nil {
		return entity.RequestContext{}, fmt.Errorf("failed to unmarshal request context change URIs for request_id=%s: %w", requestID, err)
	}
	if ret.ChangeURIs == nil {
		ret.ChangeURIs = []string{}
	}
	return ret, nil
}
