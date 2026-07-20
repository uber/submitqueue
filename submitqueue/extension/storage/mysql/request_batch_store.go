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

type requestBatchStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestBatchStore creates a new MySQL-backed RequestBatchStore.
func NewRequestBatchStore(db *sql.DB, scope tally.Scope) storage.RequestBatchStore {
	return &requestBatchStore{db: db, scope: scope}
}

// Get retrieves the immutable batch assignment for a request.
func (s *requestBatchStore) Get(ctx context.Context, requestID string) (ret entity.RequestBatch, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	err := s.db.QueryRowContext(ctx,
		"SELECT request_id, batch_id, version FROM request_batch WHERE request_id = ?",
		requestID,
	).Scan(&ret.RequestID, &ret.BatchID, &ret.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestBatch{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestBatch{}, fmt.Errorf("failed to get request batch assignment requestID=%s: %w", requestID, err)
	}
	return ret, nil
}

// Create reserves an immutable batch assignment for a request.
func (s *requestBatchStore) Create(ctx context.Context, requestBatch entity.RequestBatch) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO request_batch (request_id, batch_id, version) VALUES (?, ?, ?)",
		requestBatch.RequestID, requestBatch.BatchID, requestBatch.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request batch assignment requestID=%s: %w", requestBatch.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to create request batch assignment requestID=%s batchID=%s: %w", requestBatch.RequestID, requestBatch.BatchID, err)
	}
	return nil
}
