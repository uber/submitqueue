// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewRequestBatchStore creates a MySQL-backed RequestBatchStore.
func NewRequestBatchStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestBatchStore {
	return &requestBatchStore{db: db, scope: scope, queue: queue}
}

func (s *requestBatchStore) GetByRequestID(ctx context.Context, requestID string) (ret []entity.RequestBatch, retErr error) {
	op := metrics.Begin(s.scope, "get_by_request_id", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx,
		"SELECT request_id, batch_id, version FROM request_batch WHERE queue = ? AND request_id = ? ORDER BY batch_id",
		s.queue, requestID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get request batch associations requestID=%s: %w", requestID, err)
	}
	defer rows.Close()

	var associations []entity.RequestBatch
	for rows.Next() {
		var association entity.RequestBatch
		if err := rows.Scan(&association.RequestID, &association.BatchID, &association.Version); err != nil {
			return nil, fmt.Errorf("failed to scan request batch association requestID=%s: %w", requestID, err)
		}
		associations = append(associations, association)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request batch associations requestID=%s: %w", requestID, err)
	}
	return associations, nil
}

func (s *requestBatchStore) Create(ctx context.Context, association entity.RequestBatch) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO request_batch (queue, request_id, batch_id, version) VALUES (?, ?, ?, ?)",
		s.queue, association.RequestID, association.BatchID, association.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request batch association requestID=%s batchID=%s: %w", association.RequestID, association.BatchID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to create request batch association requestID=%s batchID=%s: %w", association.RequestID, association.BatchID, err)
	}
	return nil
}
