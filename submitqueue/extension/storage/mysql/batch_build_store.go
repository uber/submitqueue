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

type batchBuildStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBatchBuildStore creates a MySQL-backed BatchBuildStore.
func NewBatchBuildStore(db *sql.DB, scope tally.Scope) storage.BatchBuildStore {
	return &batchBuildStore{db: db, scope: scope}
}

// Create stores an immutable batch-to-build mapping.
func (s *batchBuildStore) Create(ctx context.Context, batchBuild entity.BatchBuild) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(
		ctx,
		"INSERT INTO batch_build (batch_id, build_id) VALUES (?, ?)",
		batchBuild.BatchID,
		batchBuild.BuildID,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("batch build mapping batch_id=%s: %w", batchBuild.BatchID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert batch build mapping batch_id=%s: %w", batchBuild.BatchID, err)
	}
	return nil
}

// Get retrieves the mapping for a batch.
func (s *batchBuildStore) Get(ctx context.Context, batchID string) (ret entity.BatchBuild, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	err := s.db.QueryRowContext(
		ctx,
		"SELECT batch_id, build_id FROM batch_build WHERE batch_id = ?",
		batchID,
	).Scan(&ret.BatchID, &ret.BuildID)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.BatchBuild{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.BatchBuild{}, fmt.Errorf("failed to get batch build mapping batch_id=%s: %w", batchID, err)
	}
	return ret, nil
}
