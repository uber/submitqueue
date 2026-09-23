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

type pathBuildStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewPathBuildStore creates a new MySQL-backed PathBuildStore.
func NewPathBuildStore(db *sql.DB, scope tally.Scope, queue string) storage.PathBuildStore {
	return &pathBuildStore{db: db, scope: scope, queue: queue}
}

// Get resolves an attempt to its build. Returns ErrNotFound if no build has
// been recorded for it.
func (s *pathBuildStore) Get(ctx context.Context, pathID string, attempt int) (ret entity.PathBuild, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var pathBuild entity.PathBuild

	err := s.db.QueryRowContext(ctx,
		"SELECT queue, path_id, attempt, build_id FROM path_build WHERE queue = ? AND path_id = ? AND attempt = ?",
		s.queue, pathID, attempt,
	).Scan(&pathBuild.Queue, &pathBuild.PathID, &pathBuild.Attempt, &pathBuild.BuildID)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.PathBuild{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.PathBuild{}, fmt.Errorf("failed to get path build entity queue=%s pathID=%s attempt=%d from the database: %w", s.queue, pathID, attempt, err)
	}

	return pathBuild, nil
}

// Create records the build for an attempt, permanently. Returns
// ErrAlreadyExists if the attempt already has a build.
func (s *pathBuildStore) Create(ctx context.Context, pathBuild entity.PathBuild) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if pathBuild.Queue != s.queue {
		return fmt.Errorf("path build pathID=%s attempt=%d queue %q does not match the store's bound queue %q", pathBuild.PathID, pathBuild.Attempt, pathBuild.Queue, s.queue)
	}

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO path_build (queue, path_id, attempt, build_id) VALUES (?, ?, ?, ?)",
		s.queue, pathBuild.PathID, pathBuild.Attempt, pathBuild.BuildID,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("path build entity queue=%s pathID=%s attempt=%d: %w", s.queue, pathBuild.PathID, pathBuild.Attempt, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert path build entity queue=%s pathID=%s attempt=%d: %w", s.queue, pathBuild.PathID, pathBuild.Attempt, err)
	}

	return nil
}
