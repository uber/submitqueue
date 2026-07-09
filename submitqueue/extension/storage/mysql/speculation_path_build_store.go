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

type speculationPathBuildStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewSpeculationPathBuildStore creates a new MySQL-backed SpeculationPathBuildStore.
func NewSpeculationPathBuildStore(db *sql.DB, scope tally.Scope) storage.SpeculationPathBuildStore {
	return &speculationPathBuildStore{db: db, scope: scope}
}

// Create creates a new path->build mapping. Returns ErrAlreadyExists if a
// mapping for the given PathID already exists. The caller-supplied Version is
// persisted as-is; version arithmetic is owned by the controller, not the store.
func (s *speculationPathBuildStore) Create(ctx context.Context, pathBuild entity.SpeculationPathBuild) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO speculation_path_build (path_id, build_id, batch_id, version, created_at) VALUES (?, ?, ?, ?, ?)",
		pathBuild.PathID, pathBuild.BuildID, pathBuild.BatchID, pathBuild.Version, pathBuild.CreatedAt,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("path build mapping pathID=%s: %w", pathBuild.PathID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert path build mapping pathID=%s: %w", pathBuild.PathID, err)
	}

	return nil
}

// Get retrieves the path->build mapping for the given path ID. Returns
// ErrNotFound if no mapping exists for pathID.
func (s *speculationPathBuildStore) Get(ctx context.Context, pathID string) (ret entity.SpeculationPathBuild, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	var pb entity.SpeculationPathBuild

	err := s.db.QueryRowContext(ctx,
		"SELECT path_id, build_id, batch_id, version, created_at FROM speculation_path_build WHERE path_id = ?",
		pathID,
	).Scan(&pb.PathID, &pb.BuildID, &pb.BatchID, &pb.Version, &pb.CreatedAt)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.SpeculationPathBuild{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.SpeculationPathBuild{}, fmt.Errorf("failed to get path build mapping pathID=%s from the database: %w", pathID, err)
	}

	return pb, nil
}
