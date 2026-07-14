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

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type buildStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewBuildStore creates a new MySQL-backed BuildStore.
func NewBuildStore(db *sql.DB, scope tally.Scope) storage.BuildStore {
	return &buildStore{db: db, scope: scope}
}

// Create persists a new build. Returns ErrAlreadyExists if the build ID already exists.
func (b *buildStore) Create(ctx context.Context, build entity.Build) (retErr error) {
	op := metrics.Begin(b.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := b.db.ExecContext(ctx,
		`INSERT INTO build (id, request_id, uri, base_uri, status, version)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		build.ID,
		build.RequestID,
		build.URI,
		build.BaseURI,
		build.Status,
		build.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("build entity id=%s: %w", build.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert build entity id=%s: %w", build.ID, err)
	}

	return nil
}

// Get retrieves a build by ID. Returns ErrNotFound if the build is not found.
func (b *buildStore) Get(ctx context.Context, id string) (ret entity.Build, retErr error) {
	op := metrics.Begin(b.scope, "get")
	defer func() { op.Complete(retErr) }()

	var build entity.Build
	err := b.db.QueryRowContext(ctx,
		`SELECT id, request_id, uri, base_uri, status, version
		 FROM build WHERE id = ?`,
		id,
	).Scan(
		&build.ID,
		&build.RequestID,
		&build.URI,
		&build.BaseURI,
		&build.Status,
		&build.Version,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Build{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Build{}, fmt.Errorf("failed to get build entity id=%s from the database: %w", id, err)
	}

	return build, nil
}

// Update persists the mutable fields of build (status) if the stored version matches
// oldVersion, writing newVersion. Returns ErrVersionMismatch if the stored version does not
// match (including when the build does not exist). This is a pure conditional write; the
// caller owns version arithmetic.
func (b *buildStore) Update(ctx context.Context, build entity.Build, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(b.scope, "update")
	defer func() { op.Complete(retErr) }()

	result, err := b.db.ExecContext(ctx,
		`UPDATE build
		 SET status = ?, version = ?
		 WHERE id = ? AND version = ?`,
		build.Status,
		newVersion,
		build.ID,
		oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update build id=%q oldVersion=%d newVersion=%d: %w",
			build.ID, oldVersion, newVersion, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q oldVersion=%d newVersion=%d: %w",
			build.ID, oldVersion, newVersion, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for build update: id=%q expected_version=%d: %w",
			build.ID, oldVersion, storage.ErrVersionMismatch,
		)
	}

	return nil
}
