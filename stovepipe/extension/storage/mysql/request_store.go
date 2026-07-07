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
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique/primary
// key violation. It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

type requestStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestStore creates a new MySQL-backed RequestStore.
func NewRequestStore(db *sql.DB, scope tally.Scope) storage.RequestStore {
	return &requestStore{db: db, scope: scope}
}

// Create persists a new request. Returns ErrAlreadyExists if the request ID already exists.
func (r *requestStore) Create(ctx context.Context, request entity.Request) (retErr error) {
	op := metrics.Begin(r.scope, "create")
	defer func() { op.Complete(retErr) }()

	_, err := r.db.ExecContext(ctx,
		"INSERT INTO request (id, queue, uri, state, version) VALUES (?, ?, ?, ?, ?)",
		request.ID, request.Queue, request.URI, request.State, request.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("request entity id=%s: %w", request.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request entity id=%s: %w", request.ID, err)
	}

	return nil
}

// Get retrieves a request by ID. Returns ErrNotFound if the request is not found.
func (r *requestStore) Get(ctx context.Context, id string) (ret entity.Request, retErr error) {
	op := metrics.Begin(r.scope, "get")
	defer func() { op.Complete(retErr) }()

	var req entity.Request
	err := r.db.QueryRowContext(ctx,
		"SELECT id, queue, uri, state, version FROM request WHERE id = ?",
		id,
	).Scan(&req.ID, &req.Queue, &req.URI, &req.State, &req.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Request{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Request{}, fmt.Errorf("failed to get request entity id=%s from the database: %w", id, err)
	}

	return req, nil
}

// Update persists the mutable fields of request (uri, state) if the stored version matches
// oldVersion, writing newVersion. Returns ErrVersionMismatch if the stored version does not match
// (including when the request does not exist). This is a pure conditional write; the caller owns
// version arithmetic.
func (r *requestStore) Update(ctx context.Context, request entity.Request, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(r.scope, "update")
	defer func() { op.Complete(retErr) }()

	result, err := r.db.ExecContext(ctx,
		"UPDATE request SET uri = ?, state = ?, version = ? WHERE id = ? AND version = ?",
		request.URI, request.State, newVersion, request.ID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update request id=%q oldVersion=%d newVersion=%d: %w",
			request.ID, oldVersion, newVersion, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q oldVersion=%d newVersion=%d: %w",
			request.ID, oldVersion, newVersion, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for request update: id=%q expected_version=%d: %w",
			request.ID, oldVersion, storage.ErrVersionMismatch,
		)
	}

	return nil
}

// isDuplicateEntry reports whether err is a MySQL duplicate-key (1062) error.
func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry
}
