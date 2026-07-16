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
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestStore creates a new MySQL-backed RequestStore.
func NewRequestStore(db *sql.DB, scope tally.Scope) storage.RequestStore {
	return &requestStore{db: db, scope: scope}
}

// Get retrieves a land request by ID. Returns ErrNotFound if the request is not found.
func (r *requestStore) Get(ctx context.Context, id string) (ret entity.Request, retErr error) {
	op := metrics.Begin(r.scope, "get")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	var req entity.Request
	var changeURIsJSON []byte

	err := r.db.QueryRowContext(ctx,
		"SELECT id, queue, change_uri, land_strategy, state, version FROM request WHERE id = ?",
		id,
	).Scan(&req.ID, &req.Queue, &changeURIsJSON, &req.LandStrategy, &req.State, &req.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Request{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Request{}, fmt.Errorf("failed to get request entity id=%s from the database: %w", id, err)
	}

	// Unmarshal the change URIs from JSON
	if err := json.Unmarshal(changeURIsJSON, &req.Change.URIs); err != nil {
		return entity.Request{}, fmt.Errorf("failed to unmarshal change URIs for request id=%s: %w", id, err)
	}

	return req, nil
}

// Create creates a new land request. The request must have a unique ID already assigned. Returns ErrAlreadyExists if the request ID already exists.
func (r *requestStore) Create(ctx context.Context, request entity.Request) (retErr error) {
	op := metrics.Begin(r.scope, "create")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	// Marshal the change URIs to JSON
	changeURIsJSON, err := json.Marshal(request.Change.URIs)
	if err != nil {
		return fmt.Errorf("failed to marshal change URIs for request id=%s: %w", request.ID, err)
	}

	_, err = r.db.ExecContext(ctx,
		"INSERT INTO request (id, queue, change_uri, land_strategy, state, version) VALUES (?, ?, ?, ?, ?, ?)",
		request.ID, request.Queue, changeURIsJSON, request.LandStrategy, request.State, request.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("request entity id=%s: %w", request.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request entity id=%s: %w", request.ID, err)
	}

	return nil
}

// UpdateState updates the state of a land request to newState and the version to newVersion
// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
// Version arithmetic is owned by the caller; this is a pure conditional write.
func (r *requestStore) UpdateState(ctx context.Context, id string, oldVersion, newVersion int32, newState entity.RequestState) (retErr error) {
	op := metrics.Begin(r.scope, "update_state")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	result, err := r.db.ExecContext(ctx,
		"UPDATE request SET state = ?, version = ? WHERE id = ? AND version = ?",
		newState, newVersion, id, oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update request state for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			id, oldVersion, newVersion, newState, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for id=%q oldVersion=%d newVersion=%d newState=%v: %w",
			id, oldVersion, newVersion, newState, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for request update: id=%q expected_version=%d newState=%v: %w",
			id, oldVersion, newState, storage.ErrVersionMismatch,
		)
	}

	return nil
}
