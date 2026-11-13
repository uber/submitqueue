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
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique/primary
// key violation. It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

type requestStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewRequestStore creates a new MySQL-backed RequestStore.
func NewRequestStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestStore {
	return &requestStore{db: db, scope: scope, queue: queue}
}

// Create persists a new request. Returns ErrAlreadyExists if the request ID already exists.
func (r *requestStore) Create(ctx context.Context, request entity.Request) (retErr error) {
	op := metrics.Begin(r.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if request.Queue != r.queue {
		return fmt.Errorf("request %s queue %q does not match the store's bound queue %q", request.ID, request.Queue, r.queue)
	}

	metadata, err := encodeRequestMetadata(request.Metadata)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO request (id, queue, uri, state, build_strategy, base_uri, build_id, version, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		request.ID,
		request.Queue,
		request.URI,
		request.State,
		request.BuildStrategy,
		request.BaseURI,
		request.BuildID,
		request.Version,
		metadata,
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
	op := metrics.Begin(r.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var req entity.Request
	var metadata []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT id, queue, uri, state, build_strategy, base_uri, build_id, COALESCE(terminal_build_id, ''), version, metadata
		 FROM request WHERE queue = ? AND id = ?`,
		r.queue, id,
	).Scan(
		&req.ID,
		&req.Queue,
		&req.URI,
		&req.State,
		&req.BuildStrategy,
		&req.BaseURI,
		&req.BuildID,
		&req.TerminalBuildID,
		&req.Version,
		&metadata,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Request{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Request{}, fmt.Errorf("failed to get request entity id=%s from the database: %w", id, err)
	}

	req.Metadata, err = decodeRequestMetadata(metadata)
	if err != nil {
		return entity.Request{}, err
	}
	return req, nil
}

// Update persists the mutable fields of request (URI, state, build strategy,
// base URI, associated build ID, terminal build ID, metadata) if the oldVersion, writing newVersion. Returns
// ErrVersionMismatch if the stored version does not match (including when the
// request does not exist). This is a pure conditional write; the caller owns
// version arithmetic.
func (r *requestStore) Update(ctx context.Context, request entity.Request, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(r.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if request.Queue != r.queue {
		return fmt.Errorf("request %s queue %q does not match the store's bound queue %q", request.ID, request.Queue, r.queue)
	}

	metadata, err := encodeRequestMetadata(request.Metadata)
	if err != nil {
		return err
	}
	result, err := r.db.ExecContext(ctx,
		`UPDATE request
		 SET uri = ?, state = ?, build_strategy = ?, base_uri = ?, build_id = ?, terminal_build_id = NULLIF(?, ''), version = ?, metadata = ?
		 WHERE queue = ? AND id = ? AND version = ?`,
		request.URI,
		request.State,
		request.BuildStrategy,
		request.BaseURI,
		request.BuildID,
		request.TerminalBuildID,
		newVersion,
		metadata,
		request.Queue,
		request.ID,
		oldVersion,
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

func encodeRequestMetadata(metadata map[string]json.RawMessage) ([]byte, error) {
	if len(metadata) == 0 {
		return []byte("{}"), nil
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode request metadata: %w", err)
	}
	return data, nil
}

func decodeRequestMetadata(data []byte) (map[string]json.RawMessage, error) {
	var metadata map[string]json.RawMessage
	if len(data) > 0 {
		if err := json.Unmarshal(data, &metadata); err != nil {
			return nil, fmt.Errorf("decode request metadata: %w", err)
		}
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	return metadata, nil
}

// isDuplicateEntry reports whether err is a MySQL duplicate-key (1062) error.
func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry
}
