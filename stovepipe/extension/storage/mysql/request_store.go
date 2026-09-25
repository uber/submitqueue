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
	"time"

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

	_, err := r.db.ExecContext(ctx,
		`INSERT INTO request (id, queue, uri, state, build_strategy, base_uri, version)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		request.ID,
		request.Queue,
		request.URI,
		request.State,
		request.BuildStrategy,
		request.BaseURI,
		request.Version,
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
	err := r.db.QueryRowContext(ctx,
		`SELECT id, queue, uri, state, build_strategy, base_uri, version
		 FROM request WHERE queue = ? AND id = ?`,
		r.queue, id,
	).Scan(
		&req.ID,
		&req.Queue,
		&req.URI,
		&req.State,
		&req.BuildStrategy,
		&req.BaseURI,
		&req.Version,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Request{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Request{}, fmt.Errorf("failed to get request entity id=%s from the database: %w", id, err)
	}

	return req, nil
}

// Update persists the mutable fields of request (uri, state, build strategy, base uri) if the
// oldVersion, writing newVersion. Returns ErrVersionMismatch if the stored version does not match
// (including when the request does not exist). This is a pure conditional write; the caller owns
// version arithmetic.
func (r *requestStore) Update(ctx context.Context, request entity.Request, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(r.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if request.Queue != r.queue {
		return fmt.Errorf("request %s queue %q does not match the store's bound queue %q", request.ID, request.Queue, r.queue)
	}

	result, err := r.db.ExecContext(ctx,
		`UPDATE request
		 SET uri = ?, state = ?, build_strategy = ?, base_uri = ?, version = ?
		 WHERE queue = ? AND id = ? AND version = ?`,
		request.URI,
		request.State,
		request.BuildStrategy,
		request.BaseURI,
		newVersion,
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

// FinalizeOutcome atomically advances a Request to its terminal state and retains
// the matching state entry. Keeping the winning build id in that immutable entry
// means a crash cannot leave a terminal request without its selected build.
func (r *requestStore) FinalizeOutcome(ctx context.Context, request entity.Request, oldVersion, newVersion int32, log entity.RequestLog) (retErr error) {
	op := metrics.Begin(r.scope, "finalize_outcome", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if request.Queue != r.queue || log.Queue != r.queue || log.RequestID != request.ID {
		return fmt.Errorf("request outcome queue or request binding does not match store")
	}
	if log.TimestampMs == 0 {
		log.TimestampMs = time.Now().UnixMilli()
	}
	if err := log.Validate(); err != nil {
		return fmt.Errorf("invalid terminal request log: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin request outcome transaction: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	result, err := tx.ExecContext(ctx,
		`UPDATE request SET uri = ?, state = ?, build_strategy = ?, base_uri = ?, version = ?
		 WHERE queue = ? AND id = ? AND version = ?`,
		request.URI, request.State, request.BuildStrategy, request.BaseURI, newVersion,
		request.Queue, request.ID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to finalize request outcome: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to inspect finalized request outcome: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("version mismatch for request outcome: id=%q expected_version=%d: %w", request.ID, oldVersion, storage.ErrVersionMismatch)
	}

	metadata, err := json.Marshal(log.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal terminal request log metadata: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO request_log (
		queue, request_id, log_id, timestamp_ms, state, event, request_version, outcome_reason, metadata
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		log.Queue, log.RequestID, log.ID, log.TimestampMs, log.State, log.Event, log.RequestVersion, log.OutcomeReason, metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to retain terminal request log: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit request outcome: %w", err)
	}
	return nil
}

// isDuplicateEntry reports whether err is a MySQL duplicate-key (1062) error.
func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry
}
