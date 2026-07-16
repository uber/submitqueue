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

type queueStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewQueueStore creates a new MySQL-backed QueueStore.
func NewQueueStore(db *sql.DB, scope tally.Scope) storage.QueueStore {
	return &queueStore{db: db, scope: scope}
}

// Create persists a new queue row. Returns ErrAlreadyExists if the name already exists.
func (q *queueStore) Create(ctx context.Context, queue entity.Queue) (retErr error) {
	op := metrics.Begin(q.scope, "create")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	_, err := q.db.ExecContext(ctx,
		`INSERT INTO queue (name, last_green_uri, in_flight_count, latest_request_id, version)
		 VALUES (?, ?, ?, ?, ?)`,
		queue.Name,
		queue.LastGreenURI,
		queue.InFlightCount,
		queue.LatestRequestID,
		queue.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("queue name=%s: %w", queue.Name, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert queue name=%s: %w", queue.Name, err)
	}
	return nil
}

// Get retrieves a queue by name. Returns ErrNotFound if the queue is not found.
func (q *queueStore) Get(ctx context.Context, name string) (ret entity.Queue, retErr error) {
	op := metrics.Begin(q.scope, "get")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	var queue entity.Queue
	err := q.db.QueryRowContext(ctx,
		"SELECT name, last_green_uri, in_flight_count, latest_request_id, version FROM queue WHERE name = ?",
		name,
	).Scan(
		&queue.Name,
		&queue.LastGreenURI,
		&queue.InFlightCount,
		&queue.LatestRequestID,
		&queue.Version,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.Queue{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.Queue{}, fmt.Errorf("failed to get queue name=%s from the database: %w", name, err)
	}

	return queue, nil
}

// Update persists the mutable fields of queue if the stored version matches oldVersion,
// writing newVersion. Returns ErrVersionMismatch if the stored version does not match.
func (q *queueStore) Update(ctx context.Context, queue entity.Queue, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(q.scope, "update")
	defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

	result, err := q.db.ExecContext(ctx,
		`UPDATE queue
		 SET last_green_uri = ?, in_flight_count = ?, latest_request_id = ?, version = ?
		 WHERE name = ? AND version = ?`,
		queue.LastGreenURI,
		queue.InFlightCount,
		queue.LatestRequestID,
		newVersion,
		queue.Name,
		oldVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update queue name=%q oldVersion=%d newVersion=%d: %w",
			queue.Name, oldVersion, newVersion, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for name=%q oldVersion=%d newVersion=%d: %w",
			queue.Name, oldVersion, newVersion, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for queue update: name=%q expected_version=%d: %w",
			queue.Name, oldVersion, storage.ErrVersionMismatch,
		)
	}

	return nil
}
