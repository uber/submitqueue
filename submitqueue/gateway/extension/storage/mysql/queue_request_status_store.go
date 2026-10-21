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

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// errQueueRequestStatusRecordsDisabled is returned by List on a storage built without
// WithQueueRequestStatusRecords: the records are not kept, so an empty listing would be
// indistinguishable from an empty queue.
var errQueueRequestStatusRecordsDisabled = errors.New("queue request status records are not enabled for this storage")

type queueRequestStatusStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewQueueRequestStatusStore creates a new MySQL-backed QueueRequestStatusStore.
func NewQueueRequestStatusStore(db *sql.DB, scope tally.Scope, queue string) storage.QueueRequestStatusStore {
	return &queueRequestStatusStore{db: db, scope: scope, queue: queue}
}

// List returns every record filed under the bound queue's status bucket. The WHERE clause
// is a prefix of the (queue, status, request_id) PK, so this is a PK-prefix scan.
func (s *queueRequestStatusStore) List(ctx context.Context, status entity.RequestStatus) (ret []entity.QueueRequestStatus, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { storage.CompleteReadMetricsOp(op, retErr) }()

	queue := s.queue
	const query = "SELECT queue, status, request_id, received_at_ms, entered_at_ms FROM queue_request_status WHERE queue = ? AND status = ?"
	rows, err := s.db.QueryContext(ctx, query, queue, string(status))
	if err != nil {
		return nil, fmt.Errorf("failed to query queue request status records for queue=%s status=%s: %w", queue, status, err)
	}
	defer rows.Close()

	var results []entity.QueueRequestStatus
	for rows.Next() {
		var rec entity.QueueRequestStatus
		if err := rows.Scan(&rec.Queue, &rec.Status, &rec.RequestID, &rec.ReceivedAtMs, &rec.EnteredAtMs); err != nil {
			return nil, fmt.Errorf("failed to scan queue request status record for queue=%s status=%s: %w", queue, status, err)
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate queue request status records for queue=%s status=%s: %w", queue, status, err)
	}
	return results, nil
}

// Put inserts a record. A primary-key conflict on (queue, status, request_id) is a no-op
// update that keeps the stored entered_at_ms; unlike INSERT IGNORE, it does not downgrade
// other errors to warnings.
func (s *queueRequestStatusStore) Put(ctx context.Context, record entity.QueueRequestStatus) (retErr error) {
	op := metrics.Begin(s.scope, "put", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if record.Queue != s.queue {
		return fmt.Errorf("queue request status record request_id=%s queue %q does not match the store's bound queue %q", record.RequestID, record.Queue, s.queue)
	}

	const query = "INSERT INTO queue_request_status (queue, status, request_id, received_at_ms, entered_at_ms) VALUES (?, ?, ?, ?, ?) " +
		"ON DUPLICATE KEY UPDATE entered_at_ms = entered_at_ms"
	if _, err := s.db.ExecContext(ctx, query, record.Queue, string(record.Status), record.RequestID, record.ReceivedAtMs, record.EnteredAtMs); err != nil {
		return fmt.Errorf("failed to put queue request status record queue=%s status=%s request_id=%s: %w", record.Queue, record.Status, record.RequestID, err)
	}
	return nil
}

// Delete removes the bound queue's record identified by (status, requestID). Deleting an
// absent record is a no-op success — rows-affected is intentionally not checked.
func (s *queueRequestStatusStore) Delete(ctx context.Context, status entity.RequestStatus, requestID string) (retErr error) {
	op := metrics.Begin(s.scope, "delete", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	queue := s.queue
	const query = "DELETE FROM queue_request_status WHERE queue = ? AND status = ? AND request_id = ?"
	if _, err := s.db.ExecContext(ctx, query, queue, string(status), requestID); err != nil {
		return fmt.Errorf("failed to delete queue request status record queue=%s status=%s request_id=%s: %w", queue, status, requestID, err)
	}
	return nil
}

// disabledQueueRequestStatusStore stands in when the deployment keeps no records, for
// example before the table exists in its datastore. Writes are no-ops so the materializer
// runs unchanged; List fails rather than report every queue as empty.
type disabledQueueRequestStatusStore struct{}

func (disabledQueueRequestStatusStore) List(context.Context, entity.RequestStatus) ([]entity.QueueRequestStatus, error) {
	return nil, errQueueRequestStatusRecordsDisabled
}

func (disabledQueueRequestStatusStore) Put(context.Context, entity.QueueRequestStatus) error {
	return nil
}

func (disabledQueueRequestStatusStore) Delete(context.Context, entity.RequestStatus, string) error {
	return nil
}
