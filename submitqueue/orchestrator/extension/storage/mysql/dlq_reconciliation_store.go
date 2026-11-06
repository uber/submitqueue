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

type dlqReconciliationStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewDLQReconciliationStore binds reconciliation records to one queue.
func NewDLQReconciliationStore(db *sql.DB, scope tally.Scope, queue string) storage.DLQReconciliationStore {
	return &dlqReconciliationStore{db: db, scope: scope, queue: queue}
}

func (s *dlqReconciliationStore) Get(ctx context.Context, source entity.DLQReconciliationSource) (ret entity.DLQReconciliation, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { storage.CompleteReadMetricsOp(op, retErr) }()

	var batchIDsJSON []byte
	record := entity.DLQReconciliation{Queue: s.queue, Source: source}
	err := s.db.QueryRowContext(ctx,
		"SELECT id, batch_ids, completed, version FROM dlq_reconciliation WHERE queue = ? AND source_topic = ? AND source_partition_key = ? AND source_message_id = ?",
		s.queue, source.Topic, source.PartitionKey, source.MessageID,
	).Scan(&record.ID, &batchIDsJSON, &record.Completed, &record.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.DLQReconciliation{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.DLQReconciliation{}, fmt.Errorf("get reconciliation queue=%q source=%+v: %w", s.queue, source, err)
	}
	if err := json.Unmarshal(batchIDsJSON, &record.BatchIDs); err != nil {
		return entity.DLQReconciliation{}, fmt.Errorf("decode reconciliation targets queue=%q source=%+v: %w", s.queue, source, err)
	}
	return record, nil
}

func (s *dlqReconciliationStore) Create(ctx context.Context, record entity.DLQReconciliation) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if record.Queue != s.queue {
		return fmt.Errorf("reconciliation queue %q does not match bound queue %q", record.Queue, s.queue)
	}
	batchIDsJSON, err := json.Marshal(record.BatchIDs)
	if err != nil {
		return fmt.Errorf("encode reconciliation targets: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO dlq_reconciliation (queue, source_topic, source_partition_key, source_message_id, id, batch_ids, completed, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		record.Queue, record.Source.Topic, record.Source.PartitionKey, record.Source.MessageID, record.ID, batchIDsJSON, record.Completed, record.Version,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("reconciliation queue=%q source=%+v: %w", record.Queue, record.Source, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("create reconciliation queue=%q source=%+v: %w", record.Queue, record.Source, err)
	}
	return nil
}

func (s *dlqReconciliationStore) Update(ctx context.Context, record entity.DLQReconciliation, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if record.Queue != s.queue {
		return fmt.Errorf("reconciliation queue %q does not match bound queue %q", record.Queue, s.queue)
	}
	batchIDsJSON, err := json.Marshal(record.BatchIDs)
	if err != nil {
		return fmt.Errorf("encode reconciliation targets: %w", err)
	}
	result, err := s.db.ExecContext(ctx,
		"UPDATE dlq_reconciliation SET batch_ids = ?, completed = ?, version = ? WHERE queue = ? AND source_topic = ? AND source_partition_key = ? AND source_message_id = ? AND id = ? AND version = ?",
		batchIDsJSON, record.Completed, newVersion, record.Queue, record.Source.Topic, record.Source.PartitionKey, record.Source.MessageID, record.ID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("update reconciliation queue=%q source=%+v: %w", record.Queue, record.Source, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read reconciliation update result: %w", err)
	}
	if rowsAffected != 1 {
		return fmt.Errorf("reconciliation queue=%q source=%+v version=%d: %w", record.Queue, record.Source, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}
