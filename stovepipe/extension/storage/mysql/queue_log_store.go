// Copyright (c) 2026 Uber Technologies, Inc.
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

type queueLogStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

func NewQueueLogStore(db *sql.DB, scope tally.Scope, queue string) storage.QueueLogStore {
	return &queueLogStore{db: db, scope: scope, queue: queue}
}

func (s *queueLogStore) Get(ctx context.Context, id string) (log entity.QueueLog, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx,
		"SELECT queue, log_id, timestamp_ms, state, queue_state_version, effective_from_commit_uri FROM queue_log WHERE queue = ? AND log_id = ?", s.queue, id).
		Scan(&log.Queue, &log.ID, &log.TimestampMs, &log.State, &log.QueueStateVersion, &log.EffectiveFromCommitURI)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.QueueLog{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.QueueLog{}, fmt.Errorf("read queue state log %q: %w", id, err)
	}
	return log, nil
}

func (s *queueLogStore) Create(ctx context.Context, log entity.QueueLog) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if log.Queue != s.queue {
		return fmt.Errorf("log queue differs from bound queue %q", s.queue)
	}
	if err := log.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO queue_log (queue, log_id, timestamp_ms, state, queue_state_version, effective_from_commit_uri) VALUES (?, ?, ?, ?, ?, ?)",
		log.Queue, log.ID, log.TimestampMs, log.State, log.QueueStateVersion, log.EffectiveFromCommitURI)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create queue state log %q: %w", log.ID, err)
	}
	return nil
}
