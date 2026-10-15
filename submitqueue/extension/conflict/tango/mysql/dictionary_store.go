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

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type dictionaryStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewDictionaryStore creates a new MySQL-backed TargetDictionaryStore.
func NewDictionaryStore(db *sql.DB, scope tally.Scope, queue string) tango.DictionaryStore {
	return &dictionaryStore{db: db, scope: scope, queue: queue}
}

// Create inserts a label. Returns ErrAlreadyExists if the ID is already recorded.
func (s *dictionaryStore) Create(ctx context.Context, label tango.TargetLabel) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if label.Queue != s.queue {
		return fmt.Errorf("target label id=%d queue %q does not match the store's bound queue %q", label.ID, label.Queue, s.queue)
	}

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO target_dictionary (queue, id, label, created_at) VALUES (?, ?, ?, ?)",
		label.Queue, label.ID, label.Label, label.CreatedAt,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("target label id=%d: %w", label.ID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert target label id=%d: %w", label.ID, err)
	}
	return nil
}

// Get returns the bound queue's label for id. The bool is false when none is
// recorded.
func (s *dictionaryStore) Get(ctx context.Context, id uint64) (ret tango.TargetLabel, found bool, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	label := tango.TargetLabel{Queue: s.queue, ID: id}
	err := s.db.QueryRowContext(ctx,
		"SELECT label, created_at FROM target_dictionary WHERE queue = ? AND id = ?",
		s.queue, id,
	).Scan(&label.Label, &label.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return tango.TargetLabel{}, false, nil
	}
	if err != nil {
		return tango.TargetLabel{}, false, fmt.Errorf("failed to get target label id=%d from the database: %w", id, err)
	}
	return label, true, nil
}
