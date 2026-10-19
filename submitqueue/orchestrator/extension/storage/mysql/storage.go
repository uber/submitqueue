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
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	storage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique or primary key violation.
// It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

// Storage is the MySQL storage backend. It owns the shared connection pool and
// binds queue-scoped store aggregates over the shared tables on demand via For.
// The wiring layer adapts For into the storage.Factory seam; per-queue backend
// routing stays a host decision.
type Storage struct {
	db                       *sql.DB
	scope                    tally.Scope
	queueRequestStateRecords bool
}

// StorageOption configures a Storage.
type StorageOption func(*Storage)

// WithQueueRequestStateRecords keeps the queue_request_state membership records in
// step with every request write and serves them through GetQueueRequestStateStore.
// The queue_request_state table must exist before such a Storage writes a request.
func WithQueueRequestStateRecords() StorageOption {
	return func(s *Storage) { s.queueRequestStateRecords = true }
}

// NewStorage creates a new MySQL storage backend over the given connection pool.
func NewStorage(db *sql.DB, scope tally.Scope, opts ...StorageOption) (*Storage, error) {
	s := &Storage{db: db, scope: scope}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// For returns the queue-scoped store aggregate bound to queueName over the
// shared pool. Every store the aggregate hands back reads and writes only that
// queue's records.
func (s *Storage) For(queueName string) (storage.Storage, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name must not be empty")
	}
	var queueRequestStateStore basestorage.QueueRequestStateStore = disabledQueueRequestStateStore{}
	if s.queueRequestStateRecords {
		queueRequestStateStore = NewQueueRequestStateStore(s.db, s.scope.SubScope("queue_request_state_store"), queueName)
	}
	return &boundStorage{
		requestStore:            newRequestStore(s.db, s.scope.SubScope("request_store"), queueName, s.queueRequestStateRecords),
		requestBatchStore:       NewRequestBatchStore(s.db, s.scope.SubScope("request_batch_store"), queueName),
		changeStore:             NewChangeStore(s.db, s.scope.SubScope("change_store"), queueName),
		batchStore:              NewBatchStore(s.db, s.scope.SubScope("batch_store"), queueName),
		batchDependentStore:     NewBatchDependentStore(s.db, s.scope.SubScope("batch_dependent_store"), queueName),
		queueBatchStateStore:    NewQueueBatchStateStore(s.db, s.scope.SubScope("queue_batch_state_store"), queueName),
		queueRequestStateStore:  queueRequestStateStore,
		buildStore:              NewBuildStore(s.db, s.scope.SubScope("build_store"), queueName),
		speculationPathSetStore: NewSpeculationPathSetStore(s.db, s.scope.SubScope("speculation_path_set_store"), queueName),
		pathBuildStore:          NewPathBuildStore(s.db, s.scope.SubScope("path_build_store"), queueName),
	}, nil
}

// Close closes the underlying database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// boundStorage is the queue-scoped store aggregate returned by For.
type boundStorage struct {
	requestStore            basestorage.RequestStore
	requestBatchStore       basestorage.RequestBatchStore
	changeStore             basestorage.ChangeStore
	batchStore              basestorage.BatchStore
	batchDependentStore     basestorage.BatchDependentStore
	queueBatchStateStore    basestorage.QueueBatchStateStore
	queueRequestStateStore  basestorage.QueueRequestStateStore
	buildStore              basestorage.BuildStore
	speculationPathSetStore basestorage.SpeculationPathSetStore
	pathBuildStore          basestorage.PathBuildStore
}

// Verify boundStorage implements the queue-scoped aggregate at compile time.
var _ storage.Storage = (*boundStorage)(nil)

// GetRequestStore returns the bound MySQL-backed RequestStore.
func (f *boundStorage) GetRequestStore() basestorage.RequestStore {
	return f.requestStore
}

// GetRequestBatchStore returns the bound MySQL-backed RequestBatchStore.
func (f *boundStorage) GetRequestBatchStore() basestorage.RequestBatchStore {
	return f.requestBatchStore
}

// GetChangeStore returns the bound MySQL-backed ChangeStore.
func (f *boundStorage) GetChangeStore() basestorage.ChangeStore {
	return f.changeStore
}

// GetBatchStore returns the bound MySQL-backed BatchStore.
func (f *boundStorage) GetBatchStore() basestorage.BatchStore {
	return f.batchStore
}

// GetBatchDependentStore returns the bound MySQL-backed BatchDependentStore.
func (f *boundStorage) GetBatchDependentStore() basestorage.BatchDependentStore {
	return f.batchDependentStore
}

// GetQueueBatchStateStore returns the bound MySQL-backed QueueBatchStateStore.
func (f *boundStorage) GetQueueBatchStateStore() basestorage.QueueBatchStateStore {
	return f.queueBatchStateStore
}

// GetQueueRequestStateStore returns the bound MySQL-backed QueueRequestStateStore. Its
// List fails unless the Storage was built WithQueueRequestStateRecords.
func (f *boundStorage) GetQueueRequestStateStore() basestorage.QueueRequestStateStore {
	return f.queueRequestStateStore
}

// GetBuildStore returns the bound MySQL-backed BuildStore.
func (f *boundStorage) GetBuildStore() basestorage.BuildStore {
	return f.buildStore
}

// GetSpeculationPathSetStore returns the bound MySQL-backed SpeculationPathSetStore.
func (f *boundStorage) GetSpeculationPathSetStore() basestorage.SpeculationPathSetStore {
	return f.speculationPathSetStore
}

// GetPathBuildStore returns the bound MySQL-backed PathBuildStore.
func (f *boundStorage) GetPathBuildStore() basestorage.PathBuildStore {
	return f.pathBuildStore
}
