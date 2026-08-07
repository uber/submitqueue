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

	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique or primary key violation.
// It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

// Storage is the MySQL storage backend. It owns the shared connection pool and
// the global read-model stores, and binds queue-scoped store aggregates over
// the shared tables on demand via For. The wiring layer adapts For into the
// storage.Factory seam; per-queue backend routing stays a host decision.
type Storage struct {
	db    *sql.DB
	scope tally.Scope

	requestLogStore     storage.RequestLogStore
	requestSummaryStore storage.RequestSummaryStore
	requestURIStore     storage.RequestURIStore
}

// NewStorage creates a new MySQL storage backend over the given connection pool.
func NewStorage(db *sql.DB, scope tally.Scope) (*Storage, error) {
	return &Storage{
		db:                  db,
		scope:               scope,
		requestLogStore:     NewRequestLogStore(db, scope.SubScope("request_log_store")),
		requestSummaryStore: NewRequestSummaryStore(db, scope.SubScope("request_summary_store")),
		requestURIStore:     NewRequestURIStore(db, scope.SubScope("request_uri_store")),
	}, nil
}

// For returns the queue-scoped store aggregate bound to queueName over the
// shared pool. Every store the aggregate hands back reads and writes only that
// queue's records.
func (s *Storage) For(queueName string) (storage.Storage, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name must not be empty")
	}
	return &boundStorage{
		requestStore:         NewRequestStore(s.db, s.scope.SubScope("request_store"), queueName),
		requestBatchStore:    NewRequestBatchStore(s.db, s.scope.SubScope("request_batch_store"), queueName),
		changeStore:          NewChangeStore(s.db, s.scope.SubScope("change_store"), queueName),
		batchStore:           NewBatchStore(s.db, s.scope.SubScope("batch_store"), queueName),
		batchDependentStore:  NewBatchDependentStore(s.db, s.scope.SubScope("batch_dependent_store"), queueName),
		queueBatchStateStore: NewQueueBatchStateStore(s.db, s.scope.SubScope("queue_batch_state_store"), queueName),
		buildStore:           NewBuildStore(s.db, s.scope.SubScope("build_store"), queueName),
		requestQueueStore:    NewRequestQueueSummaryStore(s.db, s.scope.SubScope("request_queue_summary_store"), queueName),
	}, nil
}

// GetRequestLogStore returns the global MySQL-backed RequestLogStore.
func (s *Storage) GetRequestLogStore() storage.RequestLogStore {
	return s.requestLogStore
}

// GetRequestSummaryStore returns the global MySQL-backed RequestSummaryStore.
func (s *Storage) GetRequestSummaryStore() storage.RequestSummaryStore {
	return s.requestSummaryStore
}

// GetRequestURIStore returns the global MySQL-backed RequestURIStore.
func (s *Storage) GetRequestURIStore() storage.RequestURIStore {
	return s.requestURIStore
}

// Close closes the underlying database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// boundStorage is the queue-scoped store aggregate returned by For.
type boundStorage struct {
	requestStore         storage.RequestStore
	requestBatchStore    storage.RequestBatchStore
	changeStore          storage.ChangeStore
	batchStore           storage.BatchStore
	batchDependentStore  storage.BatchDependentStore
	queueBatchStateStore storage.QueueBatchStateStore
	buildStore           storage.BuildStore
	requestQueueStore    storage.RequestQueueSummaryStore
}

// Verify boundStorage implements the queue-scoped aggregate at compile time.
var _ storage.Storage = (*boundStorage)(nil)

// GetRequestStore returns the bound MySQL-backed RequestStore.
func (f *boundStorage) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// GetRequestBatchStore returns the bound MySQL-backed RequestBatchStore.
func (f *boundStorage) GetRequestBatchStore() storage.RequestBatchStore {
	return f.requestBatchStore
}

// GetChangeStore returns the bound MySQL-backed ChangeStore.
func (f *boundStorage) GetChangeStore() storage.ChangeStore {
	return f.changeStore
}

// GetBatchStore returns the bound MySQL-backed BatchStore.
func (f *boundStorage) GetBatchStore() storage.BatchStore {
	return f.batchStore
}

// GetBatchDependentStore returns the bound MySQL-backed BatchDependentStore.
func (f *boundStorage) GetBatchDependentStore() storage.BatchDependentStore {
	return f.batchDependentStore
}

// GetQueueBatchStateStore returns the bound MySQL-backed QueueBatchStateStore.
func (f *boundStorage) GetQueueBatchStateStore() storage.QueueBatchStateStore {
	return f.queueBatchStateStore
}

// GetBuildStore returns the bound MySQL-backed BuildStore.
func (f *boundStorage) GetBuildStore() storage.BuildStore {
	return f.buildStore
}

// GetRequestQueueSummaryStore returns the bound MySQL-backed RequestQueueSummaryStore.
func (f *boundStorage) GetRequestQueueSummaryStore() storage.RequestQueueSummaryStore {
	return f.requestQueueStore
}
