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
	storage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique or primary key violation.
// It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

// Storage is the MySQL backend for the gateway's stores. It owns the shared
// connection pool and binds queue-scoped store aggregates over the shared
// tables on demand via For. The wiring layer adapts For into the
// storage.Factory seam; per-queue backend routing stays a host decision.
type Storage struct {
	db    *sql.DB
	scope tally.Scope
}

// NewStorage creates a new MySQL storage backend over the given connection pool.
func NewStorage(db *sql.DB, scope tally.Scope) (*Storage, error) {
	return &Storage{db: db, scope: scope}, nil
}

// For returns the queue-scoped store aggregate bound to queueName over the
// shared pool. Every store the aggregate hands back reads and writes only that
// queue's records.
func (s *Storage) For(queueName string) (storage.Storage, error) {
	if queueName == "" {
		return nil, fmt.Errorf("queue name must not be empty")
	}
	return &boundStorage{
		requestQueueStore:   NewRequestQueueSummaryStore(s.db, s.scope.SubScope("request_queue_summary_store"), queueName),
		requestSummaryStore: NewRequestSummaryStore(s.db, s.scope.SubScope("request_summary_store"), queueName),
		requestLogStore:     NewRequestLogStore(s.db, s.scope.SubScope("request_log_store"), queueName),
		requestURIStore:     NewRequestURIStore(s.db, s.scope.SubScope("request_uri_store"), queueName),
	}, nil
}

// Close closes the underlying database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// boundStorage is the queue-scoped store aggregate returned by For.
type boundStorage struct {
	requestQueueStore   basestorage.RequestQueueSummaryStore
	requestSummaryStore basestorage.RequestSummaryStore
	requestLogStore     basestorage.RequestLogStore
	requestURIStore     basestorage.RequestURIStore
}

// Verify boundStorage implements the queue-scoped aggregate at compile time.
var _ storage.Storage = (*boundStorage)(nil)

// GetRequestQueueSummaryStore returns the bound MySQL-backed RequestQueueSummaryStore.
func (f *boundStorage) GetRequestQueueSummaryStore() basestorage.RequestQueueSummaryStore {
	return f.requestQueueStore
}

// GetRequestSummaryStore returns the bound MySQL-backed RequestSummaryStore.
func (f *boundStorage) GetRequestSummaryStore() basestorage.RequestSummaryStore {
	return f.requestSummaryStore
}

// GetRequestLogStore returns the bound MySQL-backed RequestLogStore.
func (f *boundStorage) GetRequestLogStore() basestorage.RequestLogStore {
	return f.requestLogStore
}

// GetRequestURIStore returns the bound MySQL-backed RequestURIStore.
func (f *boundStorage) GetRequestURIStore() basestorage.RequestURIStore {
	return f.requestURIStore
}
