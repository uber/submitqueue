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
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// Storage is the MySQL storage backend. It owns the shared connection pool and
// binds queue-scoped store aggregates over the shared tables on demand via For.
// The wiring layer adapts For into the storage.Factory seam; per-queue backend
// routing stays a host decision.
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
	return &mysqlStorage{
		requestStore:        NewRequestStore(s.db, s.scope.SubScope("request_store"), queueName),
		requestURIStore:     NewRequestURIStore(s.db, s.scope.SubScope("request_uri_store"), queueName),
		requestLogStore:     NewRequestLogStore(s.db, s.scope.SubScope("request_log_store"), queueName),
		queueStore:          NewQueueStore(s.db, s.scope.SubScope("queue_store"), queueName),
		buildStore:          NewBuildStore(s.db, s.scope.SubScope("build_store"), queueName),
		validationFactStore: NewValidationFactStore(s.db, s.scope.SubScope("validation_fact_store"), queueName),
	}, nil
}

// Close closes the underlying database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// mysqlStorage is the queue-scoped store aggregate returned by For.
type mysqlStorage struct {
	requestStore        storage.RequestStore
	requestURIStore     storage.RequestURIStore
	requestLogStore     storage.RequestLogStore
	queueStore          storage.QueueStore
	buildStore          storage.BuildStore
	validationFactStore storage.ValidationFactStore
}

// Verify mysqlStorage implements the queue-scoped aggregate at compile time.
var _ storage.Storage = (*mysqlStorage)(nil)

// GetRequestStore returns the MySQL-backed RequestStore.
func (f *mysqlStorage) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// GetRequestURIStore returns the MySQL-backed RequestURIStore.
func (f *mysqlStorage) GetRequestURIStore() storage.RequestURIStore {
	return f.requestURIStore
}

// GetRequestLogStore returns the MySQL-backed RequestLogStore.
func (f *mysqlStorage) GetRequestLogStore() storage.RequestLogStore {
	return f.requestLogStore
}

// GetQueueStore returns the MySQL-backed QueueStore.
func (f *mysqlStorage) GetQueueStore() storage.QueueStore {
	return f.queueStore
}

// GetBuildStore returns the MySQL-backed BuildStore.
func (f *mysqlStorage) GetBuildStore() storage.BuildStore {
	return f.buildStore
}

// GetValidationFactStore returns the MySQL-backed ValidationFactStore.
func (f *mysqlStorage) GetValidationFactStore() storage.ValidationFactStore {
	return f.validationFactStore
}
