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

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type mysqlStorage struct {
	db              *sql.DB
	requestStore    storage.RequestStore
	requestURIStore storage.RequestURIStore
	queueStore      storage.QueueStore
	buildStore      storage.BuildStore
}

// NewStorage creates a new MySQL-backed storage.
func NewStorage(db *sql.DB, scope tally.Scope) (storage.Storage, error) {
	return &mysqlStorage{
		db:              db,
		requestStore:    NewRequestStore(db, scope.SubScope("request_store")),
		requestURIStore: NewRequestURIStore(db, scope.SubScope("request_uri_store")),
		queueStore:      NewQueueStore(db, scope.SubScope("queue_store")),
		buildStore:      NewBuildStore(db, scope.SubScope("build_store")),
	}, nil
}

// GetRequestStore returns the MySQL-backed RequestStore.
func (f *mysqlStorage) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// GetRequestURIStore returns the MySQL-backed RequestURIStore.
func (f *mysqlStorage) GetRequestURIStore() storage.RequestURIStore {
	return f.requestURIStore
}

// GetQueueStore returns the MySQL-backed QueueStore.
func (f *mysqlStorage) GetQueueStore() storage.QueueStore {
	return f.queueStore
}

// GetBuildStore returns the MySQL-backed BuildStore.
func (f *mysqlStorage) GetBuildStore() storage.BuildStore {
	return f.buildStore
}

// Close closes the underlying database connection.
func (f *mysqlStorage) Close() error {
	return f.db.Close()
}
