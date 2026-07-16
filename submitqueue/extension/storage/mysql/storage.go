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

	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// mysqlErrDuplicateEntry is MySQL error code 1062 ("Duplicate entry"), returned on a unique or primary key violation.
// It requires a unique index on the table to be raised.
const mysqlErrDuplicateEntry = 1062

type mysqlStorage struct {
	db                        *sql.DB
	requestStore              storage.RequestStore
	changeStore               storage.ChangeStore
	batchStore                storage.BatchStore
	batchDependentStore       storage.BatchDependentStore
	buildStore                storage.BuildStore
	speculationPathBuildStore storage.SpeculationPathBuildStore
	speculationTreeStore      storage.SpeculationTreeStore
	requestLogStore           storage.RequestLogStore
	requestSummaryStore       storage.RequestSummaryStore
	requestQueueStore         storage.RequestQueueSummaryStore
	requestURIStore           storage.RequestURIStore
}

// NewStorage creates a new MySQL storage.
func NewStorage(db *sql.DB, scope tally.Scope) (storage.Storage, error) {
	return &mysqlStorage{
		db:                        db,
		requestStore:              NewRequestStore(db, scope.SubScope("request_store")),
		changeStore:               NewChangeStore(db, scope.SubScope("change_store")),
		batchStore:                NewBatchStore(db, scope.SubScope("batch_store")),
		batchDependentStore:       NewBatchDependentStore(db, scope.SubScope("batch_dependent_store")),
		buildStore:                NewBuildStore(db, scope.SubScope("build_store")),
		speculationPathBuildStore: NewSpeculationPathBuildStore(db, scope.SubScope("speculation_path_build_store")),
		speculationTreeStore:      NewSpeculationTreeStore(db, scope.SubScope("speculation_tree_store")),
		requestLogStore:           NewRequestLogStore(db, scope.SubScope("request_log_store")),
		requestSummaryStore:       NewRequestSummaryStore(db, scope.SubScope("request_summary_store")),
		requestQueueStore:         NewRequestQueueSummaryStore(db, scope.SubScope("request_queue_summary_store")),
		requestURIStore:           NewRequestURIStore(db, scope.SubScope("request_uri_store")),
	}, nil
}

// GetRequestStore returns the MySQL-backed RequestStore.
func (f *mysqlStorage) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// GetChangeStore returns the MySQL-backed ChangeStore.
func (f *mysqlStorage) GetChangeStore() storage.ChangeStore {
	return f.changeStore
}

// GetBatchStore returns the MySQL-backed BatchStore.
func (f *mysqlStorage) GetBatchStore() storage.BatchStore {
	return f.batchStore
}

// GetBatchDependentStore returns the MySQL-backed BatchDependentStore.
func (f *mysqlStorage) GetBatchDependentStore() storage.BatchDependentStore {
	return f.batchDependentStore
}

// GetBuildStore returns the MySQL-backed BuildStore.
func (f *mysqlStorage) GetBuildStore() storage.BuildStore {
	return f.buildStore
}

// GetSpeculationPathBuildStore returns the MySQL-backed SpeculationPathBuildStore.
func (f *mysqlStorage) GetSpeculationPathBuildStore() storage.SpeculationPathBuildStore {
	return f.speculationPathBuildStore
}

// GetSpeculationTreeStore returns the MySQL-backed SpeculationTreeStore.
func (f *mysqlStorage) GetSpeculationTreeStore() storage.SpeculationTreeStore {
	return f.speculationTreeStore
}

// GetRequestLogStore returns the MySQL-backed RequestLogStore.
func (f *mysqlStorage) GetRequestLogStore() storage.RequestLogStore {
	return f.requestLogStore
}

// GetRequestSummaryStore returns the MySQL-backed RequestSummaryStore.
func (f *mysqlStorage) GetRequestSummaryStore() storage.RequestSummaryStore {
	return f.requestSummaryStore
}

// GetRequestQueueSummaryStore returns the MySQL-backed RequestQueueSummaryStore.
func (f *mysqlStorage) GetRequestQueueSummaryStore() storage.RequestQueueSummaryStore {
	return f.requestQueueStore
}

// GetRequestURIStore returns the MySQL-backed RequestURIStore.
func (f *mysqlStorage) GetRequestURIStore() storage.RequestURIStore {
	return f.requestURIStore
}

// Close closes the underlying database connection.
func (f *mysqlStorage) Close() error {
	return f.db.Close()
}
