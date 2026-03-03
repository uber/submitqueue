package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/extension/storage"
)

type mysqlStorage struct {
	db                    *sql.DB
	requestStore          storage.RequestStore
	changeProviderStore   storage.ChangeProviderStore
	batchStore            storage.BatchStore
	batchDependentStore   storage.BatchDependentStore
	buildStore            storage.BuildStore
	speculationTreeStore  storage.SpeculationTreeStore
}

// NewStorage creates a new MySQL storage.
func NewStorage(db *sql.DB, scope tally.Scope) (storage.Storage, error) {
	return &mysqlStorage{
		db:                    db,
		requestStore:          NewRequestStore(db, scope.SubScope("request_store")),
		changeProviderStore:   NewChangeProviderStore(db, scope.SubScope("change_provider_store")),
		batchStore:            NewBatchStore(db, scope.SubScope("batch_store")),
		batchDependentStore:   NewBatchDependentStore(db, scope.SubScope("batch_dependent_store")),
		buildStore:            NewBuildStore(db, scope.SubScope("build_store")),
		speculationTreeStore:  NewSpeculationTreeStore(db, scope.SubScope("speculation_tree_store")),
	}, nil
}

// GetRequestStore returns the MySQL-backed RequestStore.
func (f *mysqlStorage) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// GetChangeProviderStore returns the MySQL-backed ChangeProviderStore.
func (f *mysqlStorage) GetChangeProviderStore() storage.ChangeProviderStore {
	return f.changeProviderStore
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

// GetSpeculationTreeStore returns the MySQL-backed SpeculationTreeStore.
func (f *mysqlStorage) GetSpeculationTreeStore() storage.SpeculationTreeStore {
	return f.speculationTreeStore
}

// Close closes the underlying database connection.
func (f *mysqlStorage) Close() error {
	return f.db.Close()
}
