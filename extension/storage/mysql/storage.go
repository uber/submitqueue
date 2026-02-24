package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/extension/storage"
)

type mysqlStorage struct {
	db                    *sql.DB
	requestStore          storage.RequestStore
	changeProviderStore   storage.ChangeProviderStore
	batchStore            storage.BatchStore
	batchDependentStore   storage.BatchDependentStore
	buildStore            storage.BuildStore
}

// NewStorage creates a new MySQL storage.
func NewStorage(db *sql.DB) (storage.Storage, error) {
	return &mysqlStorage{
		db:                    db,
		requestStore:          NewRequestStore(db),
		changeProviderStore:   NewChangeProviderStore(db),
		batchStore:            NewBatchStore(db),
		batchDependentStore:   NewBatchDependentStore(db),
		buildStore:            NewBuildStore(db),
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

// Close closes the underlying database connection.
func (f *mysqlStorage) Close() error {
	return f.db.Close()
}
