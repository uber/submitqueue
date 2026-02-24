package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/extension/storage"
)

type mysqlStorage struct {
	db                  *sql.DB
	requestStore        storage.RequestStore
	changeProviderStore storage.ChangeProviderStore
	batchStore          storage.BatchStore
}

// NewStorage creates a new MySQL storage.
func NewStorage(db *sql.DB) (storage.Storage, error) {
	return &mysqlStorage{
		db:                  db,
		requestStore:        NewRequestStore(db),
		changeProviderStore: NewChangeProviderStore(db),
		batchStore:          NewBatchStore(db),
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

// Close closes the underlying database connection.
func (f *mysqlStorage) Close() error {
	return f.db.Close()
}
