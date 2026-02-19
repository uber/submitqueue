package mysql

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/extensions/storage"
)

// MySQLParameters defines the configuration for the MySQL storage factory.
// TODO: integrate with configuration system.
type MySQLParameters struct {
	// DSN is the MySQL Data Source Name.
	// Format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	DSN string

	// MaxOpenConns sets the maximum number of open connections to the database. 0 means unlimited.
	MaxOpenConns int

	// MaxIdleConns sets the maximum number of idle connections in the pool. 0 means no idle connections are retained.
	MaxIdleConns int

	// ConnMaxLifetime sets the maximum amount of time a connection may be reused. 0 means connections are not closed due to age.
	ConnMaxLifetime time.Duration
}

type factory struct {
	db           *sql.DB
	requestStore storage.RequestStore
}

// NewFactory creates a new MySQL storage factory.
func NewFactory(p MySQLParameters) (storage.StoreFactory, error) {
	db, err := sql.Open("mysql", p.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	if p.MaxOpenConns > 0 {
		db.SetMaxOpenConns(p.MaxOpenConns)
	}
	if p.MaxIdleConns > 0 {
		db.SetMaxIdleConns(p.MaxIdleConns)
	}
	if p.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(p.ConnMaxLifetime)
	}

	return &factory{
		db:           db,
		requestStore: NewRequestStore(db),
	}, nil
}

// GetRequestStore returns the MySQL-backed RequestStore.
func (f *factory) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

// Close closes the underlying database connection.
func (f *factory) Close() error {
	return f.db.Close()
}
