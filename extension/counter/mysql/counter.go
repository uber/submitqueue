package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uber/submitqueue/extension/counter"
)

type mysqlCounter struct {
	db *sql.DB
}

// NewCounter creates a new MySQL-backed Counter.
func NewCounter(db *sql.DB) counter.Counter {
	return &mysqlCounter{db: db}
}

// Next atomically increments the counter for the given domain and returns the new value.
// Uses MySQL's LAST_INSERT_ID() to set the value atomically and read the incremented value.
func (c *mysqlCounter) Next(ctx context.Context, domain string) (int64, error) {
	result, err := c.db.ExecContext(ctx,
		"INSERT INTO counter (domain, value) VALUES (?, LAST_INSERT_ID(1)) ON DUPLICATE KEY UPDATE value = LAST_INSERT_ID(value + 1)",
		domain,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to increment counter for domain=%s: %w", domain, err)
	}

	value, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get counter value for domain=%s: %w", domain, err)
	}

	return value, nil
}
