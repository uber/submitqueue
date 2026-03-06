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
	"context"
	"database/sql"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/extension/counter"
)

type mysqlCounter struct {
	db    *sql.DB
	scope tally.Scope
}

// NewCounter creates a new MySQL-backed Counter.
func NewCounter(db *sql.DB, scope tally.Scope) counter.Counter {
	return &mysqlCounter{db: db, scope: scope}
}

// Next atomically increments the counter for the given domain and returns the new value.
// Uses MySQL's LAST_INSERT_ID() to set the value atomically and read the incremented value.
func (c *mysqlCounter) Next(ctx context.Context, domain string) (ret int64, retErr error) {
	op := metrics.Begin(c.scope, "next")
	defer func() { op.Complete(retErr) }()
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
