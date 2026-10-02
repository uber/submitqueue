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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
)

type mysqlCounter struct {
	db          *sql.DB
	scope       tally.Scope
	ownerDomain string
	// queue is the queue name this counter instance is bound to; every sequence
	// it advances is scoped to it.
	queue string
}

// NewCounter creates a new MySQL-backed Counter bound to ownerDomain and queue.
func NewCounter(db *sql.DB, scope tally.Scope, ownerDomain, queue string) counter.Counter {
	return &mysqlCounter{db: db, scope: scope, ownerDomain: ownerDomain, queue: queue}
}

// Next atomically increments the counter for the given resource kind within the bound scope
// and returns the new value.
// Uses MySQL's LAST_INSERT_ID() to set the value atomically and read the incremented value.
func (c *mysqlCounter) Next(ctx context.Context, resourceKind string) (ret int64, retErr error) {
	op := metrics.Begin(c.scope, "next", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	result, err := c.db.ExecContext(ctx,
		"INSERT INTO counter (owner_domain, queue, resource_kind, value) VALUES (?, ?, ?, LAST_INSERT_ID(1)) ON DUPLICATE KEY UPDATE value = LAST_INSERT_ID(value + 1)",
		c.ownerDomain, c.queue, resourceKind,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to increment counter for owner_domain=%s queue=%s resource_kind=%s: %w", c.ownerDomain, c.queue, resourceKind, err)
	}

	value, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get counter value for owner_domain=%s queue=%s resource_kind=%s: %w", c.ownerDomain, c.queue, resourceKind, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("counter returned non-positive value for owner_domain=%s queue=%s resource_kind=%s", c.ownerDomain, c.queue, resourceKind)
	}

	return value, nil
}
