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

package testutil

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
)

// StageHold is a test helper that starves a single queue consumer controller by
// planting a phantom partition lease in the MySQL queue's partition_leases table.
//
// Mechanism: the MySQL queue implementation (platform/extension/messagequeue/mysql)
// schedules delivery via partition leases keyed by (consumer_group, topic,
// partition_key). TryAcquireLease steals a lease only when lease_renewed_at is
// older than now minus the lease duration. By inserting a row owned by a phantom
// subscriber ("e2e-hold") with lease_renewed_at set far in the future, the
// partition is held indefinitely — the real service subscriber's discovery loop
// finds the lease unexpired, yields acquired=false, and skips the partition.
// Messages published to the held partition accumulate but are never delivered.
// Deleting the phantom row lets the service's next discovery tick re-acquire the
// lease and resume consumption.
//
// IMPORTANT LIMITATION: this is a PRE-hold primitive — it must be planted before
// the partition's first message is published (or at least before the service's
// subscriber discovers and acquires the partition). You cannot deterministically
// steal an actively renewed lease from a running subscriber because TryAcquireLease
// only steals when the existing renewal timestamp is stale. NewStageHold enforces
// this: it fails loudly if a lease row for the partition already exists.
//
// This helper is explicitly MySQL-queue-specific. It relies on the
// queue_partition_leases schema and the lease-expiry semantics documented in
// partition_lease_store.go. It uses the exported PartitionLeasesTableName constant
// from the mysql queue package.
//
// StageHold is shared by pointer (an exception to the repo's value-type
// preference) because release ownership is shared: the t.Cleanup registered by
// NewStageHold and the caller's explicit Release() must observe the same
// released state.
type StageHold struct {
	// db is the queue database connection.
	db *sql.DB
	// consumerGroup identifies the consumer group whose partition is held.
	consumerGroup string
	// topic is the queue topic name.
	topic string
	// partitionKey is the partition being starved.
	partitionKey string
	// log receives structured diagnostics.
	log *TestLogger
	// mu guards released.
	mu sync.Mutex
	// released is set only after a successful DELETE, so a failed release
	// attempt (e.g. a transient DB error) stays retryable by a later call —
	// including the t.Cleanup-registered one.
	released bool
}

// phantomSubscriber is the leased_by value for phantom leases. It must not
// collide with any real subscriber name.
const phantomSubscriber = "e2e-hold"

// holdDuration is how far into the future the phantom lease's renewal timestamp
// is set. Several hours ensures no real subscriber can steal it during a test.
const holdDuration = 4 * time.Hour

// NewStageHold plants a phantom partition lease and returns a handle. The lease
// is removed automatically via the provided cleanup registrar (typically
// t.Cleanup) if Release is not called explicitly.
//
// The plant is a plain INSERT: if a lease row for (consumerGroup, topic,
// partitionKey) already exists — whether owned by a real subscriber or another
// hold — planting fails with an error instead of silently stealing the lease.
// An existing row means the hold was planted too late (the service already
// acquired the partition, violating the pre-hold contract) or collides with
// another test's hold on the same partition.
func NewStageHold(log *TestLogger, db *sql.DB, consumerGroup, topic, partitionKey string, cleanup func(func())) (*StageHold, error) {
	now := time.Now().UnixMilli()
	futureRenewal := now + holdDuration.Milliseconds()

	_, err := db.Exec(fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, leased_by, leased_at, lease_renewed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, queueMySQL.PartitionLeasesTableName),
		consumerGroup, topic, partitionKey, phantomSubscriber, now, futureRenewal)
	if err != nil {
		return nil, fmt.Errorf(
			"plant stage hold (group=%s topic=%s partition=%s): partition may already be leased — "+
				"a pre-hold must be planted before the service acquires the partition, and no two holds "+
				"may target the same partition: %w",
			consumerGroup, topic, partitionKey, err)
	}

	h := &StageHold{
		db:            db,
		consumerGroup: consumerGroup,
		topic:         topic,
		partitionKey:  partitionKey,
		log:           log,
	}

	log.Logf("StageHold planted: group=%s topic=%s partition=%s (phantom renewal %s ahead)",
		consumerGroup, topic, partitionKey, holdDuration)

	cleanup(func() { h.Release() })

	return h, nil
}

// Release removes the phantom lease, allowing the real subscriber to re-acquire
// the partition on its next discovery tick. Release is idempotent after a
// successful delete; a failed delete leaves the hold releasable so a later call
// (including the cleanup-registered one) can retry.
func (h *StageHold) Release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return
	}

	_, err := h.db.Exec(fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, queueMySQL.PartitionLeasesTableName),
		h.consumerGroup, h.topic, h.partitionKey, phantomSubscriber)
	if err != nil {
		h.log.Logf("StageHold release failed (group=%s topic=%s partition=%s), will retry on next Release: %v",
			h.consumerGroup, h.topic, h.partitionKey, err)
		return
	}

	h.released = true
	h.log.Logf("StageHold released: group=%s topic=%s partition=%s",
		h.consumerGroup, h.topic, h.partitionKey)
}
