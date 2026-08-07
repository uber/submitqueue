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
	"sort"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// sqlpartitionLeaseStore is the SQL implementation of partitionLeaseStore
type sqlpartitionLeaseStore struct {
	db     *sql.DB
	logger *zap.SugaredLogger
	scope  tally.Scope
}

// newPartitionLeaseStore creates a new SQL partition lease store
func newPartitionLeaseStore(db *sql.DB, logger *zap.SugaredLogger, scope tally.Scope) partitionLeaseStore {
	return &sqlpartitionLeaseStore{
		db:     db,
		logger: logger.Named("partition_lease_store"),
		scope:  scope.SubScope("partition_lease_store"),
	}
}

// TryAcquireLease attempts to acquire or renew a lease for a partition
func (s *sqlpartitionLeaseStore) TryAcquireLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (_ bool, retErr error) {
	op := metrics.Begin(s.scope, "try_acquire_lease", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	now := currentTimeMillis()
	staleThreshold := now - leaseDurationMs

	// Try to insert or update stale lease
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, leased_by, leased_at, lease_renewed_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			leased_by = IF(lease_renewed_at < ?, VALUES(leased_by), leased_by),
			leased_at = IF(lease_renewed_at < ?, VALUES(leased_at), leased_at),
			lease_renewed_at = IF(lease_renewed_at < ?, VALUES(lease_renewed_at), lease_renewed_at)
	`, PartitionLeasesTableName),
		consumerGroup, topic, partitionKey, subscriberName, now, now,
		staleThreshold, staleThreshold, staleThreshold)

	if err != nil {
		return false, fmt.Errorf("acquire lease topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	// Check if we own the lease
	var owner string
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT leased_by FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ?
	`, PartitionLeasesTableName), consumerGroup, topic, partitionKey).Scan(&owner)

	if err != nil {
		return false, fmt.Errorf("check lease ownership topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	acquired := owner == subscriberName
	if acquired {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "acquired", 1, metrics.NewTag("topic", topic))
		s.logger.Debugw("acquired lease",
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
	} else {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "not_acquired", 1, metrics.NewTag("topic", topic))
	}

	return acquired, nil
}

// RenewLease renews the lease for a partition owned by this worker
func (s *sqlpartitionLeaseStore) RenewLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "renew_lease", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	now := currentTimeMillis()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET lease_renewed_at = ?
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, PartitionLeasesTableName), now, consumerGroup, topic, partitionKey, subscriberName)

	if err != nil {
		return fmt.Errorf("renew lease topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check renewal result topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	if rows == 0 {
		return &ErrLeaseExpired{Topic: topic, PartitionKey: partitionKey}
	}

	s.logger.Debugw("renewed lease",
		logTopic, topic,
		logPartitionKey, partitionKey,
	)

	return nil
}

// ReleaseLease releases the lease for a partition owned by this worker
func (s *sqlpartitionLeaseStore) ReleaseLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "release_lease", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, PartitionLeasesTableName), consumerGroup, topic, partitionKey, subscriberName)

	if err != nil {
		return fmt.Errorf("release lease topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	// RowsAffected error is swallowed because the DELETE query itself succeeded.
	// This is a driver-level diagnostic failure — the lease is already released.
	// We log for visibility but the release operation is complete.
	rows, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnw("failed to get rows affected after release lease",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
	}
	if rows > 0 {
		s.logger.Debugw("released lease",
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
	}

	return nil
}

// GetLeasedPartitions returns all partitions currently leased by this worker
func (s *sqlpartitionLeaseStore) GetLeasedPartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string) (_ []string, retErr error) {
	op := metrics.Begin(s.scope, "get_leased_partitions", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT partition_key FROM %s
		WHERE consumer_group = ? AND topic = ? AND leased_by = ?
	`, PartitionLeasesTableName), consumerGroup, topic, subscriberName)

	if err != nil {
		return nil, fmt.Errorf("get leased partitions topic=%s: %w", topic, err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err != nil {
			return nil, fmt.Errorf("scan partition topic=%s: %w", topic, err)
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}

	s.logger.Debugw("retrieved leased partitions",
		logTopic, topic,
		"count", len(partitions),
	)

	return partitions, nil
}

// GetAllLeases returns the lease row for every partition currently leased
// under (topic, consumerGroup) by any subscriber.
func (s *sqlpartitionLeaseStore) GetAllLeases(ctx context.Context, topic string, consumerGroup string) (_ []leaseInfo, retErr error) {
	op := metrics.Begin(s.scope, "get_all_leases", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT partition_key, leased_by, lease_renewed_at FROM %s
		WHERE consumer_group = ? AND topic = ?
	`, PartitionLeasesTableName), consumerGroup, topic)

	if err != nil {
		return nil, fmt.Errorf("get all leases topic=%s: %w", topic, err)
	}
	defer rows.Close()

	var leases []leaseInfo
	for rows.Next() {
		var lease leaseInfo
		if err := rows.Scan(&lease.PartitionKey, &lease.LeasedBy, &lease.LeaseRenewedAt); err != nil {
			return nil, fmt.Errorf("scan lease topic=%s: %w", topic, err)
		}
		leases = append(leases, lease)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}

	return leases, nil
}

// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases.
// Returns the number of new leases acquired and the full list of discovered partitions.
// maxPartitions limits how many total partitions this subscriber can own (0 = unlimited)
//
// Acquisition is lease-aware: one GetAllLeases read classifies every
// discovered partition, and TryAcquireLease is attempted only for partitions
// that are unleased or whose lease is stale (stealable). Partitions already
// owned by this subscriber are counted against the cap but not re-probed
// (renewal is the lease tick's job), and partitions validly held by another
// subscriber are skipped entirely — probing them is a guaranteed-futile
// write on a contended lease row. The classification is advisory (a lease
// can expire or renew between the read and the attempt); TryAcquireLease
// remains the atomic arbiter.
func (s *sqlpartitionLeaseStore) DiscoverAndAcquirePartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string, leaseDurationMs int64, maxPartitions int) (_ int, _ []string, retErr error) {
	op := metrics.Begin(s.scope, "discover_and_acquire", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	// Query distinct partition_keys from messages table.
	// No LIMIT is applied because all partitions must be discoverable for fair
	// share computation to be accurate — a LIMIT would silently exclude partitions,
	// making them permanently unprocessable. The maxPartitions cap only limits how
	// many leases this subscriber acquires, not how many partitions are visible.
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT partition_key FROM %s WHERE topic = ? ORDER BY partition_key
	`, MessagesTableName), topic)
	if err != nil {
		return 0, nil, fmt.Errorf("discover partitions topic=%s: %w", topic, err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionKey string
		if err := rows.Scan(&partitionKey); err != nil {
			return 0, nil, fmt.Errorf("scan partition key topic=%s: %w", topic, err)
		}
		partitions = append(partitions, partitionKey)
	}

	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}

	s.logger.Debugw("discovered partitions",
		logTopic, topic,
		"count", len(partitions),
	)

	// One read of every lease row classifies the discovered partitions:
	// self-owned (count toward the cap, no re-probe), validly held by
	// another subscriber (skip), or unleased/stale (acquisition candidates).
	allLeases, err := s.GetAllLeases(ctx, topic, consumerGroup)
	if err != nil {
		return 0, nil, fmt.Errorf("get all leases for acquisition topic=%s: %w", topic, err)
	}
	staleThreshold := currentTimeMillis() - leaseDurationMs
	ownedCount := 0
	ownedSet := make(map[string]struct{})
	heldByOther := make(map[string]struct{})
	for _, lease := range allLeases {
		switch {
		case lease.LeasedBy == subscriberName:
			// Self-owned, fresh or stale: a stale self-lease means our own
			// renewals are lagging, not that ownership moved.
			ownedSet[lease.PartitionKey] = struct{}{}
			ownedCount++
		case lease.LeaseRenewedAt >= staleThreshold:
			heldByOther[lease.PartitionKey] = struct{}{}
		}
	}

	// Sort partitions deterministically
	sort.Strings(partitions)

	// Try to acquire leases for unleased or stale discovered partitions
	acquiredCount := 0
	skippedCount := 0
	for _, partitionKey := range partitions {
		if _, owned := ownedSet[partitionKey]; owned {
			continue
		}
		if _, held := heldByOther[partitionKey]; held {
			skippedCount++
			continue
		}

		// Enforce maxPartitions cap using local count
		if maxPartitions > 0 && ownedCount >= maxPartitions {
			s.logger.Debugw("reached max partitions cap, stopping acquisition",
				logTopic, topic,
				"max_partitions", maxPartitions,
				"owned_count", ownedCount,
			)
			break
		}

		acquired, err := s.TryAcquireLease(ctx, topic, partitionKey, subscriberName, consumerGroup, leaseDurationMs)
		if err != nil {
			// Per-partition error is swallowed because one partition's DB failure
			// should not prevent acquiring leases for other partitions. The failed
			// partition is retried on the next discovery cycle.
			s.logger.Errorw("failed to acquire lease for partition",
				logTopic, topic,
				logPartitionKey, partitionKey,
				logError, err,
			)
			continue
		}
		if acquired {
			acquiredCount++
			ownedCount++
		}
	}

	metrics.NamedCounter(s.scope, "discover_and_acquire", "partitions_discovered", int64(len(partitions)), metrics.NewTag("topic", topic))
	metrics.NamedCounter(s.scope, "discover_and_acquire", "partitions_acquired", int64(acquiredCount), metrics.NewTag("topic", topic))
	metrics.NamedCounter(s.scope, "discover_and_acquire", "lease_aware_skipped", int64(skippedCount), metrics.NewTag("topic", topic))
	s.logger.Debugw("completed partition discovery and acquisition",
		logTopic, topic,
		"discovered_count", len(partitions),
		"acquired_count", acquiredCount,
		"skipped_held_by_other", skippedCount,
	)

	return acquiredCount, partitions, nil
}

// currentTimeMillis returns the current time in milliseconds since epoch.
func currentTimeMillis() int64 {
	return time.Now().UnixMilli()
}
