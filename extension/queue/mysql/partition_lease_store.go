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
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"go.uber.org/zap"
)

// sqlpartitionLeaseStore is the SQL implementation of partitionLeaseStore
type sqlpartitionLeaseStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	scope   tally.Scope
}

// newPartitionLeaseStore creates a new SQL partition lease store
func newPartitionLeaseStore(db *sql.DB, logger *zap.Logger, scope tally.Scope) partitionLeaseStore {
	return &sqlpartitionLeaseStore{
		db:      db,
		logger:  logger.Sugar().Named("queue_mysql_partition_lease_store"),
		scope:   scope.SubScope("queue_mysql_partition_lease_store"),
	}
}

// TryAcquireLease attempts to acquire or renew a lease for a partition
func (s *sqlpartitionLeaseStore) TryAcquireLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (_ bool, retErr error) {
	op := metrics.Begin(s.scope, "try_acquire_lease")
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
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
		return false, fmt.Errorf("failed to acquire lease for topic %s partition %s: %w", topic, partitionKey, err)
	}

	// Check if we own the lease
	var owner string
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT leased_by FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ?
	`, PartitionLeasesTableName), consumerGroup, topic, partitionKey).Scan(&owner)

	if err != nil {
		return false, fmt.Errorf("failed to check lease ownership for topic %s partition %s: %w", topic, partitionKey, err)
	}

	acquired := owner == subscriberName
	if acquired {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "acquired", 1)
		s.logger.Debugw("acquired lease",
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
	} else {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "not_acquired", 1)
	}

	return acquired, nil
}

// RenewLease renews the lease for a partition owned by this worker
func (s *sqlpartitionLeaseStore) RenewLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "renew_lease")
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET lease_renewed_at = ?
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, PartitionLeasesTableName), now, consumerGroup, topic, partitionKey, subscriberName)

	if err != nil {
		return fmt.Errorf("failed to renew lease for topic %s partition %s: %w", topic, partitionKey, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check renewal result for topic %s partition %s: %w", topic, partitionKey, err)
	}

	if rows == 0 {
		s.logger.Warnw("lease not owned by this worker or already expired",
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
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
	op := metrics.Begin(s.scope, "release_lease")
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, PartitionLeasesTableName), consumerGroup, topic, partitionKey, subscriberName)

	if err != nil {
		return fmt.Errorf("failed to release lease for topic %s partition %s: %w", topic, partitionKey, err)
	}

	// Only log if we actually deleted a row (idempotent)
	rows, _ := result.RowsAffected()
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
	op := metrics.Begin(s.scope, "get_leased_partitions")
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT partition_key FROM %s
		WHERE consumer_group = ? AND topic = ? AND leased_by = ?
	`, PartitionLeasesTableName), consumerGroup, topic, subscriberName)

	if err != nil {
		return nil, fmt.Errorf("failed to get leased partitions for topic %s: %w", topic, err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err != nil {
			return nil, fmt.Errorf("failed to scan partition for topic %s: %w", topic, err)
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error for leased partitions in topic %s: %w", topic, err)
	}

	metrics.NamedCounter(s.scope, "get_leased_partitions", "partitions_leased", int64(len(partitions)))
	s.logger.Debugw("retrieved leased partitions",
		logTopic, topic,
		"count", len(partitions),
	)

	return partitions, nil
}

// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases
// Returns the number of new leases acquired
// maxPartitions limits how many total partitions this subscriber can own (0 = unlimited)
func (s *sqlpartitionLeaseStore) DiscoverAndAcquirePartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string, leaseDurationMs int64, maxPartitions int) (_ int, retErr error) {
	op := metrics.Begin(s.scope, "discover_and_acquire")
	defer func() { op.Complete(retErr) }()

	// Query distinct partition_keys from messages table.
	// The maxPartitions cap limits how many leases this subscriber acquires,
	// so we discover all partitions to ensure none are silently dropped.
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT partition_key FROM %s WHERE topic = ? ORDER BY partition_key
	`, MessagesTableName), topic)
	if err != nil {
		return 0, fmt.Errorf("failed to discover partitions for topic %s: %w", topic, err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionKey string
		if err := rows.Scan(&partitionKey); err != nil {
			return 0, fmt.Errorf("failed to scan partition key for topic %s: %w", topic, err)
		}
		partitions = append(partitions, partitionKey)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error for partition discovery in topic %s: %w", topic, err)
	}

	s.logger.Debugw("discovered partitions",
		logTopic, topic,
		"count", len(partitions),
	)

	// Query owned partitions once before the loop to avoid N+1 queries
	ownedCount := 0
	if maxPartitions > 0 {
		owned, err := s.GetLeasedPartitions(ctx, topic, subscriberName, consumerGroup)
		if err != nil {
			return 0, fmt.Errorf("failed to get owned partitions for cap check: %w", err)
		}
		ownedCount = len(owned)
	}

	// Try to acquire leases for discovered partitions
	acquiredCount := 0
	for _, partitionKey := range partitions {
		// Enforce maxPartitions cap using local count
		if maxPartitions > 0 && ownedCount >= maxPartitions {
			s.logger.Infow("reached max partitions cap, stopping acquisition",
				logTopic, topic,
				"max_partitions", maxPartitions,
				"owned_count", ownedCount,
			)
			break
		}

		acquired, err := s.TryAcquireLease(ctx, topic, partitionKey, subscriberName, consumerGroup, leaseDurationMs)
		if err != nil {
			// Log but continue trying other partitions
			s.logger.Warnw("failed to acquire lease for partition",
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

	metrics.NamedCounter(s.scope, "discover_and_acquire", "partitions_discovered", int64(len(partitions)))
	metrics.NamedCounter(s.scope, "discover_and_acquire", "partitions_acquired", int64(acquiredCount))
	s.logger.Infow("completed partition discovery and acquisition",
		logTopic, topic,
		"discovered_count", len(partitions),
		"acquired_count", acquiredCount,
	)

	return acquiredCount, nil
}

// DiscoverPartitions returns all distinct partition keys for a topic from the messages table.
func (s *sqlpartitionLeaseStore) DiscoverPartitions(ctx context.Context, topic string) (_ []string, retErr error) {
	op := metrics.Begin(s.scope, "discover_partitions")
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT partition_key FROM %s WHERE topic = ? ORDER BY partition_key
	`, MessagesTableName), topic)
	if err != nil {
		return nil, fmt.Errorf("failed to discover partitions for topic %s: %w", topic, err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionKey string
		if err := rows.Scan(&partitionKey); err != nil {
			return nil, fmt.Errorf("failed to scan partition key for topic %s: %w", topic, err)
		}
		partitions = append(partitions, partitionKey)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error for partition discovery in topic %s: %w", topic, err)
	}

	return partitions, nil
}
