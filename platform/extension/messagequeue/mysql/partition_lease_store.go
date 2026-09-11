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
func (s *sqlpartitionLeaseStore) TryAcquireLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (_ bool, retErr error) {
	op := metrics.Begin(s.scope, "try_acquire_lease", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	now := currentTimeMillis()
	staleThreshold := now - leaseDurationMs

	// Try to insert or update stale lease
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, partition_key, leased_by, leased_at, lease_renewed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			leased_by = IF(lease_renewed_at < ?, VALUES(leased_by), leased_by),
			leased_at = IF(lease_renewed_at < ?, VALUES(leased_at), leased_at),
			lease_renewed_at = IF(lease_renewed_at < ?, VALUES(lease_renewed_at), lease_renewed_at)
	`, PartitionLeasesTableName),
		tenant, consumerGroup, topic, partitionKey, subscriberName, now, now,
		staleThreshold, staleThreshold, staleThreshold)

	if err != nil {
		return false, fmt.Errorf("acquire lease tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	// Check if we own the lease
	var owner string
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT leased_by FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?
	`, PartitionLeasesTableName), tenant, consumerGroup, topic, partitionKey).Scan(&owner)

	if err != nil {
		return false, fmt.Errorf("check lease ownership tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	acquired := owner == subscriberName
	if acquired {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "acquired", 1, metrics.NewTag("topic", topic))
		s.logger.Debugw("acquired lease",
			logTenant, tenant,
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
	} else {
		metrics.NamedCounter(s.scope, "try_acquire_lease", "not_acquired", 1, metrics.NewTag("topic", topic))
	}

	return acquired, nil
}

// ReleaseLease releases the lease for a partition owned by this worker
func (s *sqlpartitionLeaseStore) ReleaseLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "release_lease", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ? AND leased_by = ?
	`, PartitionLeasesTableName), tenant, consumerGroup, topic, partitionKey, subscriberName)

	if err != nil {
		return fmt.Errorf("release lease tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	// RowsAffected error is swallowed because the DELETE query itself succeeded.
	// This is a driver-level diagnostic failure — the lease is already released.
	// We log for visibility but the release operation is complete.
	rows, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnw("failed to get rows affected after release lease",
			logTenant, tenant,
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
	}
	if rows > 0 {
		s.logger.Debugw("released lease",
			logTenant, tenant,
			logTopic, topic,
			logPartitionKey, partitionKey,
		)
	}

	return nil
}

func (s *sqlpartitionLeaseStore) GetLeasedPartitionsForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (_ map[string][]string, retErr error) {
	op := metrics.Begin(s.scope, "get_leased_partitions_for_tenants", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return map[string][]string{}, nil
	}
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, subscriberName)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT tenant, partition_key FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND leased_by = ?
	`, PartitionLeasesTableName, placeholders), args...)
	if err != nil {
		return nil, fmt.Errorf("get leased partitions topic=%s: %w", topic, err)
	}
	defer rows.Close()

	byTenant := make(map[string][]string)
	for rows.Next() {
		var tenant, partition string
		if err := rows.Scan(&tenant, &partition); err != nil {
			return nil, fmt.Errorf("scan leased partition topic=%s: %w", topic, err)
		}
		byTenant[tenant] = append(byTenant[tenant], partition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}
	return byTenant, nil
}

func (s *sqlpartitionLeaseStore) GetAllLeasesForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string) (_ map[string][]leaseInfo, retErr error) {
	op := metrics.Begin(s.scope, "get_all_leases_for_tenants", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return map[string][]leaseInfo{}, nil
	}
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT tenant, partition_key, leased_by, lease_renewed_at FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ?
	`, PartitionLeasesTableName, placeholders), args...)
	if err != nil {
		return nil, fmt.Errorf("get all leases topic=%s: %w", topic, err)
	}
	defer rows.Close()

	byTenant := make(map[string][]leaseInfo)
	for rows.Next() {
		var tenant string
		var lease leaseInfo
		if err := rows.Scan(&tenant, &lease.PartitionKey, &lease.LeasedBy, &lease.LeaseRenewedAt); err != nil {
			return nil, fmt.Errorf("scan lease topic=%s: %w", topic, err)
		}
		byTenant[tenant] = append(byTenant[tenant], lease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}
	return byTenant, nil
}

func (s *sqlpartitionLeaseStore) RenewOwnedLeases(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "renew_owned_leases", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return nil
	}
	now := currentTimeMillis()
	args := []any{now}
	args = appendStrings(args, tenants)
	args = append(args, consumerGroup, topic, subscriberName)

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET lease_renewed_at = ?
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND leased_by = ?
	`, PartitionLeasesTableName, placeholders), args...)
	if err != nil {
		return fmt.Errorf("renew owned leases topic=%s: %w", topic, err)
	}
	return nil
}

func (s *sqlpartitionLeaseStore) ReleaseOwnedLeases(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "release_owned_leases", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return nil
	}
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, subscriberName)

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND leased_by = ?
	`, PartitionLeasesTableName, placeholders), args...)
	if err != nil {
		return fmt.Errorf("release owned leases topic=%s: %w", topic, err)
	}
	return nil
}

func (s *sqlpartitionLeaseStore) PurgeStaleForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, olderThanMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "purge_stale_for_tenants", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return nil
	}
	threshold := currentTimeMillis() - olderThanMs
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, threshold)

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND lease_renewed_at < ?
	`, PartitionLeasesTableName, placeholders), args...)
	if err != nil {
		return fmt.Errorf("failed to purge stale leases topic=%s: %w", topic, err)
	}
	if deleted, err := result.RowsAffected(); err == nil && deleted > 0 {
		metrics.NamedCounter(s.scope, "purge_stale_for_tenants", "rows_deleted", deleted, metrics.NewTag("topic", topic))
	}
	return nil
}

func (s *sqlpartitionLeaseStore) DiscoverPartitions(ctx context.Context, tenants []string, topic string) (_ map[string][]string, retErr error) {
	op := metrics.Begin(s.scope, "discover_partitions", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return map[string][]string{}, nil
	}
	args := appendStrings(nil, tenants)
	args = append(args, topic)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT tenant, partition_key FROM %s
		WHERE tenant IN (%s) AND topic = ?
		ORDER BY tenant, partition_key
	`, MessagesTableName, placeholders), args...)
	if err != nil {
		return nil, fmt.Errorf("discover partitions topic=%s: %w", topic, err)
	}
	defer rows.Close()

	byTenant := make(map[string][]string)
	for rows.Next() {
		var tenant, partitionKey string
		if err := rows.Scan(&tenant, &partitionKey); err != nil {
			return nil, fmt.Errorf("scan partition key topic=%s: %w", topic, err)
		}
		byTenant[tenant] = append(byTenant[tenant], partitionKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s: %w", topic, err)
	}
	return byTenant, nil
}

// currentTimeMillis returns the current time in milliseconds since epoch.
func currentTimeMillis() int64 {
	return time.Now().UnixMilli()
}
