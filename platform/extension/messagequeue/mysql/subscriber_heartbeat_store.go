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
	"strings"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
)

// sqlSubscriberHeartbeatStore is the SQL implementation of subscriberHeartbeatStore
type sqlSubscriberHeartbeatStore struct {
	db      *sql.DB
	scope   tally.Scope
	nowFunc func() time.Time
}

// newSubscriberHeartbeatStore creates a new SQL subscriber heartbeat store
func newSubscriberHeartbeatStore(db *sql.DB, scope tally.Scope, nowFunc func() time.Time) subscriberHeartbeatStore {
	return &sqlSubscriberHeartbeatStore{
		db:      db,
		scope:   scope.SubScope("subscriber_heartbeat_store"),
		nowFunc: nowFunc,
	}
}

func (s *sqlSubscriberHeartbeatStore) HeartbeatForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "heartbeat_for_tenants", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if len(tenants) == 0 {
		return nil
	}
	now := s.nowFunc().UnixMilli()
	valueParts := make([]string, len(tenants))
	args := make([]any, 0, len(tenants)*5)
	for i, tenant := range tenants {
		valueParts[i] = "(?, ?, ?, ?, ?, 0)"
		args = append(args, tenant, consumerGroup, topic, subscriberName, now)
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (tenant, consumer_group, topic, subscriber_name, heartbeat_at, deregistered_at)
		VALUES %s
		ON DUPLICATE KEY UPDATE heartbeat_at = VALUES(heartbeat_at), deregistered_at = 0
	`, SubscriberHeartbeatsTableName, strings.Join(valueParts, ", ")), args...)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat topic=%s: %w", topic, err)
	}
	return nil
}

func (s *sqlSubscriberHeartbeatStore) ActiveSubscribersForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, staleDurationMs int64) (_ map[string][]string, retErr error) {
	op := metrics.Begin(s.scope, "active_subscribers_for_tenants", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return map[string][]string{}, nil
	}
	staleThreshold := s.nowFunc().UnixMilli() - staleDurationMs
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, staleThreshold)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT tenant, subscriber_name FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND heartbeat_at >= ? AND deregistered_at = 0
	`, SubscriberHeartbeatsTableName, placeholders), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query active subscribers topic=%s: %w", topic, err)
	}
	defer rows.Close()

	byTenant := make(map[string][]string)
	for rows.Next() {
		var tenant, name string
		if err := rows.Scan(&tenant, &name); err != nil {
			return nil, fmt.Errorf("failed to scan subscriber name topic=%s: %w", topic, err)
		}
		byTenant[tenant] = append(byTenant[tenant], name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error topic=%s: %w", topic, err)
	}
	return byTenant, nil
}

func (s *sqlSubscriberHeartbeatStore) DeregisterForTenants(ctx context.Context, tenants []string, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "deregister_for_tenants", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return nil
	}
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, subscriberName)

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND subscriber_name = ?
	`, SubscriberHeartbeatsTableName, placeholders), args...)
	if err != nil {
		return fmt.Errorf("failed to deregister subscriber topic=%s: %w", topic, err)
	}
	return nil
}

func (s *sqlSubscriberHeartbeatStore) PurgeStaleForTenants(ctx context.Context, tenants []string, topic string, consumerGroup string, olderThanMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "purge_stale_for_tenants", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	placeholders, ok := inListPlaceholders(len(tenants))
	if !ok {
		return nil
	}
	threshold := s.nowFunc().UnixMilli() - olderThanMs
	args := appendStrings(nil, tenants)
	args = append(args, consumerGroup, topic, threshold)

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE tenant IN (%s) AND consumer_group = ? AND topic = ? AND heartbeat_at < ?
	`, SubscriberHeartbeatsTableName, placeholders), args...)
	if err != nil {
		return fmt.Errorf("failed to purge stale heartbeats topic=%s: %w", topic, err)
	}
	if deleted, err := result.RowsAffected(); err == nil && deleted > 0 {
		metrics.NamedCounter(s.scope, "purge_stale_for_tenants", "rows_deleted", deleted, metrics.NewTag("topic", topic))
	}
	return nil
}
