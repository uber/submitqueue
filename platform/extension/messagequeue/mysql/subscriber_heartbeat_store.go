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

// sqlSubscriberHeartbeatStore is the SQL implementation of subscriberHeartbeatStore
type sqlSubscriberHeartbeatStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	scope   tally.Scope
	nowFunc func() time.Time
}

// newSubscriberHeartbeatStore creates a new SQL subscriber heartbeat store
func newSubscriberHeartbeatStore(db *sql.DB, logger *zap.SugaredLogger, scope tally.Scope, nowFunc func() time.Time) subscriberHeartbeatStore {
	return &sqlSubscriberHeartbeatStore{
		db:      db,
		logger:  logger.Named("subscriber_heartbeat_store"),
		scope:   scope.SubScope("subscriber_heartbeat_store"),
		nowFunc: nowFunc,
	}
}

// Heartbeat registers or renews a subscriber's heartbeat.
func (s *sqlSubscriberHeartbeatStore) Heartbeat(ctx context.Context, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "heartbeat", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	now := s.nowFunc().UnixMilli()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, subscriber_name, heartbeat_at, deregistered_at)
		VALUES (?, ?, ?, ?, 0)
		ON DUPLICATE KEY UPDATE heartbeat_at = VALUES(heartbeat_at), deregistered_at = 0
	`, SubscriberHeartbeatsTableName), consumerGroup, topic, subscriberName, now)

	if err != nil {
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	return nil
}

// ActiveSubscribers returns the names of subscribers with a heartbeat newer than the stale threshold.
func (s *sqlSubscriberHeartbeatStore) ActiveSubscribers(ctx context.Context, topic string, consumerGroup string, staleDurationMs int64) (_ []string, retErr error) {
	op := metrics.Begin(s.scope, "active_subscribers", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	staleThreshold := s.nowFunc().UnixMilli() - staleDurationMs

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT subscriber_name FROM %s
		WHERE consumer_group = ? AND topic = ? AND heartbeat_at >= ? AND deregistered_at = 0
	`, SubscriberHeartbeatsTableName), consumerGroup, topic, staleThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to query active subscribers: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan subscriber name: %w", err)
		}
		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	s.logger.Debugw("found active subscribers",
		logTopic, topic,
		"count", len(names),
		"subscribers", names,
	)

	return names, nil
}

// Deregister removes a subscriber's heartbeat row (hard delete — see the
// subscriberHeartbeatStore interface doc). Idempotent: no-op if already gone.
func (s *sqlSubscriberHeartbeatStore) Deregister(ctx context.Context, topic string, subscriberName string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "deregister", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_group = ? AND topic = ? AND subscriber_name = ?
	`, SubscriberHeartbeatsTableName), consumerGroup, topic, subscriberName)

	if err != nil {
		return fmt.Errorf("failed to deregister subscriber: %w", err)
	}

	s.logger.Debugw("deregistered subscriber",
		logTopic, topic,
		"subscriber_name", subscriberName,
	)

	return nil
}

// PurgeStale deletes heartbeat rows older than olderThanMs for the topic and
// consumer group. See the subscriberHeartbeatStore interface doc.
func (s *sqlSubscriberHeartbeatStore) PurgeStale(ctx context.Context, topic string, consumerGroup string, olderThanMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "purge_stale", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	threshold := s.nowFunc().UnixMilli() - olderThanMs

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE consumer_group = ? AND topic = ? AND heartbeat_at < ?
	`, SubscriberHeartbeatsTableName), consumerGroup, topic, threshold)

	if err != nil {
		return fmt.Errorf("failed to purge stale heartbeats: %w", err)
	}

	// RowsAffected error is swallowed because the DELETE itself succeeded;
	// the count is for observability only.
	if deleted, err := result.RowsAffected(); err == nil && deleted > 0 {
		metrics.NamedCounter(s.scope, "purge_stale", "rows_deleted", deleted, metrics.NewTag("topic", topic))
		s.logger.Debugw("purged stale heartbeats",
			logTopic, topic,
			"deleted", deleted,
		)
	}

	return nil
}
