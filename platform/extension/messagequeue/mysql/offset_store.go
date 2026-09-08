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
)

// sqloffsetStore is the SQL implementation of offsetStore
type sqloffsetStore struct {
	db    *sql.DB
	scope tally.Scope
}

// newOffsetStore creates a new SQL offset store
func newOffsetStore(db *sql.DB, scope tally.Scope) offsetStore {
	return &sqloffsetStore{
		db:    db,
		scope: scope.SubScope("offset_store"),
	}
}

// Initialize creates an offset entry for a tenant+topic+partition if it doesn't exist
func (s *sqloffsetStore) Initialize(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "initialize", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()

	// Try to insert, ignore if already exists
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT IGNORE INTO %s (tenant, topic, partition_key, consumer_group, offset_acked, updated_at)
		VALUES (?, ?, ?, ?, 0, ?)
	`, OffsetsTableName), tenant, topic, partitionKey, consumerGroup, now)

	if err != nil {
		return fmt.Errorf("initialize offset tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	return nil
}

// GetAckedOffset returns the current acked offset for a tenant+topic+partition
func (s *sqloffsetStore) GetAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) (_ int64, retErr error) {
	op := metrics.Begin(s.scope, "get_acked_offset", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	var offset int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT offset_acked FROM %s WHERE tenant = ? AND topic = ? AND partition_key = ? AND consumer_group = ?
	`, OffsetsTableName), tenant, topic, partitionKey, consumerGroup).Scan(&offset)

	if err == sql.ErrNoRows {
		// Partition not yet initialized, return 0
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("get acked offset tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	return offset, nil
}

// UpdateAckedOffset updates the offset_acked for a tenant+topic+partition (only if new offset is greater)
func (s *sqloffsetStore) UpdateAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string, offset int64, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "update_acked_offset", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET offset_acked = ?, updated_at = ?
		WHERE tenant = ? AND topic = ? AND partition_key = ? AND consumer_group = ? AND offset_acked < ?
	`, OffsetsTableName), offset, now, tenant, topic, partitionKey, consumerGroup, offset)

	if err != nil {
		return fmt.Errorf("update acked offset tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	return nil
}

// GetMinAckedOffset returns the minimum offset_acked across all consumer groups
// for a tenant+topic+partition. Returns (0, false, nil) if no offset rows exist.
func (s *sqloffsetStore) GetMinAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string) (_ int64, _ bool, retErr error) {
	op := metrics.Begin(s.scope, "get_min_acked_offset", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	var minOffset int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(MIN(offset_acked), 0) FROM %s WHERE tenant = ? AND topic = ? AND partition_key = ?
	`, OffsetsTableName), tenant, topic, partitionKey).Scan(&minOffset)

	if err != nil {
		return 0, false, fmt.Errorf("query min acked offset tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	if minOffset == 0 {
		return 0, false, nil
	}

	return minOffset, true, nil
}

// DeleteOffset removes one consumer group's offset row for a partition.
// Idempotent — see the offsetStore interface doc.
func (s *sqloffsetStore) DeleteOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "delete_offset", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup))
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE tenant = ? AND topic = ? AND partition_key = ? AND consumer_group = ?
	`, OffsetsTableName), tenant, topic, partitionKey, consumerGroup)

	if err != nil {
		return fmt.Errorf("delete offset tenant=%s topic=%s partition=%s: %w", tenant, topic, partitionKey, err)
	}

	return nil
}
