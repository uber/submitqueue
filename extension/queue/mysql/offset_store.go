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

// sqloffsetStore is the SQL implementation of offsetStore
type sqloffsetStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	scope   tally.Scope
}

// newOffsetStore creates a new SQL offset store
func newOffsetStore(db *sql.DB, logger *zap.Logger, scope tally.Scope) offsetStore {
	return &sqloffsetStore{
		db:      db,
		logger:  logger.Sugar().Named("queue_mysql_offset_store"),
		scope:   scope.SubScope("queue_mysql_offset_store"),
	}
}

// Initialize creates an offset entry for a topic+partition if it doesn't exist
func (s *sqloffsetStore) Initialize(ctx context.Context, topic string, partitionKey string, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "initialize")
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()

	// Try to insert, ignore if already exists
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT IGNORE INTO %s (consumer_group, topic, partition_key, offset_acked, updated_at)
		VALUES (?, ?, ?, 0, ?)
	`, OffsetsTableName), consumerGroup, topic, partitionKey, now)

	if err != nil {
		return fmt.Errorf("failed to initialize offset for topic %s partition %s: %w", topic, partitionKey, err)
	}

	return nil
}

// GetAckedOffset returns the current acked offset for a topic+partition
func (s *sqloffsetStore) GetAckedOffset(ctx context.Context, topic string, partitionKey string, consumerGroup string) (_ int64, retErr error) {
	op := metrics.Begin(s.scope, "get_acked_offset")
	defer func() { op.Complete(retErr) }()

	var offset int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT offset_acked FROM %s WHERE consumer_group = ? AND topic = ? AND partition_key = ?
	`, OffsetsTableName), consumerGroup, topic, partitionKey).Scan(&offset)

	if err == sql.ErrNoRows {
		// Partition not yet initialized, return 0
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get acked offset for topic %s partition %s: %w", topic, partitionKey, err)
	}

	return offset, nil
}

// UpdateAckedOffset updates the offset_acked for a topic+partition (only if new offset is greater)
func (s *sqloffsetStore) UpdateAckedOffset(ctx context.Context, topic string, partitionKey string, offset int64, consumerGroup string) (retErr error) {
	op := metrics.Begin(s.scope, "update_acked_offset")
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET offset_acked = ?, updated_at = ?
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND offset_acked < ?
	`, OffsetsTableName), offset, now, consumerGroup, topic, partitionKey, offset)

	if err != nil {
		return fmt.Errorf("failed to update acked offset for topic %s partition %s: %w", topic, partitionKey, err)
	}

	return nil
}

// AckMessage atomically deletes a message and updates the acked offset
func (s *sqloffsetStore) AckMessage(ctx context.Context, topic string, partitionKey string, messageID string, offset int64, consumerGroup string, messageStore messageStore) (retErr error) {
	op := metrics.Begin(s.scope, "ack_message")
	defer func() { op.Complete(retErr) }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for topic %s partition %s: %w", topic, partitionKey, err)
	}
	defer tx.Rollback()

	// Delete message
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete message %s in topic %s partition %s: %w", messageID, topic, partitionKey, err)
	}

	now := time.Now().UnixMilli()

	// Update offset_acked (insert if not exists)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, offset_acked, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			offset_acked = IF(VALUES(offset_acked) > offset_acked, VALUES(offset_acked), offset_acked),
			updated_at = VALUES(updated_at)
	`, OffsetsTableName), consumerGroup, topic, partitionKey, offset, now)
	if err != nil {
		return fmt.Errorf("failed to update offset for topic %s partition %s: %w", topic, partitionKey, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for topic %s partition %s: %w", topic, partitionKey, err)
	}

	s.logger.Debugw("acked message",
		logTopic, topic,
		logPartitionKey, partitionKey,
		logMessageID, messageID,
		"offset", offset,
	)

	return nil
}
