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
	"go.uber.org/zap"
)


// sqloffsetStore is the SQL implementation of offsetStore
type sqloffsetStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	metrics tally.Scope
}

// Metric names for offset store
const (
	metricAckMessageErrors = "ack_message.errors"
)

// newOffsetStore creates a new SQL offset store
func newOffsetStore(db *sql.DB, logger *zap.Logger, metrics tally.Scope) offsetStore {
	return &sqloffsetStore{
		db:      db,
		logger:  logger.Sugar().Named("offset_store"),
		metrics: metrics.SubScope("offset_store"),
	}
}

// Initialize creates an offset entry for a topic+partition if it doesn't exist
func (s *sqloffsetStore) Initialize(ctx context.Context, topic string, partitionKey string, consumerGroup string) error {
	now := time.Now().UnixMilli()

	// Try to insert, ignore if already exists
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT IGNORE INTO %s (consumer_group, topic, partition_key, offset_acked, updated_at)
		VALUES (?, ?, ?, 0, ?)
	`, OffsetsTableName), consumerGroup, topic, partitionKey, now)

	return err
}

// GetAckedOffset returns the current acked offset for a topic+partition
func (s *sqloffsetStore) GetAckedOffset(ctx context.Context, topic string, partitionKey string, consumerGroup string) (int64, error) {
	var offset int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT offset_acked FROM %s WHERE consumer_group = ? AND topic = ? AND partition_key = ?
	`, OffsetsTableName), consumerGroup, topic, partitionKey).Scan(&offset)

	if err == sql.ErrNoRows {
		// Partition not yet initialized, return 0
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get acked offset: %w", err)
	}

	return offset, nil
}

// UpdateAckedOffset updates the offset_acked for a topic+partition (only if new offset is greater)
func (s *sqloffsetStore) UpdateAckedOffset(ctx context.Context, topic string, partitionKey string, offset int64, consumerGroup string) error {
	now := time.Now().UnixMilli()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET offset_acked = ?, updated_at = ?
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND offset_acked < ?
	`, OffsetsTableName), offset, now, consumerGroup, topic, partitionKey, offset)

	return err
}

// AckMessage atomically deletes a message and updates the acked offset
func (s *sqloffsetStore) AckMessage(ctx context.Context, topic string, partitionKey string, messageID string, offset int64, consumerGroup string, messageStore messageStore) error {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("ack_message.latency").Record(time.Since(start))
	}()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.metrics.Tagged(map[string]string{tagErrorType: "begin_transaction"}).Counter(metricAckMessageErrors).Inc(1)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Delete message
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)
	if err != nil {
		s.metrics.Tagged(map[string]string{tagErrorType: "delete_message"}).Counter(metricAckMessageErrors).Inc(1)
		return fmt.Errorf("failed to delete message: %w", err)
	}

	now := start.UnixMilli()

	// Update offset_acked (insert if not exists)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, offset_acked, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			offset_acked = IF(VALUES(offset_acked) > offset_acked, VALUES(offset_acked), offset_acked),
			updated_at = VALUES(updated_at)
	`, OffsetsTableName), consumerGroup, topic, partitionKey, offset, now)
	if err != nil {
		s.metrics.Tagged(map[string]string{tagErrorType: "update_offset"}).Counter(metricAckMessageErrors).Inc(1)
		return fmt.Errorf("failed to update offset: %w", err)
	}

	if err := tx.Commit(); err != nil {
		s.metrics.Tagged(map[string]string{tagErrorType: "commit"}).Counter(metricAckMessageErrors).Inc(1)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Log and emit metrics after transaction completes
	s.metrics.Counter("ack_message.success").Inc(1)
	s.logger.Debugw("acked message",
		logTopic, topic,
		logPartitionKey, partitionKey,
		logMessageID, messageID,
		"offset", offset,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	success = true
	return nil
}
