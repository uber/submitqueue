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

// sqldeliveryStateStore is the SQL implementation of deliveryStateStore
type sqldeliveryStateStore struct {
	db     *sql.DB
	logger *zap.SugaredLogger
	scope  tally.Scope
}

// newDeliveryStateStore creates a new SQL delivery state store
func newDeliveryStateStore(db *sql.DB, logger *zap.SugaredLogger, scope tally.Scope) deliveryStateStore {
	return &sqldeliveryStateStore{
		db:     db,
		logger: logger.Named("delivery_state_store"),
		scope:  scope.SubScope("delivery_state_store"),
	}
}

// MarkDelivered inserts a row marking message as in-flight for this consumer group.
// Returns the resulting retry_count after the operation.
//
// The INSERT and subsequent SELECT are not in a transaction. This is safe because
// partition leasing guarantees a single writer per (consumer_group, topic, partition_key)
// — only the lease holder calls MarkDelivered for a given partition, so no concurrent
// mutation can occur between the two statements.
func (s *sqldeliveryStateStore) MarkDelivered(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (_ int, retErr error) {
	op := metrics.Begin(s.scope, "mark_delivered",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count)
		VALUES (?, ?, ?, ?, FALSE, ?, 0)
		ON DUPLICATE KEY UPDATE
			invisible_until = IF(acked = FALSE, VALUES(invisible_until), invisible_until),
			retry_count = IF(acked = FALSE, retry_count + 1, retry_count)
	`, DeliveryStateTableName),
		consumerGroup, topic, partitionKey, offset, invisibleUntil)

	if err != nil {
		return 0, fmt.Errorf("mark delivered topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	// Read retry_count after INSERT/UPDATE to get the current value.
	// For new inserts, retry_count = 0. For updates (redelivery), retry_count was incremented.
	var retryCount int
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT retry_count FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ?
	`, DeliveryStateTableName), consumerGroup, topic, partitionKey, offset).Scan(&retryCount)
	if err != nil {
		return 0, fmt.Errorf("get retry count after mark delivered topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	return retryCount, nil
}

// ExtendVisibility extends the visibility timeout for an in-flight message
// without incrementing retry_count. Used by ExtendVisibilityTimeout.
func (s *sqldeliveryStateStore) ExtendVisibility(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "extend_visibility",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET invisible_until = ?
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ? AND acked = FALSE
	`, DeliveryStateTableName),
		invisibleUntil, consumerGroup, topic, partitionKey, offset)

	if err != nil {
		return fmt.Errorf("extend visibility topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	return nil
}

// MarkAcked sets acked = TRUE to indicate this group has processed the message.
func (s *sqldeliveryStateStore) MarkAcked(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64) (retErr error) {
	op := metrics.Begin(s.scope, "mark_acked",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count)
		VALUES (?, ?, ?, ?, TRUE, 0, 0)
		ON DUPLICATE KEY UPDATE acked = TRUE
	`, DeliveryStateTableName),
		consumerGroup, topic, partitionKey, offset)

	if err != nil {
		return fmt.Errorf("mark acked topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	return nil
}

// MarkNacked sets invisible_until = now + delay to schedule redelivery.
// retry_count is NOT incremented here — it is incremented by MarkDelivered on redelivery.
func (s *sqldeliveryStateStore) MarkNacked(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, delayMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "mark_nacked",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + delayMs

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (consumer_group, topic, partition_key, message_offset, acked, invisible_until, retry_count)
		VALUES (?, ?, ?, ?, FALSE, ?, 0)
		ON DUPLICATE KEY UPDATE
			invisible_until = IF(acked = FALSE, VALUES(invisible_until), invisible_until)
	`, DeliveryStateTableName),
		consumerGroup, topic, partitionKey, offset, invisibleUntil)

	if err != nil {
		return fmt.Errorf("mark nacked topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	return nil
}

// GetDeliveryState returns the full delivery state for a message offset.
// Returns (state, found, error). found=false means no row (never delivered).
func (s *sqldeliveryStateStore) GetDeliveryState(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64) (_ DeliveryState, _ bool, retErr error) {
	op := metrics.Begin(s.scope, "get_delivery_state",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	var state DeliveryState
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT acked, invisible_until, retry_count FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset = ?
	`, DeliveryStateTableName), consumerGroup, topic, partitionKey, offset).Scan(&state.Acked, &state.InvisibleUntil, &state.RetryCount)

	if err == sql.ErrNoRows {
		return DeliveryState{}, false, nil
	}
	if err != nil {
		return DeliveryState{}, false, fmt.Errorf("get delivery state topic=%s partition=%s offset=%d: %w", topic, partitionKey, offset, err)
	}

	return state, true, nil
}

// AdvanceWatermark computes the new contiguous acked watermark and cleans up
// delivery state rows that are behind it.
// offsets are the actual message offsets above the current watermark (from messageStore).
// Returns the new watermark (highest contiguous acked offset from currentWatermark).
func (s *sqldeliveryStateStore) AdvanceWatermark(ctx context.Context, consumerGroup, topic, partitionKey string, currentWatermark int64, offsets []int64) (_ int64, retErr error) {
	op := metrics.Begin(s.scope, "advance_watermark",
		metrics.NewTag("topic", topic),
		metrics.NewTag("consumer_group", consumerGroup),
		metrics.NewTag("partition_key", partitionKey))
	defer func() { op.Complete(retErr) }()

	if len(offsets) == 0 {
		return currentWatermark, nil
	}

	// Batch-fetch delivery state for the provided offsets.
	placeholders := make([]byte, 0, len(offsets)*2-1)
	args := make([]interface{}, 0, 3+len(offsets))
	args = append(args, consumerGroup, topic, partitionKey)
	for i, offset := range offsets {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, offset)
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT message_offset, acked FROM %s
		WHERE consumer_group = ? AND topic = ? AND partition_key = ?
		AND message_offset IN (%s)
	`, DeliveryStateTableName, string(placeholders)), args...)
	if err != nil {
		return currentWatermark, fmt.Errorf("query delivery state for watermark topic=%s partition=%s: %w", topic, partitionKey, err)
	}
	defer rows.Close()

	// Build lookup map: offset -> acked
	ackedMap := make(map[int64]bool, len(offsets))
	for rows.Next() {
		var offset int64
		var acked bool
		if err := rows.Scan(&offset, &acked); err != nil {
			return currentWatermark, fmt.Errorf("scan delivery state topic=%s partition=%s: %w", topic, partitionKey, err)
		}
		ackedMap[offset] = acked
	}
	if err := rows.Err(); err != nil {
		return currentWatermark, fmt.Errorf("delivery state iteration topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	// Walk message offsets in order. Advance while contiguous acked.
	// Stop at first offset that is not acked (in-flight, nacked, or undelivered).
	newWatermark := currentWatermark
	for _, offset := range offsets {
		acked, exists := ackedMap[offset]
		if !exists || !acked {
			// No delivery state (undelivered) or not acked — stop
			break
		}
		newWatermark = offset
	}

	// Cleanup error is swallowed because the watermark was already computed and
	// will be returned to the caller. The stale delivery state rows behind the
	// watermark are harmless — they are never read again (all queries use
	// offset > watermark). Cleanup is retried on the next AdvanceWatermark call.
	if newWatermark > currentWatermark {
		_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
			DELETE FROM %s
			WHERE consumer_group = ? AND topic = ? AND partition_key = ? AND message_offset <= ?
		`, DeliveryStateTableName), consumerGroup, topic, partitionKey, newWatermark)
		if err != nil {
			metrics.NamedCounter(s.scope, "advance_watermark", "cleanup_errors", 1,
				metrics.NewTag("topic", topic))
			s.logger.Warnw("failed to clean up delivery state behind watermark",
				logTopic, topic,
				logPartitionKey, partitionKey,
				"watermark", newWatermark,
				logError, err,
			)
		}
	}

	return newWatermark, nil
}
