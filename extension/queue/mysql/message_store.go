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
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity/queue"
)

// sqlmessageStore is the SQL implementation of messageStore
type sqlmessageStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	scope   tally.Scope
}

// newMessageStore creates a new SQL message store
func newMessageStore(db *sql.DB, logger *zap.Logger, scope tally.Scope) messageStore {
	return &sqlmessageStore{
		db:      db,
		logger:  logger.Sugar().Named("queue_mysql_message_store"),
		scope:   scope.SubScope("queue_mysql_message_store"),
	}
}

// Insert inserts messages into the messages table
func (s *sqlmessageStore) Insert(ctx context.Context, topic string, messages []queue.Message) (retErr error) {
	op := metrics.Begin(s.scope, "insert")
	defer func() { op.Complete(retErr) }()

	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for topic %s: %w", topic, err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, retry_count, invisible_until, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, '', '')
	`, MessagesTableName))
	if err != nil {
		return fmt.Errorf("failed to prepare statement for topic %s: %w", topic, err)
	}
	defer stmt.Close()

	now := time.Now().UnixMilli()
	for _, msg := range messages {
		var metadataJSON []byte
		if len(msg.Metadata) > 0 {
			metadataJSON, err = json.Marshal(msg.Metadata)
			if err != nil {
				return fmt.Errorf("failed to marshal metadata for message ID %s in topic %s: %w", msg.ID, topic, err)
			}
		}

		_, err = stmt.ExecContext(ctx,
			topic,
			msg.ID,
			msg.Payload,
			metadataJSON,
			msg.PartitionKey,
			now,
			msg.PublishedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert message ID %s with partition key %s in topic %s: %w", msg.ID, msg.PartitionKey, topic, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for message ID %s with partition key %s in topic %s: %w", messages[0].ID, messages[0].PartitionKey, topic, err)
	}

	s.logger.Debugw("inserted messages", "count", len(messages), logTopic, topic)
	return nil
}

// Delete deletes a message by topic, partition key, and ID
func (s *sqlmessageStore) Delete(ctx context.Context, topic string, partitionKey string, messageID string) (retErr error) {
	op := metrics.Begin(s.scope, "delete")
	defer func() { op.Complete(retErr) }()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)

	if err != nil {
		return fmt.Errorf("failed to delete message %s in topic %s partition %s: %w", messageID, topic, partitionKey, err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		metrics.NamedCounter(s.scope, "delete", "messages_deleted", rows)
	}

	return nil
}

// FetchByOffset fetches visible messages with offset > currentOffset for a specific partition
// Atomically sets invisible_until and increments retry_count for fetched messages
func (s *sqlmessageStore) FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, limit int, visibilityTimeoutMs int64) (_ []messageRow, retErr error) {
	op := metrics.Begin(s.scope, "fetch")
	defer func() { op.Complete(retErr) }()

	now := time.Now().UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	// Start transaction to atomically fetch and update messages
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin fetch transaction for topic %s partition %s: %w", topic, partitionKey, err)
	}
	defer tx.Rollback()

	// Fetch visible messages (invisible_until <= now)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT offset, id, payload, metadata, partition_key, retry_count, published_at, failed_at, failure_count, last_error, original_topic
		FROM %s
		WHERE topic = ? AND partition_key = ? AND offset > ? AND invisible_until <= ?
		ORDER BY offset
		LIMIT ?
	`, MessagesTableName), topic, partitionKey, currentOffset, now, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages for topic %s partition %s: %w", topic, partitionKey, err)
	}
	defer rows.Close()

	var results []messageRow
	var messageIDs []string

	for rows.Next() {
		var (
			offset           int64
			id               string
			payload          []byte
			metadataJSON     []byte
			partKey          string
			retryCount       int
			publishedAtMilli int64
			failedAt         int64
			failureCount     int
			lastError        string
			originalTopic    string
		)

		if err := rows.Scan(&offset, &id, &payload, &metadataJSON, &partKey, &retryCount, &publishedAtMilli, &failedAt, &failureCount, &lastError, &originalTopic); err != nil {
			return nil, fmt.Errorf("failed to scan message row for topic %s partition %s: %w", topic, partitionKey, err)
		}

		var metadata map[string]string
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
				return nil, fmt.Errorf("failed to unmarshal metadata for message %s in topic %s partition %s: %w", id, topic, partitionKey, err)
			}
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}

		results = append(results, messageRow{
			Offset:        offset,
			ID:            id,
			Payload:       payload,
			Metadata:      metadata,
			PartitionKey:  partKey,
			RetryCount:    retryCount,
			PublishedAt:   publishedAtMilli,
			FailedAt:      failedAt,
			FailureCount:  failureCount,
			LastError:     lastError,
			OriginalTopic: originalTopic,
		})

		messageIDs = append(messageIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error for topic %s partition %s: %w", topic, partitionKey, err)
	}

	// Update invisible_until and increment retry_count for fetched messages
	if len(messageIDs) > 0 {
		// Build IN clause for message IDs
		placeholders := ""
		for i := range messageIDs {
			if i > 0 {
				placeholders += ","
			}
			placeholders += "?"
		}

		query := fmt.Sprintf(`
			UPDATE %s
			SET invisible_until = ?, retry_count = retry_count + 1
			WHERE topic = ? AND partition_key = ? AND id IN (%s)
		`, MessagesTableName, placeholders)

		args := []interface{}{invisibleUntil, topic, partitionKey}
		for _, id := range messageIDs {
			args = append(args, id)
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to update visibility for %d messages in topic %s partition %s: %w", len(messageIDs), topic, partitionKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit fetch transaction for topic %s partition %s: %w", topic, partitionKey, err)
	}

	metrics.NamedCounter(s.scope, "fetch", "messages_fetched", int64(len(results)))
	s.logger.Debugw("fetched messages",
		logTopic, topic,
		logPartitionKey, partitionKey,
		"count", len(results),
	)

	return results, nil
}

// MoveToDLQ atomically moves a message to the DLQ by reinserting it with the DLQ topic name
// The message is inserted back into queue_messages table with the DLQ topic (original + suffix)
// This allows DLQ messages to be consumed using the normal subscriber
func (s *sqlmessageStore) MoveToDLQ(ctx context.Context, topic string, partitionKey string, messageID string, failureCount int, lastError string, dlqTopicSuffix string) (retErr error) {
	op := metrics.Begin(s.scope, "move_to_dlq")
	defer func() { op.Complete(retErr) }()

	// Construct DLQ topic name
	dlqTopic := topic + dlqTopicSuffix

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin DLQ transaction for message %s in topic %s: %w", messageID, topic, err)
	}
	defer tx.Rollback()

	// Fetch the message from main table
	var (
		payload          []byte
		metadataJSON     []byte
		createdAtMilli   int64
		publishedAtMilli int64
		retryCount       int
	)

	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT payload, metadata, created_at, published_at, retry_count
		FROM %s
		WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID).Scan(&payload, &metadataJSON, &createdAtMilli, &publishedAtMilli, &retryCount)

	if err != nil {
		if err == sql.ErrNoRows {
			// Message already deleted or doesn't exist
			s.logger.Warnw("message not found for DLQ move",
				logTopic, topic,
				logMessageID, messageID,
			)
			return nil
		}
		return fmt.Errorf("failed to fetch message %s for DLQ in topic %s: %w", messageID, topic, err)
	}

	// Insert into queue_messages table with DLQ topic name and DLQ-specific fields
	// Reset retry_count to 0 since this is a new topic (DLQ processing starts fresh)
	// Store the original failure count for tracking purposes
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, invisible_until, retry_count, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, MessagesTableName), dlqTopic, messageID, payload, metadataJSON, partitionKey, createdAtMilli, publishedAtMilli, int64(0), 0, now, failureCount, lastError, topic)

	if err != nil {
		return fmt.Errorf("failed to insert message %s into DLQ topic %s (partition %s, failure_count %d): %w", messageID, dlqTopic, partitionKey, failureCount, err)
	}

	// Delete from original topic
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)

	if err != nil {
		return fmt.Errorf("failed to delete message %s from topic %s after DLQ insert: %w", messageID, topic, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit DLQ transaction for message %s in topic %s: %w", messageID, topic, err)
	}

	s.logger.Infow("moved message to DLQ",
		logTopic, topic,
		"dlq_topic", dlqTopic,
		logMessageID, messageID,
		logPartitionKey, partitionKey,
		"failure_count", failureCount,
		"last_error", lastError,
	)

	return nil
}

// SetVisibilityTimeout sets the invisible_until timestamp for a message
// visibilityTimeoutMillis: milliseconds from now to hide the message
// If visibilityTimeoutMillis is 0, makes the message visible immediately
// If visibilityTimeoutMillis > 0, makes the message invisible until now + visibilityTimeoutMillis
func (s *sqlmessageStore) SetVisibilityTimeout(ctx context.Context, topic string, partitionKey string, messageID string, visibilityTimeoutMillis int64) (retErr error) {
	op := metrics.Begin(s.scope, "set_visibility")
	defer func() { op.Complete(retErr) }()

	var invisibleUntil int64
	if visibilityTimeoutMillis > 0 {
		invisibleUntil = time.Now().UnixMilli() + visibilityTimeoutMillis
	} else {
		invisibleUntil = 0
	}

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET invisible_until = ?
		WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), invisibleUntil, topic, partitionKey, messageID)

	if err != nil {
		return fmt.Errorf("failed to set visibility timeout for message %s in topic %s (timeout_ms %d): %w", messageID, topic, visibilityTimeoutMillis, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnw("failed to check rows affected",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
	}

	if rows == 0 {
		s.logger.Debugw("no rows updated when setting visibility",
			logTopic, topic,
			logMessageID, messageID,
		)
	}

	s.logger.Debugw("set visibility timeout",
		logTopic, topic,
		logMessageID, messageID,
		"timeout_ms", visibilityTimeoutMillis,
	)

	return nil
}
