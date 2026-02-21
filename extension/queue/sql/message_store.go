package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entity/queue"
)


// sqlmessageStore is the SQL implementation of messageStore
type sqlmessageStore struct {
	db      *sql.DB
	logger  *zap.SugaredLogger
	metrics tally.Scope
}

// Metric names for message store
const (
	metricInsertErrors   = "insert.errors"
	metricFetchErrors    = "fetch.errors"
	metricMoveToDLQErrors = "move_to_dlq.errors"
)

// newMessageStore creates a new SQL message store
func newMessageStore(db *sql.DB, logger *zap.Logger, metrics tally.Scope) messageStore {
	return &sqlmessageStore{
		db:      db,
		logger:  logger.Sugar().Named("message_store"),
		metrics: metrics.SubScope("message_store"),
	}
}

// Insert inserts messages into the messages table
func (s *sqlmessageStore) Insert(ctx context.Context, topic string, messages []queue.Message) error {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("insert.latency").Record(time.Since(start))
	}()

	if len(messages) == 0 {
		return nil
	}

	s.logger.Debugw("inserting messages",
		logTopic, topic,
		"count", len(messages),
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Errorw("failed to begin transaction",
			logTopic, topic,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "begin_transaction"}).Counter(metricInsertErrors).Inc(1)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, retry_count, invisible_until, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, '', '')
	`, MessagesTableName))
	if err != nil {
		s.logger.Errorw("failed to prepare statement",
			logTopic, topic,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "prepare_statement"}).Counter(metricInsertErrors).Inc(1)
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := start.UnixMilli()
	for _, msg := range messages {
		var metadataJSON []byte
		if len(msg.Metadata) > 0 {
			metadataJSON, err = json.Marshal(msg.Metadata)
			if err != nil {
				s.logger.Errorw("failed to marshal metadata",
					logTopic, topic,
					logMessageID, msg.ID,
					logError, err,
				)
				s.metrics.Tagged(map[string]string{tagErrorType: "marshal_metadata"}).Counter(metricInsertErrors).Inc(1)
				return fmt.Errorf("failed to marshal metadata: %w", err)
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
			s.logger.Errorw("failed to insert message",
				logTopic, topic,
				logMessageID, msg.ID,
				logPartitionKey, msg.PartitionKey,
				logError, err,
			)
			s.metrics.Tagged(map[string]string{tagErrorType: "exec_statement"}).Counter(metricInsertErrors).Inc(1)
			return fmt.Errorf("failed to insert message: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		s.logger.Errorw("failed to commit transaction",
			logTopic, topic,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "commit"}).Counter(metricInsertErrors).Inc(1)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.metrics.Counter("insert.success").Inc(1)
	s.metrics.Counter("messages.inserted").Inc(int64(len(messages)))
	s.logger.Debugw("inserted messages",
		logTopic, topic,
		"count", len(messages),
		"duration_ms", time.Since(start).Milliseconds(),
	)

	success = true
	return nil
}

// Delete deletes a message by topic and ID
func (s *sqlmessageStore) Delete(ctx context.Context, topic string, messageID string) error {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("delete.latency").Record(time.Since(start))
	}()

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND id = ?
	`, MessagesTableName), topic, messageID)

	if err != nil {
		s.logger.Errorw("failed to delete message",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "exec_delete"}).Counter("delete.errors").Inc(1)
		return err
	}

	rows, _ := result.RowsAffected()
	s.metrics.Counter("delete.success").Inc(1)
	if rows > 0 {
		s.metrics.Counter("messages.deleted").Inc(rows)
	}

	success = true
	return nil
}

// FetchByOffset fetches visible messages with offset > currentOffset for a specific partition
// Atomically sets invisible_until and increments retry_count for fetched messages
func (s *sqlmessageStore) FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, limit int, visibilityTimeoutMs int64) ([]messageRow, error) {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("fetch.latency").Record(time.Since(start))
	}()

	now := start.UnixMilli()
	invisibleUntil := now + visibilityTimeoutMs

	// Start transaction to atomically fetch and update messages
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Errorw("failed to begin transaction for fetch",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "begin_transaction"}).Counter(metricFetchErrors).Inc(1)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
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
		s.logger.Errorw("failed to query messages",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "query"}).Counter(metricFetchErrors).Inc(1)
		return nil, fmt.Errorf("failed to query messages: %w", err)
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
			s.logger.Errorw("failed to scan message row",
				logTopic, topic,
				logPartitionKey, partitionKey,
				logError, err,
			)
			s.metrics.Tagged(map[string]string{tagErrorType: "scan_row"}).Counter(metricFetchErrors).Inc(1)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		var metadata map[string]string
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
				s.logger.Errorw("failed to unmarshal metadata",
					logTopic, topic,
					logPartitionKey, partitionKey,
					logMessageID, id,
					logError, err,
				)
				s.metrics.Tagged(map[string]string{tagErrorType: "unmarshal_metadata"}).Counter(metricFetchErrors).Inc(1)
				return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
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
		s.logger.Errorw("row iteration error",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "row_iteration"}).Counter(metricFetchErrors).Inc(1)
		return nil, fmt.Errorf("row iteration error: %w", err)
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
			s.logger.Errorw("failed to update message visibility",
				logTopic, topic,
				logPartitionKey, partitionKey,
				"message_count", len(messageIDs),
				logError, err,
			)
			s.metrics.Tagged(map[string]string{tagErrorType: "update_visibility"}).Counter(metricFetchErrors).Inc(1)
			return nil, fmt.Errorf("failed to update messages: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		s.logger.Errorw("failed to commit fetch transaction",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "commit"}).Counter(metricFetchErrors).Inc(1)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.metrics.Counter("fetch.success").Inc(1)
	s.metrics.Counter("messages.fetched").Inc(int64(len(results)))
	s.logger.Debugw("fetched messages",
		logTopic, topic,
		logPartitionKey, partitionKey,
		"count", len(results),
		"duration_ms", time.Since(start).Milliseconds(),
	)

	success = true
	return results, nil
}

// MoveToDLQ atomically moves a message to the DLQ by reinserting it with the DLQ topic name
// The message is inserted back into queue_messages table with the DLQ topic (original + suffix)
// This allows DLQ messages to be consumed using the normal subscriber
func (s *sqlmessageStore) MoveToDLQ(ctx context.Context, topic string, messageID string, failureCount int, lastError string, dlqTopicSuffix string) error {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("move_to_dlq.latency").Record(time.Since(start))
	}()

	// Construct DLQ topic name
	dlqTopic := topic + dlqTopicSuffix

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Errorw("failed to begin transaction for DLQ move",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "begin_transaction"}).Counter(metricMoveToDLQErrors).Inc(1)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Fetch the message from main table
	var (
		payload          []byte
		metadataJSON     []byte
		partitionKey     string
		createdAtMilli   int64
		publishedAtMilli int64
		retryCount       int
	)

	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT payload, metadata, partition_key, created_at, published_at, retry_count
		FROM %s
		WHERE topic = ? AND id = ?
	`, MessagesTableName), topic, messageID).Scan(&payload, &metadataJSON, &partitionKey, &createdAtMilli, &publishedAtMilli, &retryCount)

	if err != nil {
		if err == sql.ErrNoRows {
			// Message already deleted or doesn't exist
			s.logger.Debugw("message not found for DLQ move",
				logTopic, topic,
				logMessageID, messageID,
			)
			return nil
		}
		s.logger.Errorw("failed to fetch message for DLQ",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "fetch_message"}).Counter(metricMoveToDLQErrors).Inc(1)
		return fmt.Errorf("failed to fetch message: %w", err)
	}

	// Insert into queue_messages table with DLQ topic name and DLQ-specific fields
	// Reset retry_count to 0 since this is a new topic (DLQ processing starts fresh)
	// Store the original failure count for tracking purposes
	now := start.UnixMilli()
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, invisible_until, retry_count, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, MessagesTableName), dlqTopic, messageID, payload, metadataJSON, partitionKey, createdAtMilli, publishedAtMilli, int64(0), 0, now, failureCount, lastError, topic)

	if err != nil {
		s.logger.Errorw("failed to insert into DLQ topic",
			logTopic, topic,
			"dlq_topic", dlqTopic,
			logMessageID, messageID,
			logPartitionKey, partitionKey,
			"failure_count", failureCount,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "insert_dlq"}).Counter(metricMoveToDLQErrors).Inc(1)
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	// Delete from original topic
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND id = ?
	`, MessagesTableName), topic, messageID)

	if err != nil {
		s.logger.Errorw("failed to delete from main table after DLQ insert",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "delete_from_main"}).Counter(metricMoveToDLQErrors).Inc(1)
		return fmt.Errorf("failed to delete from main table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		s.logger.Errorw("failed to commit DLQ transaction",
			logTopic, topic,
			logMessageID, messageID,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "commit"}).Counter(metricMoveToDLQErrors).Inc(1)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.metrics.Counter("move_to_dlq.success").Inc(1)
	s.metrics.Counter("messages.moved_to_dlq").Inc(1)
	s.logger.Infow("moved message to DLQ",
		logTopic, topic,
		"dlq_topic", dlqTopic,
		logMessageID, messageID,
		logPartitionKey, partitionKey,
		"failure_count", failureCount,
		"last_error", lastError,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	success = true
	return nil
}

// SetVisibilityTimeout sets the invisible_until timestamp for a message
// visibilityTimeoutMillis: milliseconds from now to hide the message
// If visibilityTimeoutMillis is 0, makes the message visible immediately
// If visibilityTimeoutMillis > 0, makes the message invisible until now + visibilityTimeoutMillis
func (s *sqlmessageStore) SetVisibilityTimeout(ctx context.Context, topic string, messageID string, visibilityTimeoutMillis int64) error {
	start := time.Now()
	success := false
	defer func() {
		result := "error"
		if success {
			result = "success"
		}
		s.metrics.Tagged(map[string]string{"result": result}).Timer("set_visibility.latency").Record(time.Since(start))
	}()

	var invisibleUntil int64
	if visibilityTimeoutMillis > 0 {
		invisibleUntil = start.UnixMilli() + visibilityTimeoutMillis
	} else {
		invisibleUntil = 0
	}

	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET invisible_until = ?
		WHERE topic = ? AND id = ?
	`, MessagesTableName), invisibleUntil, topic, messageID)

	if err != nil {
		s.logger.Errorw("failed to set visibility timeout",
			logTopic, topic,
			logMessageID, messageID,
			"timeout_ms", visibilityTimeoutMillis,
			logError, err,
		)
		s.metrics.Tagged(map[string]string{tagErrorType: "exec_set"}).Counter("set_visibility.errors").Inc(1)
		return fmt.Errorf("failed to set visibility timeout: %w", err)
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

	s.metrics.Counter("set_visibility.success").Inc(1)
	s.logger.Debugw("set visibility timeout",
		logTopic, topic,
		logMessageID, messageID,
		"timeout_ms", visibilityTimeoutMillis,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	success = true
	return nil
}
