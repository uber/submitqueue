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

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/platform/metrics"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

// sqlmessageStore is the SQL implementation of messageStore
type sqlmessageStore struct {
	db     *sql.DB
	logger *zap.SugaredLogger
	scope  tally.Scope
}

// newMessageStore creates a new SQL message store
func newMessageStore(db *sql.DB, logger *zap.SugaredLogger, scope tally.Scope) messageStore {
	return &sqlmessageStore{
		db:     db,
		logger: logger.Named("message_store"),
		scope:  scope.SubScope("message_store"),
	}
}

// Insert inserts messages into the messages table with no visibility delay.
// Equivalent to InsertDelayed with visibleAfterMs == 0.
func (s *sqlmessageStore) Insert(ctx context.Context, topic string, messages []entityqueue.Message) error {
	return s.InsertDelayed(ctx, topic, messages, 0)
}

// InsertDelayed inserts messages into the messages table, optionally deferring
// delivery until visibleAfterMs (epoch milliseconds). 0 means immediately
// visible; FetchByOffset skips rows where visible_after > now.
//
// Publishes are idempotent on the (topic, partition_key, id) unique key: a
// repeated publish for the same key is silently treated as success and does
// not overwrite the original payload. This matches the queue_messages schema's
// documented intent ("Supports: INSERT ... ON DUPLICATE KEY to enforce
// idempotent publishes") and lets callers safely retry publishes (e.g. a
// second Cancel RPC for the same request) without surfacing 1062 duplicate-key
// errors.
func (s *sqlmessageStore) InsertDelayed(ctx context.Context, topic string, messages []entityqueue.Message, visibleAfterMs int64) (retErr error) {
	op := metrics.Begin(s.scope, "insert", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	if len(messages) == 0 {
		return nil
	}

	s.logger.Debugw("inserting messages",
		logTopic, topic,
		"count", len(messages),
		"visible_after", visibleAfterMs,
	)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction topic=%s: %w", topic, err)
	}
	defer tx.Rollback()

	// ON DUPLICATE KEY UPDATE topic=topic is a no-op write that makes MySQL
	// swallow the unique-key violation without mutating the existing row.
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, visible_after, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', '')
		ON DUPLICATE KEY UPDATE topic = topic
	`, MessagesTableName))
	if err != nil {
		return fmt.Errorf("prepare statement topic=%s: %w", topic, err)
	}
	defer stmt.Close()

	now := time.Now().UnixMilli()
	for _, msg := range messages {
		var metadataJSON []byte
		if len(msg.Metadata) > 0 {
			metadataJSON, err = json.Marshal(msg.Metadata)
			if err != nil {
				return fmt.Errorf("marshal metadata topic=%s message=%s: %w", topic, msg.ID, err)
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
			visibleAfterMs,
		)
		if err != nil {
			return fmt.Errorf("insert message topic=%s message=%s partition=%s: %w", topic, msg.ID, msg.PartitionKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction topic=%s: %w", topic, err)
	}

	s.logger.Debugw("inserted messages",
		logTopic, topic,
		"count", len(messages),
	)

	return nil
}

// Delete deletes a message by topic, partition key, and ID
func (s *sqlmessageStore) Delete(ctx context.Context, topic string, partitionKey string, messageID string) (retErr error) {
	op := metrics.Begin(s.scope, "delete", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)

	if err != nil {
		return fmt.Errorf("delete message topic=%s partition=%s message=%s: %w", topic, partitionKey, messageID, err)
	}

	return nil
}

// FetchByOffset fetches messages with offset > currentOffset for a specific partition.
// Rows whose visible_after > nowMs are skipped — those are deferred deliveries
// (published via InsertDelayed) that should not yet be surfaced to subscribers.
// Messages are fetched from the immutable log; no per-message mutation occurs.
func (s *sqlmessageStore) FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, nowMs int64, limit int) (_ []messageRow, retErr error) {
	op := metrics.Begin(s.scope, "fetch", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT offset, id, payload, metadata, partition_key, published_at, failed_at, failure_count, last_error, original_topic
		FROM %s
		WHERE topic = ? AND partition_key = ? AND offset > ? AND visible_after <= ?
		ORDER BY offset
		LIMIT ?
	`, MessagesTableName), topic, partitionKey, currentOffset, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("query messages topic=%s partition=%s: %w", topic, partitionKey, err)
	}
	defer rows.Close()

	var results []messageRow

	for rows.Next() {
		var (
			offset           int64
			id               string
			payload          []byte
			metadataJSON     []byte
			partKey          string
			publishedAtMilli int64
			failedAt         int64
			failureCount     int
			lastError        string
			originalTopic    string
		)

		if err := rows.Scan(&offset, &id, &payload, &metadataJSON, &partKey, &publishedAtMilli, &failedAt, &failureCount, &lastError, &originalTopic); err != nil {
			return nil, fmt.Errorf("scan row topic=%s partition=%s: %w", topic, partitionKey, err)
		}

		var metadata map[string]string
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata topic=%s partition=%s message=%s: %w", topic, partitionKey, id, err)
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
			PublishedAt:   publishedAtMilli,
			FailedAt:      failedAt,
			FailureCount:  failureCount,
			LastError:     lastError,
			OriginalTopic: originalTopic,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration topic=%s partition=%s: %w", topic, partitionKey, err)
	}

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
	op := metrics.Begin(s.scope, "move_to_dlq", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	// Construct DLQ topic name
	dlqTopic := topic + dlqTopicSuffix

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction topic=%s message=%s: %w", topic, messageID, err)
	}
	defer tx.Rollback()

	// Fetch the message from main table
	var (
		payload          []byte
		metadataJSON     []byte
		fetchPartKey     string
		createdAtMilli   int64
		publishedAtMilli int64
	)

	err = tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT payload, metadata, partition_key, created_at, published_at
		FROM %s
		WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID).Scan(&payload, &metadataJSON, &fetchPartKey, &createdAtMilli, &publishedAtMilli)

	if err != nil {
		if err == sql.ErrNoRows {
			// Message already deleted or doesn't exist
			s.logger.Debugw("message not found for DLQ move",
				logTopic, topic,
				logMessageID, messageID,
			)
			return nil
		}
		return fmt.Errorf("fetch message for DLQ topic=%s partition=%s message=%s: %w", topic, partitionKey, messageID, err)
	}

	// Insert into queue_messages table with DLQ topic name and DLQ-specific fields.
	// DLQ messages are always immediately visible (visible_after=0); any delay on
	// the original message has already been consumed by the time it failed.
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (topic, id, payload, metadata, partition_key, created_at, published_at, visible_after, failed_at, failure_count, last_error, original_topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
	`, MessagesTableName), dlqTopic, messageID, payload, metadataJSON, fetchPartKey, createdAtMilli, publishedAtMilli, now, failureCount, lastError, topic)

	if err != nil {
		return fmt.Errorf("insert into DLQ topic=%s dlq=%s partition=%s message=%s: %w", topic, dlqTopic, partitionKey, messageID, err)
	}

	// Delete from original topic
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND id = ?
	`, MessagesTableName), topic, partitionKey, messageID)

	if err != nil {
		return fmt.Errorf("delete from main table topic=%s partition=%s message=%s: %w", topic, partitionKey, messageID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DLQ transaction topic=%s message=%s: %w", topic, messageID, err)
	}

	return nil
}

// GarbageCollect deletes messages with offset <= minAckedOffset.
// The caller provides minAckedOffset (from offsetStore), keeping messageStore
// free of cross-table queries.
// Returns the number of rows deleted.
func (s *sqlmessageStore) GarbageCollect(ctx context.Context, topic string, partitionKey string, minAckedOffset int64) (_ int64, retErr error) {
	op := metrics.Begin(s.scope, "gc", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	if minAckedOffset == 0 {
		return 0, nil
	}

	// Delete messages up to the minimum acked offset
	result, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		DELETE FROM %s WHERE topic = ? AND partition_key = ? AND offset <= ?
	`, MessagesTableName), topic, partitionKey, minAckedOffset)

	if err != nil {
		return 0, fmt.Errorf("garbage collect messages topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	// RowsAffected error is swallowed because the DELETE query itself succeeded.
	// This is a driver-level diagnostic failure — the messages are already deleted.
	// We log for visibility but the GC operation is complete.
	deleted, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnw("garbage collect succeeded but row count unavailable (driver diagnostic failure), no impact on correctness",
			logTopic, topic,
			logPartitionKey, partitionKey,
			logError, err,
		)
	}
	if deleted > 0 {
		s.logger.Debugw("garbage collected messages",
			logTopic, topic,
			logPartitionKey, partitionKey,
			"deleted", deleted,
			"min_offset", minAckedOffset,
		)
		metrics.NamedCounter(s.scope, "gc", "messages_deleted", deleted, metrics.NewTag("topic", topic))
	}

	return deleted, nil
}

// GetOffsetsAbove returns message offsets above afterOffset for a partition, ordered ascending.
func (s *sqlmessageStore) GetOffsetsAbove(ctx context.Context, topic string, partitionKey string, afterOffset int64, limit int) (_ []int64, retErr error) {
	op := metrics.Begin(s.scope, "get_offsets_above", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT offset FROM %s
		WHERE topic = ? AND partition_key = ? AND offset > ?
		ORDER BY offset ASC
		LIMIT ?
	`, MessagesTableName), topic, partitionKey, afterOffset, limit)
	if err != nil {
		return nil, fmt.Errorf("query offsets topic=%s partition=%s: %w", topic, partitionKey, err)
	}
	defer rows.Close()

	var offsets []int64
	for rows.Next() {
		var offset int64
		if err := rows.Scan(&offset); err != nil {
			return nil, fmt.Errorf("scan offset topic=%s partition=%s: %w", topic, partitionKey, err)
		}
		offsets = append(offsets, offset)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("offset iteration topic=%s partition=%s: %w", topic, partitionKey, err)
	}

	return offsets, nil
}
