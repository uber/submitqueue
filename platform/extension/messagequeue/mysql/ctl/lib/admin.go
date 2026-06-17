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

package lib

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	mysql "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
)

// AdminStore provides read-only inspection and targeted admin mutations
// for the MySQL queue tables.
type AdminStore struct {
	db *sql.DB
}

// NewAdminStore creates a new AdminStore backed by the given database connection.
func NewAdminStore(db *sql.DB) *AdminStore {
	return &AdminStore{db: db}
}

// MessageSummary contains a subset of message fields for listing.
type MessageSummary struct {
	// Offset is the auto-incrementing sequence number
	Offset int64
	// ID is the unique message identifier
	ID string
	// Topic identifies the queue type
	Topic string
	// PartitionKey determines message distribution
	PartitionKey string
	// CreatedAt is the epoch milliseconds when the message was created
	CreatedAt int64
	// PublishedAt is the epoch milliseconds when the message was published
	PublishedAt int64
}

// MessageDetail contains all message fields including payload and DLQ info.
type MessageDetail struct {
	MessageSummary
	// Payload is the message body
	Payload []byte
	// Metadata contains key-value pairs for message attributes
	Metadata map[string]string
	// FailedAt is epoch milliseconds when the message failed (0 for normal)
	FailedAt int64
	// FailureCount is total failures before DLQ move (0 for normal)
	FailureCount int
	// LastError is the error message from final failure
	LastError string
	// OriginalTopic is where the message originally failed
	OriginalTopic string
}

// OffsetInfo contains consumer group offset information.
type OffsetInfo struct {
	// ConsumerGroup is the consumer group name
	ConsumerGroup string
	// Topic is the topic being consumed
	Topic string
	// PartitionKey is the partition being consumed
	PartitionKey string
	// OffsetAcked is the last successfully acked offset
	OffsetAcked int64
	// UpdatedAt is the epoch milliseconds of the last update
	UpdatedAt int64
}

// LeaseInfo contains partition lease information.
type LeaseInfo struct {
	// ConsumerGroup is the consumer group name
	ConsumerGroup string
	// Topic is the topic being consumed
	Topic string
	// PartitionKey is the partition that is leased
	PartitionKey string
	// LeasedBy is the worker that owns the lease
	LeasedBy string
	// LeasedAt is the epoch milliseconds when the lease was acquired
	LeasedAt int64
	// LeaseRenewedAt is the epoch milliseconds of the last renewal
	LeaseRenewedAt int64
}

// TopicInfo contains a topic name and its message count.
type TopicInfo struct {
	// Topic is the queue topic name
	Topic string
	// MessageCount is the number of messages in this topic
	MessageCount int64
}

// TopicStats contains detailed statistics for a topic.
type TopicStats struct {
	// Topic is the queue topic name
	Topic string
	// TotalMessages is the total number of messages
	TotalMessages int64
	// DLQCount is the number of messages in the DLQ for this topic
	DLQCount int64
	// PartitionCount is the number of distinct partitions
	PartitionCount int64
	// ConsumerGroupCount is the number of consumer groups consuming this topic
	ConsumerGroupCount int64
}

// ListTopics returns all topics with their message counts.
func (s *AdminStore) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	query := fmt.Sprintf(
		"SELECT topic, COUNT(*) FROM %s GROUP BY topic ORDER BY topic",
		mysql.MessagesTableName,
	)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	defer rows.Close()

	var topics []TopicInfo
	for rows.Next() {
		var t TopicInfo
		if err := rows.Scan(&t.Topic, &t.MessageCount); err != nil {
			return nil, fmt.Errorf("scan topic row: %w", err)
		}
		topics = append(topics, t)
	}
	return topics, rows.Err()
}

// GetTopicStats returns detailed statistics for a topic.
func (s *AdminStore) GetTopicStats(ctx context.Context, topic string, dlqSuffix string) (TopicStats, error) {
	stats := TopicStats{Topic: topic}

	// Total messages
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE topic = ?", mysql.MessagesTableName),
		topic,
	).Scan(&stats.TotalMessages)
	if err != nil {
		return stats, fmt.Errorf("count total: %w", err)
	}

	// DLQ count
	dlqTopic := topic + dlqSuffix
	err = s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE topic = ?", mysql.MessagesTableName),
		dlqTopic,
	).Scan(&stats.DLQCount)
	if err != nil {
		return stats, fmt.Errorf("count dlq: %w", err)
	}

	// Distinct partitions
	err = s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(DISTINCT partition_key) FROM %s WHERE topic = ?", mysql.MessagesTableName),
		topic,
	).Scan(&stats.PartitionCount)
	if err != nil {
		return stats, fmt.Errorf("count partitions: %w", err)
	}

	// Consumer groups from offsets
	err = s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(DISTINCT consumer_group) FROM %s WHERE topic = ?", mysql.OffsetsTableName),
		topic,
	).Scan(&stats.ConsumerGroupCount)
	if err != nil {
		return stats, fmt.Errorf("count consumer groups: %w", err)
	}

	return stats, nil
}

// ListMessages returns messages for a topic, optionally filtered by partition.
func (s *AdminStore) ListMessages(ctx context.Context, topic string, partition string, limit int) ([]MessageSummary, error) {
	var rows *sql.Rows
	var err error

	if partition != "" {
		rows, err = s.db.QueryContext(ctx,
			fmt.Sprintf("SELECT `offset`, id, topic, partition_key, created_at, published_at FROM %s WHERE topic = ? AND partition_key = ? ORDER BY `offset` LIMIT ?", mysql.MessagesTableName),
			topic, partition, limit,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			fmt.Sprintf("SELECT `offset`, id, topic, partition_key, created_at, published_at FROM %s WHERE topic = ? ORDER BY `offset` LIMIT ?", mysql.MessagesTableName),
			topic, limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var messages []MessageSummary
	for rows.Next() {
		var m MessageSummary
		if err := rows.Scan(&m.Offset, &m.ID, &m.Topic, &m.PartitionKey, &m.CreatedAt, &m.PublishedAt); err != nil {
			return nil, fmt.Errorf("scan message row: %w", err)
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

// InspectMessage returns full message details including payload and DLQ fields.
func (s *AdminStore) InspectMessage(ctx context.Context, topic string, messageID string) (MessageDetail, bool, error) {
	var d MessageDetail
	var metadataJSON []byte

	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT `offset`, id, topic, partition_key, created_at, published_at, payload, metadata, failed_at, failure_count, last_error, original_topic FROM %s WHERE topic = ? AND id = ?", mysql.MessagesTableName),
		topic, messageID,
	).Scan(&d.Offset, &d.ID, &d.Topic, &d.PartitionKey, &d.CreatedAt, &d.PublishedAt, &d.Payload, &metadataJSON, &d.FailedAt, &d.FailureCount, &d.LastError, &d.OriginalTopic)
	if err == sql.ErrNoRows {
		return d, false, nil
	}
	if err != nil {
		return d, false, fmt.Errorf("inspect message: %w", err)
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &d.Metadata); err != nil {
			return d, false, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}
	if d.Metadata == nil {
		d.Metadata = make(map[string]string)
	}

	return d, true, nil
}

// DeleteMessage deletes a specific message by topic and ID.
func (s *AdminStore) DeleteMessage(ctx context.Context, topic string, messageID string) (int64, error) {
	result, err := s.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE topic = ? AND id = ?", mysql.MessagesTableName),
		topic, messageID,
	)
	if err != nil {
		return 0, fmt.Errorf("delete message: %w", err)
	}
	return result.RowsAffected()
}

// PurgeTopic deletes all messages for a topic.
func (s *AdminStore) PurgeTopic(ctx context.Context, topic string) (int64, error) {
	result, err := s.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE topic = ?", mysql.MessagesTableName),
		topic,
	)
	if err != nil {
		return 0, fmt.Errorf("purge topic: %w", err)
	}
	return result.RowsAffected()
}

// RequeueDLQ moves a message from the DLQ topic back to its original topic.
// This is done transactionally: read from DLQ, insert into original topic, delete from DLQ.
func (s *AdminStore) RequeueDLQ(ctx context.Context, topic string, messageID string, dlqSuffix string) error {
	dlqTopic := topic + dlqSuffix

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Read the DLQ message
	var payload []byte
	var metadataJSON []byte
	var partitionKey string
	var createdAt, publishedAt int64

	err = tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT payload, metadata, partition_key, created_at, published_at FROM %s WHERE topic = ? AND id = ?", mysql.MessagesTableName),
		dlqTopic, messageID,
	).Scan(&payload, &metadataJSON, &partitionKey, &createdAt, &publishedAt)
	if err == sql.ErrNoRows {
		return fmt.Errorf("message %q not found in DLQ topic %q", messageID, dlqTopic)
	}
	if err != nil {
		return fmt.Errorf("read dlq message: %w", err)
	}

	// Insert into original topic with reset fields
	nowMs := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (topic, partition_key, id, payload, metadata, created_at, published_at, failed_at, failure_count, last_error, original_topic) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, '', '')", mysql.MessagesTableName),
		topic, partitionKey, messageID, payload, metadataJSON, createdAt, nowMs,
	)
	if err != nil {
		return fmt.Errorf("insert requeued message: %w", err)
	}

	// Delete from DLQ
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE topic = ? AND id = ?", mysql.MessagesTableName),
		dlqTopic, messageID,
	)
	if err != nil {
		return fmt.Errorf("delete dlq message: %w", err)
	}

	return tx.Commit()
}

// ListOffsets returns consumer group offsets, optionally filtered by group.
func (s *AdminStore) ListOffsets(ctx context.Context, consumerGroup string) ([]OffsetInfo, error) {
	var rows *sql.Rows
	var err error

	if consumerGroup != "" {
		rows, err = s.db.QueryContext(ctx,
			fmt.Sprintf("SELECT consumer_group, topic, partition_key, offset_acked, updated_at FROM %s WHERE consumer_group = ? ORDER BY consumer_group, topic, partition_key", mysql.OffsetsTableName),
			consumerGroup,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			fmt.Sprintf("SELECT consumer_group, topic, partition_key, offset_acked, updated_at FROM %s ORDER BY consumer_group, topic, partition_key", mysql.OffsetsTableName),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	defer rows.Close()

	var offsets []OffsetInfo
	for rows.Next() {
		var o OffsetInfo
		if err := rows.Scan(&o.ConsumerGroup, &o.Topic, &o.PartitionKey, &o.OffsetAcked, &o.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan offset row: %w", err)
		}
		offsets = append(offsets, o)
	}
	return offsets, rows.Err()
}

// ResetOffset updates the acked offset for a consumer group/topic/partition.
func (s *AdminStore) ResetOffset(ctx context.Context, consumerGroup, topic, partition string, offset int64) (int64, error) {
	nowMs := time.Now().UnixMilli()
	result, err := s.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET offset_acked = ?, updated_at = ? WHERE consumer_group = ? AND topic = ? AND partition_key = ?", mysql.OffsetsTableName),
		offset, nowMs, consumerGroup, topic, partition,
	)
	if err != nil {
		return 0, fmt.Errorf("reset offset: %w", err)
	}
	return result.RowsAffected()
}

// ListLeases returns all partition leases.
func (s *AdminStore) ListLeases(ctx context.Context) ([]LeaseInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf("SELECT consumer_group, topic, partition_key, leased_by, leased_at, lease_renewed_at FROM %s ORDER BY consumer_group, topic, partition_key", mysql.PartitionLeasesTableName),
	)
	if err != nil {
		return nil, fmt.Errorf("list leases: %w", err)
	}
	defer rows.Close()

	var leases []LeaseInfo
	for rows.Next() {
		var l LeaseInfo
		if err := rows.Scan(&l.ConsumerGroup, &l.Topic, &l.PartitionKey, &l.LeasedBy, &l.LeasedAt, &l.LeaseRenewedAt); err != nil {
			return nil, fmt.Errorf("scan lease row: %w", err)
		}
		leases = append(leases, l)
	}
	return leases, rows.Err()
}

// LagInfo contains consumer lag information for a single partition.
type LagInfo struct {
	// ConsumerGroup is the consumer group name
	ConsumerGroup string
	// Topic is the topic being consumed
	Topic string
	// PartitionKey is the partition being consumed
	PartitionKey string
	// LatestOffset is the highest message offset in this partition
	LatestOffset int64
	// AckedOffset is the last acked offset for this consumer group
	AckedOffset int64
	// Lag is the number of unprocessed messages (LatestOffset - AckedOffset)
	Lag int64
}

// ConsumerLag returns per-partition lag for each consumer group on a topic.
// Lag = max message offset in partition - consumer group's acked offset.
func (s *AdminStore) ConsumerLag(ctx context.Context, topic string) ([]LagInfo, error) {
	query := fmt.Sprintf(`
		SELECT o.consumer_group, o.topic, o.partition_key, o.offset_acked,
		       COALESCE(m.latest_offset, 0) AS latest_offset
		FROM %s o
		LEFT JOIN (
			SELECT topic, partition_key, MAX(`+"`offset`"+`) AS latest_offset
			FROM %s
			WHERE topic = ?
			GROUP BY topic, partition_key
		) m ON o.topic = m.topic AND o.partition_key = m.partition_key
		WHERE o.topic = ?
		ORDER BY o.consumer_group, o.partition_key`,
		mysql.OffsetsTableName, mysql.MessagesTableName,
	)

	rows, err := s.db.QueryContext(ctx, query, topic, topic)
	if err != nil {
		return nil, fmt.Errorf("consumer lag: %w", err)
	}
	defer rows.Close()

	var results []LagInfo
	for rows.Next() {
		var l LagInfo
		if err := rows.Scan(&l.ConsumerGroup, &l.Topic, &l.PartitionKey, &l.AckedOffset, &l.LatestOffset); err != nil {
			return nil, fmt.Errorf("scan lag row: %w", err)
		}
		l.Lag = l.LatestOffset - l.AckedOffset
		if l.Lag < 0 {
			l.Lag = 0
		}
		results = append(results, l)
	}
	return results, rows.Err()
}

// StaleLeases returns leases whose lease_renewed_at is older than the threshold.
// thresholdMs is the staleness threshold in milliseconds — leases not renewed
// within this duration from now are considered stale.
func (s *AdminStore) StaleLeases(ctx context.Context, thresholdMs int64) ([]LeaseInfo, error) {
	cutoff := time.Now().UnixMilli() - thresholdMs
	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf("SELECT consumer_group, topic, partition_key, leased_by, leased_at, lease_renewed_at FROM %s WHERE lease_renewed_at < ? ORDER BY lease_renewed_at", mysql.PartitionLeasesTableName),
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("stale leases: %w", err)
	}
	defer rows.Close()

	var leases []LeaseInfo
	for rows.Next() {
		var l LeaseInfo
		if err := rows.Scan(&l.ConsumerGroup, &l.Topic, &l.PartitionKey, &l.LeasedBy, &l.LeasedAt, &l.LeaseRenewedAt); err != nil {
			return nil, fmt.Errorf("scan stale lease row: %w", err)
		}
		leases = append(leases, l)
	}
	return leases, rows.Err()
}

// ReleaseLease force-releases a partition lease.
func (s *AdminStore) ReleaseLease(ctx context.Context, consumerGroup, topic, partition string) (int64, error) {
	result, err := s.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE consumer_group = ? AND topic = ? AND partition_key = ?", mysql.PartitionLeasesTableName),
		consumerGroup, topic, partition,
	)
	if err != nil {
		return 0, fmt.Errorf("release lease: %w", err)
	}
	return result.RowsAffected()
}
