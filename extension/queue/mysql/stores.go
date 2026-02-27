package mysql

//go:generate mockgen -source=stores.go -destination=mock_stores.go -package=mysql

import (
	"context"

	"github.com/uber/submitqueue/entity/queue"
)

const (
	// Fixed table names for single-table design
	MessagesTableName        = "queue_messages"
	PartitionLeasesTableName = "queue_partition_leases"
	OffsetsTableName         = "queue_offsets"
	DLQTableName             = "queue_dlq"
)

// messageRow represents a row from the messages table (internal use only)
type messageRow struct {
	// Offset is the auto-incrementing sequence number for message ordering within a partition
	Offset int64
	// ID is the unique message identifier
	ID string
	// Payload is the message body in bytes
	Payload []byte
	// Metadata contains key-value pairs for message attributes
	Metadata map[string]string
	// PartitionKey determines which partition this message belongs to for ordering guarantees
	PartitionKey string
	// RetryCount tracks how many times this message has been retried on the current topic
	RetryCount int
	// PublishedAt is the Unix timestamp in milliseconds when message was published
	PublishedAt int64
	// FailedAt is the Unix timestamp in milliseconds when the message failed (0 for normal messages, >0 for DLQ)
	FailedAt int64
	// FailureCount tracks total failures before moving to DLQ (0 for normal messages, >0 for DLQ)
	FailureCount int
	// LastError contains the error message from the final failure ("" for normal messages)
	LastError string
	// OriginalTopic is the topic where the message originally failed ("" for normal messages)
	OriginalTopic string
}

// messageStore handles message table operations (internal use only)
type messageStore interface {
	// Insert inserts messages into the topic table
	Insert(ctx context.Context, topic string, messages []queue.Message) error

	// Delete deletes a message by ID
	Delete(ctx context.Context, topic string, messageID string) error

	// FetchByOffset fetches messages with offset > currentOffset for a specific partition
	// Only fetches visible messages (invisible_until <= now)
	// Atomically sets invisible_until and increments retry_count
	// visibilityTimeoutMs specifies how long messages should be invisible after fetching (in milliseconds)
	FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, limit int, visibilityTimeoutMs int64) ([]messageRow, error)

	// MoveToDLQ moves a message to the dead letter queue
	// dlqTopicSuffix is appended to the original topic to form the DLQ topic name
	MoveToDLQ(ctx context.Context, topic string, messageID string, failureCount int, lastError string, dlqTopicSuffix string) error

	// SetVisibilityTimeout sets the invisible_until timestamp for a message
	// visibilityTimeoutMillis: milliseconds from now to hide the message
	// If visibilityTimeoutMillis is 0, makes the message visible immediately
	// If visibilityTimeoutMillis > 0, makes the message invisible until now + visibilityTimeoutMillis
	SetVisibilityTimeout(ctx context.Context, topic string, messageID string, visibilityTimeoutMillis int64) error
}

// offsetStore handles offset table operations for per-partition offset tracking (internal use only)
type offsetStore interface {
	// Initialize creates an offset entry for a topic+partition if it doesn't exist
	Initialize(ctx context.Context, topic string, partitionKey string, consumerGroup string) error

	// GetAckedOffset returns the current acked offset for a topic+partition
	GetAckedOffset(ctx context.Context, topic string, partitionKey string, consumerGroup string) (int64, error)

	// UpdateAckedOffset updates the offset_acked for a topic+partition (only if new offset is greater)
	UpdateAckedOffset(ctx context.Context, topic string, partitionKey string, offset int64, consumerGroup string) error

	// AckMessage atomically deletes a message and updates the acked offset
	AckMessage(ctx context.Context, topic string, partitionKey string, messageID string, offset int64, consumerGroup string, messageStore messageStore) error
}

// partitionLeaseStore handles partition lease operations (internal use only)
type partitionLeaseStore interface {
	// TryAcquireLease attempts to acquire or renew a lease for a partition
	// Returns true if lease is acquired/owned by this worker
	// leaseDurationMs is how long the lease is valid (in milliseconds)
	TryAcquireLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (bool, error)

	// RenewLease renews the lease for a partition owned by this worker
	// leaseDurationMs is how long the lease is valid (in milliseconds)
	RenewLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) error

	// ReleaseLease releases the lease for a partition owned by this worker
	ReleaseLease(ctx context.Context, topic string, partitionKey string, subscriberName string, consumerGroup string) error

	// GetLeasedPartitions returns all partitions currently leased by this worker
	GetLeasedPartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string) ([]string, error)

	// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases
	// Returns the number of new leases acquired
	// leaseDurationMs is how long the lease is valid (in milliseconds)
	DiscoverAndAcquirePartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string, leaseDurationMs int64) (int, error)
}
