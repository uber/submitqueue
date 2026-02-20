package sql

//go:generate mockgen -source=stores.go -destination=mock_stores.go -package=sql

import (
	"context"

	"github.com/uber/submitqueue/entities/queue"
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
	FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, limit int) ([]messageRow, error)

	// MoveToDLQ moves a message to the dead letter queue
	MoveToDLQ(ctx context.Context, topic string, messageID string, failureCount int, lastError string) error

	// SetVisibilityTimeout sets the invisible_until timestamp for a message
	// visibilityTimeoutMillis: milliseconds from now to hide the message
	// If visibilityTimeoutMillis is 0, makes the message visible immediately
	// If visibilityTimeoutMillis > 0, makes the message invisible until now + visibilityTimeoutMillis
	SetVisibilityTimeout(ctx context.Context, topic string, messageID string, visibilityTimeoutMillis int64) error
}

// offsetStore handles offset table operations for per-partition offset tracking (internal use only)
type offsetStore interface {
	// Initialize creates an offset entry for a topic+partition if it doesn't exist
	Initialize(ctx context.Context, topic string, partitionKey string) error

	// GetAckedOffset returns the current acked offset for a topic+partition
	GetAckedOffset(ctx context.Context, topic string, partitionKey string) (int64, error)

	// UpdateAckedOffset updates the offset_acked for a topic+partition (only if new offset is greater)
	UpdateAckedOffset(ctx context.Context, topic string, partitionKey string, offset int64) error

	// AckMessage atomically deletes a message and updates the acked offset
	AckMessage(ctx context.Context, topic string, partitionKey string, messageID string, offset int64, messageStore messageStore) error
}

// partitionLeaseStore handles partition lease operations (internal use only)
type partitionLeaseStore interface {
	// TryAcquireLease attempts to acquire or renew a lease for a partition
	// Returns true if lease is acquired/owned by this worker
	TryAcquireLease(ctx context.Context, topic string, partitionKey string) (bool, error)

	// RenewLease renews the lease for a partition owned by this worker
	RenewLease(ctx context.Context, topic string, partitionKey string) error

	// ReleaseLease releases the lease for a partition owned by this worker
	ReleaseLease(ctx context.Context, topic string, partitionKey string) error

	// GetLeasedPartitions returns all partitions currently leased by this worker
	GetLeasedPartitions(ctx context.Context, topic string) ([]string, error)

	// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases
	// Returns the number of new leases acquired
	DiscoverAndAcquirePartitions(ctx context.Context, topic string) (int, error)
}
