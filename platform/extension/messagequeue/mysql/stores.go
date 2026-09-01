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

//go:generate mockgen -source=stores.go -destination=mock_stores.go -package=mysql

import (
	"context"

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
)

const (
	// Fixed table names for single-table design
	MessagesTableName             = "queue_messages"
	PartitionLeasesTableName      = "queue_partition_leases"
	OffsetsTableName              = "queue_offsets"
	SubscriberHeartbeatsTableName = "queue_subscriber_heartbeats"
	DeliveryStateTableName        = "queue_delivery_state"
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
	// FailureDetail is the encoded structured half of the failure — its
	// subjects and free-form context. Empty for normal messages, and for a DLQ
	// message whose failure recorded no structure.
	FailureDetail []byte
}

// messageStore handles message table operations (internal use only)
type messageStore interface {
	// Insert inserts messages into the topic table.
	Insert(ctx context.Context, topic string, messages []entityqueue.Message) error

	// Delete deletes a message by topic, partition key, and ID
	Delete(ctx context.Context, topic string, partitionKey string, messageID string) error

	// FetchByOffset fetches messages with offset > currentOffset for a specific partition.
	// Per-consumer-group visibility is handled by the deliveryStateStore.
	FetchByOffset(ctx context.Context, topic string, partitionKey string, currentOffset int64, limit int) ([]messageRow, error)

	// MoveToDLQ moves a message to the dead letter queue
	// dlqTopicSuffix is appended to the original topic to form the DLQ topic name
	// f is split across the row: its message into last_error, its structured
	// half into failure_detail.
	MoveToDLQ(ctx context.Context, topic string, partitionKey string, messageID string, failureCount int, f failure.Failure, dlqTopicSuffix string) error

	// GarbageCollect deletes messages with offset <= minAckedOffset.
	// The caller (subscriber) is responsible for computing minAckedOffset from the
	// offsetStore, keeping messageStore free of cross-table queries.
	// Returns the number of rows deleted.
	GarbageCollect(ctx context.Context, topic string, partitionKey string, minAckedOffset int64) (int64, error)

	// GetOffsetsAbove returns message offsets above afterOffset for a partition,
	// ordered ascending, up to limit rows. Used by the subscriber to drive
	// watermark advancement without requiring a cross-table JOIN in the delivery
	// state store. Watermark advancement is incremental and idempotent, so
	// limiting the result set is safe — it converges over multiple calls.
	GetOffsetsAbove(ctx context.Context, topic string, partitionKey string, afterOffset int64, limit int) ([]int64, error)
}

// offsetStore handles offset table operations for per-partition offset tracking (internal use only)
type offsetStore interface {
	// Initialize creates an offset entry for a topic+partition if it doesn't exist
	Initialize(ctx context.Context, topic string, partitionKey string, consumerGroup string) error

	// GetAckedOffset returns the current acked offset for a topic+partition
	GetAckedOffset(ctx context.Context, topic string, partitionKey string, consumerGroup string) (int64, error)

	// UpdateAckedOffset updates the offset_acked for a topic+partition (only if new offset is greater)
	UpdateAckedOffset(ctx context.Context, topic string, partitionKey string, offset int64, consumerGroup string) error

	// GetMinAckedOffset returns the minimum offset_acked across all consumer groups
	// for a topic+partition. Returns (0, false, nil) if no offset rows exist.
	// Used by the subscriber to compute the GC threshold without messageStore
	// needing to query the offsets table.
	GetMinAckedOffset(ctx context.Context, topic string, partitionKey string) (offset int64, found bool, err error)

	// DeleteOffset removes one consumer group's offset row for a partition.
	// Callers use this when retiring a fully-drained partition; Initialize
	// recreates the row if the partition ever receives messages again.
	// Idempotent: no-op if the row is already gone.
	DeleteOffset(ctx context.Context, topic string, partitionKey string, consumerGroup string) error
}

// leaseInfo describes one partition's current lease row (internal use only)
type leaseInfo struct {
	// PartitionKey is the partition this lease covers
	PartitionKey string
	// LeasedBy is the subscriber name currently holding the lease
	LeasedBy string
	// LeaseRenewedAt is the epoch milliseconds of the last renewal; a lease
	// is stale (stealable) once this is older than the lease duration
	LeaseRenewedAt int64
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

	// GetAllLeases returns the lease row for every partition currently leased
	// under (topic, consumerGroup) by any subscriber. One PK-prefix read that
	// lets acquisition skip partitions validly held by other subscribers
	// instead of write-probing every lease row each discovery tick.
	GetAllLeases(ctx context.Context, topic string, consumerGroup string) ([]leaseInfo, error)

	// PurgeStale deletes lease rows not renewed within olderThanMs. Backstop
	// for holders that crashed while owning a drained partition: acquisition
	// only probes discovered partitions, so a stale lease on a partition
	// with no messages is otherwise never refreshed or removed. Deleting a
	// stale row is equivalent to lease expiry — a concurrent renewal makes
	// the row fresh and the age predicate skips it.
	PurgeStale(ctx context.Context, topic string, consumerGroup string, olderThanMs int64) error

	// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases.
	// Returns the number of new leases acquired and the full list of discovered partitions.
	// leaseDurationMs is how long the lease is valid (in milliseconds)
	// maxPartitions limits how many total partitions this subscriber can own (0 = unlimited)
	DiscoverAndAcquirePartitions(ctx context.Context, topic string, subscriberName string, consumerGroup string, leaseDurationMs int64, maxPartitions int) (acquiredCount int, discoveredPartitions []string, err error)
}

// subscriberHeartbeatStore handles subscriber heartbeat operations for fair partition leasing (internal use only)
type subscriberHeartbeatStore interface {
	// Heartbeat registers or renews a subscriber's heartbeat
	Heartbeat(ctx context.Context, topic string, subscriberName string, consumerGroup string) error

	// ActiveSubscribers returns the names of subscribers with a recent heartbeat.
	// staleDurationMs defines the staleness threshold: subscribers without a heartbeat
	// within this duration are considered dead.
	ActiveSubscribers(ctx context.Context, topic string, consumerGroup string, staleDurationMs int64) ([]string, error)

	// Deregister removes a subscriber's heartbeat row. Hard delete: the row
	// is not needed once the subscriber is gone, and subscriber names are
	// unique per process (hostname-pid), so rows would otherwise accumulate
	// forever across deploys. Re-subscribing re-inserts via Heartbeat.
	Deregister(ctx context.Context, topic string, subscriberName string, consumerGroup string) error

	// PurgeStale deletes heartbeat rows whose last heartbeat is older than
	// olderThanMs. Backstop for subscribers that never deregistered
	// (crashes, SIGKILL): without it the table grows monotonically since
	// every process registers under a fresh name. Deleting a live-but-stalled
	// subscriber's row is harmless — its next heartbeat re-inserts it.
	PurgeStale(ctx context.Context, topic string, consumerGroup string, olderThanMs int64) error
}

// DeliveryState represents the full per-message delivery tracking state.
type DeliveryState struct {
	// Acked indicates whether this consumer group has processed the message
	Acked bool
	// InvisibleUntil is the epoch milliseconds until which the message is hidden
	InvisibleUntil int64
	// RetryCount tracks how many times the message has been delivered
	RetryCount int
	// Postponed indicates the last delivery was postponed (a deliberate wait,
	// not a failure). While set and invisible, the message is a partition
	// barrier and its next delivery is exempt from the retry_count increment.
	Postponed bool
}

// deliveryStateStore handles per-consumer-group delivery tracking (internal use only)
type deliveryStateStore interface {
	// MarkDelivered inserts a row marking message as in-flight for this consumer group.
	// Increments retry_count on redelivery (ON DUPLICATE KEY UPDATE), except when the
	// row is marked postponed — that delivery is exempt and clears the postponed flag.
	// Returns the resulting retry_count after the operation.
	MarkDelivered(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (retryCount int, err error)

	// ExtendVisibility extends the visibility timeout for an in-flight message
	// without incrementing retry_count.
	ExtendVisibility(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) error

	// MarkAcked sets acked = TRUE to indicate this group has processed the message.
	MarkAcked(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64) error

	// MarkNacked makes the message eligible for redelivery after delayMs.
	MarkNacked(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, delayMs int64) error

	// MarkPostponed sets invisible_until = now + delay, resets retry_count, and
	// sets the postponed flag. The message becomes a partition barrier until it
	// redelivers, and the redelivery does not count as a failure.
	MarkPostponed(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64, delayMs int64) error

	// GetDeliveryState returns the full delivery state for a message offset.
	// Returns (state, found, error). found=false means no row (never delivered).
	GetDeliveryState(ctx context.Context, consumerGroup, topic, partitionKey string, offset int64) (DeliveryState, bool, error)

	// AdvanceWatermark computes the new contiguous acked watermark and cleans up
	// delivery state rows behind it.
	// offsets are the actual message offsets above the current watermark (from messageStore).
	// Returns the new watermark (highest contiguous acked offset from currentWatermark).
	AdvanceWatermark(ctx context.Context, consumerGroup, topic, partitionKey string, currentWatermark int64, offsets []int64) (int64, error)
}
