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
	// Tenant is the shard isolation identity
	Tenant string
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
	Insert(ctx context.Context, tenant string, topic string, messages []entityqueue.Message) error

	// Delete deletes a message by tenant, topic, partition key, and ID
	Delete(ctx context.Context, tenant string, topic string, partitionKey string, messageID string) error

	// FetchByOffset fetches messages with offset > currentOffset for a specific partition.
	FetchByOffset(ctx context.Context, tenant string, topic string, partitionKey string, currentOffset int64, limit int) ([]messageRow, error)

	// MoveToDLQ moves a message to the dead letter queue
	MoveToDLQ(ctx context.Context, tenant string, topic string, partitionKey string, messageID string, failureCount int, f failure.Failure, dlqTopicSuffix string) error

	// GarbageCollect deletes messages with offset <= minAckedOffset.
	GarbageCollect(ctx context.Context, tenant string, topic string, partitionKey string, minAckedOffset int64) (int64, error)

	// GetOffsetsAbove returns message offsets above afterOffset for a partition,
	// ordered ascending, up to limit rows.
	GetOffsetsAbove(ctx context.Context, tenant string, topic string, partitionKey string, afterOffset int64, limit int) ([]int64, error)
}

// offsetStore handles offset table operations for per-partition offset tracking (internal use only)
type offsetStore interface {
	// Initialize creates an offset entry for a tenant+topic+partition if it doesn't exist
	Initialize(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) error

	// GetAckedOffset returns the current acked offset for a tenant+topic+partition
	GetAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) (int64, error)

	// UpdateAckedOffset updates the offset_acked for a tenant+topic+partition (only if new offset is greater)
	UpdateAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string, offset int64, consumerGroup string) error

	// GetMinAckedOffset returns the minimum offset_acked across all consumer groups
	// for a tenant+topic+partition. Returns (0, false, nil) if no offset rows exist.
	GetMinAckedOffset(ctx context.Context, tenant string, topic string, partitionKey string) (offset int64, found bool, err error)

	// DeleteOffset removes one consumer group's offset row for a partition.
	DeleteOffset(ctx context.Context, tenant string, topic string, partitionKey string, consumerGroup string) error
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
	TryAcquireLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) (bool, error)

	// RenewLease renews the lease for a partition owned by this worker
	RenewLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string, leaseDurationMs int64) error

	// ReleaseLease releases the lease for a partition owned by this worker
	ReleaseLease(ctx context.Context, tenant string, topic string, partitionKey string, subscriberName string, consumerGroup string) error

	// GetLeasedPartitions returns all partitions currently leased by this worker
	GetLeasedPartitions(ctx context.Context, tenant string, topic string, subscriberName string, consumerGroup string) ([]string, error)

	// GetAllLeases returns the lease row for every partition currently leased
	// under (tenant, topic, consumerGroup) by any subscriber.
	GetAllLeases(ctx context.Context, tenant string, topic string, consumerGroup string) ([]leaseInfo, error)

	// PurgeStale deletes lease rows not renewed within olderThanMs.
	PurgeStale(ctx context.Context, tenant string, topic string, consumerGroup string, olderThanMs int64) error

	// DiscoverAndAcquirePartitions discovers partitions from messages table and tries to acquire leases.
	DiscoverAndAcquirePartitions(ctx context.Context, tenant string, topic string, subscriberName string, consumerGroup string, leaseDurationMs int64, maxPartitions int) (acquiredCount int, discoveredPartitions []string, err error)
}

// subscriberHeartbeatStore handles subscriber heartbeat operations for fair partition leasing (internal use only)
type subscriberHeartbeatStore interface {
	// Heartbeat registers or renews a subscriber's heartbeat
	Heartbeat(ctx context.Context, tenant string, topic string, subscriberName string, consumerGroup string) error

	// ActiveSubscribers returns the names of subscribers with a recent heartbeat.
	ActiveSubscribers(ctx context.Context, tenant string, topic string, consumerGroup string, staleDurationMs int64) ([]string, error)

	// Deregister removes a subscriber's heartbeat row.
	Deregister(ctx context.Context, tenant string, topic string, subscriberName string, consumerGroup string) error

	// PurgeStale deletes heartbeat rows whose last heartbeat is older than olderThanMs.
	PurgeStale(ctx context.Context, tenant string, topic string, consumerGroup string, olderThanMs int64) error
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
	MarkDelivered(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) (retryCount int, err error)

	// ExtendVisibility extends the visibility timeout for an in-flight message
	ExtendVisibility(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64, visibilityTimeoutMs int64) error

	// MarkAcked sets acked = TRUE to indicate this group has processed the message.
	MarkAcked(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64) error

	// MarkNacked makes the message eligible for redelivery after delayMs.
	MarkNacked(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) error

	// MarkPostponed sets invisible_until = now + delay, resets retry_count, and sets the postponed flag.
	MarkPostponed(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64, delayMs int64) error

	// GetDeliveryState returns the full delivery state for a message offset.
	GetDeliveryState(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, offset int64) (DeliveryState, bool, error)

	// AdvanceWatermark computes the new contiguous acked watermark and cleans up
	// delivery state rows behind it.
	AdvanceWatermark(ctx context.Context, consumerGroup, tenant, topic, partitionKey string, currentWatermark int64, offsets []int64) (int64, error)
}
