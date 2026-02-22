# SQL Queue Implementation

MySQL-based distributed queue with partition leasing, visibility timeout, and at-least-once delivery.

## Key Features

- **Partition leasing** - Workers coordinate via database leases with automatic failover
- **Visibility timeout** - Messages retry automatically if worker crashes
- **At-least-once delivery** - Offset tracking for crash recovery
- **Dead letter queue** - Failed messages moved to DLQ after max retries

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    queueSQL "github.com/uber/submitqueue/extension/queue/sql"
    extqueue "github.com/uber/submitqueue/extension/queue"
    "github.com/uber/submitqueue/entity/queue"
)

// Setup
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db")
q, _ := queueSQL.NewQueue(queueSQL.Params{
    DB:           db,
    Logger:       logger,
    MetricsScope: metrics,
})
defer q.Close()

// Publish
msg := queue.NewMessage("msg-id", []byte(`{"data": "value"}`), "repo-123", nil)
q.Publisher().Publish(ctx, "merge_events", msg)

// Subscribe with per-subscription config
subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "orchestrator")
deliveryCh, _ := q.Subscriber().Subscribe(ctx, "merge_events", subConfig)
for delivery := range deliveryCh {
    if err := process(delivery.Message()); err != nil {
        delivery.Nack(ctx, 0)  // Retry
        continue
    }
    delivery.Ack(ctx)
}
```

## Configuration

Per-subscription configuration enables different settings for each topic:

```go
import extqueue "github.com/uber/submitqueue/extension/queue"

// Default config
subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")

// Customize for this subscription
subConfig.PollIntervalMs = 50                         // Poll frequency (milliseconds)
subConfig.BatchSize = 20                              // Messages per poll
subConfig.VisibilityTimeoutMs = 60000                 // Retry delay (milliseconds)
subConfig.LeaseRenewalIntervalMs = 10000              // Lease renewal frequency (milliseconds)
subConfig.LeaseDurationMs = 30000                     // Lease timeout (milliseconds)
subConfig.Retry.MaxAttempts = 3                       // Max retries before DLQ
subConfig.Retry.InitialBackoffMs = 1000               // Initial retry backoff (milliseconds)
subConfig.Retry.MaxBackoffMs = 30000                  // Max retry backoff (milliseconds)
subConfig.Retry.BackoffMultiplier = 2.0               // Backoff multiplier for exponential backoff
subConfig.DLQ.Enabled = true                          // Enable dead letter queue
subConfig.DLQ.TopicSuffix = "_dlq"                    // DLQ topic suffix

// Use config when subscribing
deliveryCh, _ := q.Subscriber().Subscribe(ctx, "my-topic", subConfig)
```

**Key Configuration Fields:**

- `SubscriberName`: Unique worker identifier for partition leasing (e.g., hostname, pod name)
- `ConsumerGroup`: Consumer group for independent offset tracking
- `PollIntervalMs`: How often to poll for new messages (milliseconds)
- `BatchSize`: Maximum messages to fetch per poll
- `VisibilityTimeoutMs`: How long messages are invisible after being fetched (milliseconds)
- `LeaseRenewalIntervalMs`: How often to renew partition leases (milliseconds)
- `LeaseDurationMs`: How long leases remain valid without renewal (milliseconds)
- `Retry.MaxAttempts`: Maximum processing attempts before moving to DLQ
- `Retry.InitialBackoffMs`: Initial retry backoff delay (milliseconds)
- `Retry.MaxBackoffMs`: Maximum retry backoff delay (milliseconds)
- `Retry.BackoffMultiplier`: Multiplier for exponential backoff
- `DLQ.TopicSuffix`: Suffix appended to topic name for DLQ (e.g., "orders" → "orders_dlq")
```

## How It Works

**Partition Leasing:**
1. Workers discover partitions from messages table
2. Workers acquire leases (one worker per partition)
3. Stale leases can be stolen by other workers

**Message Flow:**
1. Fetch visible messages (invisible_until <= now)
2. Process message
3. Ack: DELETE message, UPDATE offset
4. Nack: Message becomes visible after timeout
5. If retry_count >= MaxAttempts: Move to DLQ

**Crash Recovery:**
- Messages become visible after visibility timeout
- Other workers steal stale leases
- Resume from last acked offset

## Partition Ordering

Messages with same `PartitionKey` are processed in order by a single worker.

## Distributed Processing

Multiple workers in the same consumer group share partitions. Workers in different consumer groups consume independently.
