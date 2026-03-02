# SQL Queue Implementation

MySQL-based distributed queue with partition leasing, visibility timeout, and at-least-once delivery.

## Key Features

- **Partition leasing** — workers coordinate via database leases with automatic failover
- **Per-partition workers** — each leased partition gets its own goroutine for isolation
- **Visibility timeout** — messages retry automatically if worker crashes
- **At-least-once delivery** — offset tracking for crash recovery
- **Dead letter queue** — failed messages moved to DLQ after max retries

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    queueMySQL "github.com/uber/submitqueue/extension/queue/mysql"
    extqueue "github.com/uber/submitqueue/extension/queue"
    "github.com/uber/submitqueue/entity/queue"
)

// Setup
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db")
q, _ := queueMySQL.NewQueue(queueMySQL.Params{
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
- `DLQ.TopicSuffix`: Suffix appended to topic name for DLQ (e.g., "orders" -> "orders_dlq")

## Architecture

### Goroutine Model

Each subscription has a **supervisor goroutine** (`managePartitions`) that:
1. Discovers partitions from the messages table
2. Acquires and renews partition leases
3. Reconciles **per-partition worker goroutines** based on current leases

Each partition worker goroutine polls and delivers messages for its partition independently. This provides fault isolation — a slow or blocked partition does not affect other partitions.

```
Subscribe()
  └── managePartitions (supervisor)
        ├── partitionWorker("part-1")  ← polls & delivers
        ├── partitionWorker("part-2")  ← polls & delivers
        └── partitionWorker("part-3")  ← polls & delivers
```

### Shutdown Sequence

Shutdown uses two `sync.WaitGroup`s to ensure correctness:
- `wg` tracks the supervisor goroutine (`managePartitions`)
- `workerWg` tracks all partition worker goroutines

When `Close()` is called:
1. Subscription context is cancelled
2. `managePartitions` calls `stopAllWorkers` — cancels each worker and waits up to 5s per worker
3. Partition leases are released
4. `workerWg.Wait()` blocks until all workers have fully exited
5. `deliveryCh` is closed — safe because no workers can send after step 4
6. `managePartitions` returns, `wg.Done()` fires
7. `Close()` returns

The `workerWg.Wait()` step prevents a race where a slow worker (blocked on I/O past the 5s timeout) could send on a closed channel.

### Worker Stop Behavior

When a partition worker is stopped (lease lost or shutdown):
- The worker is immediately removed from the workers map and its context is cancelled
- The caller waits up to 5s for the worker to confirm exit (logging a warning on timeout)
- `workerWg` tracks the worker regardless, so `Close()` always waits for full exit
- If the worker times out, reconciliation is free to start a replacement — any brief overlap is harmless with at-least-once delivery semantics

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
