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
    queueSQL "github.com/uber/submitqueue/extensions/queue/sql"
    "github.com/uber/submitqueue/entities/queue"
)

// Setup
db, _ := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db")
q, _ := queueSQL.NewQueue(queueSQL.Params{
    DB:     db,
    Logger: logger,
    Config: queueSQL.DefaultConfig("orchestrator", "worker-1"),
})
defer q.Close()

// Publish
msg := queue.NewMessage("msg-id", []byte(`{"data": "value"}`))
msg.PartitionKey = "repo-123"  // Required for ordering
q.Publisher().Publish(ctx, "merge_events", msg)

// Subscribe
deliveryCh, _ := q.Subscriber().Subscribe(ctx, "merge_events")
for delivery := range deliveryCh {
    if err := process(delivery.Message()); err != nil {
        delivery.Nack(ctx, 0)  // Retry
        continue
    }
    delivery.Ack(ctx)
}
```

## Configuration

```go
config := queueSQL.DefaultConfig("consumer-group", "worker-id")
config.PollInterval = 50 * time.Millisecond        // Poll frequency
config.BatchSize = 20                              // Messages per poll
config.VisibilityTimeout = 60 * time.Second        // Retry delay
config.Retry.MaxAttempts = 3                       // Max retries before DLQ
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
