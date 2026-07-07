# SQL Queue Implementation

MySQL-based distributed queue with partition leasing, delivery state tracking, and at-least-once delivery.

For design rationale, guarantees, and trade-offs, see the [RFC](../../../doc/rfc/sql-queue-rfc.md).

## Quick Start

```go
import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
    extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
    entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
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
msg := entityqueue.NewMessage("msg-id", []byte(`{"data": "value"}`), "repo-123", nil)
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
import extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"

subConfig := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")

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
```

**Key Configuration Fields:**

| Field | Description |
|-------|-------------|
| `SubscriberName` | Unique worker identifier for partition leasing (e.g., hostname, pod name) |
| `ConsumerGroup` | Consumer group for independent offset tracking |
| `PollIntervalMs` | How often to poll for new messages |
| `BatchSize` | Maximum messages to fetch per poll. Set to `1` for strict serialization |
| `VisibilityTimeoutMs` | How long messages are invisible after fetch. Must exceed max processing time for `BatchSize=1` |
| `LeaseRenewalIntervalMs` | How often to renew partition leases |
| `LeaseDurationMs` | How long leases remain valid without renewal |
| `Retry.MaxAttempts` | Maximum processing attempts before DLQ |
| `DLQ.TopicSuffix` | Suffix appended to topic name for DLQ (e.g., `"orders"` → `"orders_dlq"`) |

## Package Layout

```
platform/extension/messagequeue/mysql/
├── sql.go                          # NewQueue constructor, wires stores → publisher/subscriber
├── stores.go                       # Internal store interfaces (messageStore, offsetStore, etc.)
├── message_store.go                # queue_messages table operations (immutable log)
├── delivery_state_store.go         # queue_delivery_state table operations (per-consumer-group)
├── offset_store.go                 # queue_offsets table operations (watermark tracking)
├── partition_lease_store.go        # queue_partition_leases table operations
├── subscriber_heartbeat_store.go   # queue_subscriber_heartbeats table operations
├── publisher.go                    # Publisher implementation
├── subscriber.go                   # Subscriber, delivery, goroutine management
├── constants.go                    # Log key constants
├── errors.go                       # Error types
├── schema/                         # SQL schema files (one per table)
│   ├── queue_messages.sql
│   ├── queue_delivery_state.sql
│   ├── queue_offsets.sql
│   ├── queue_partition_leases.sql
│   └── queue_subscriber_heartbeats.sql
└── ctl/                            # Admin CLI (see ctl/README.md)
```

## Internal Architecture

### Database Tables

| Table | Purpose | Scoped To |
|-------|---------|-----------|
| `queue_messages` | Immutable append-only message log | `(topic, partition_key)` — shared across consumer groups |
| `queue_delivery_state` | Visibility, ack state, retry count | `(consumer_group, topic, partition_key, offset)` |
| `queue_offsets` | Contiguous acked watermark | `(consumer_group, topic, partition_key)` |
| `queue_partition_leases` | Partition lease coordination | `(consumer_group, topic, partition_key)` |
| `queue_subscriber_heartbeats` | Active subscriber tracking | `(consumer_group, topic, subscriber_name)` |

`queue_messages` has a `visible_after BIGINT UNSIGNED NOT NULL DEFAULT 0` column that supports `Publisher.PublishAfter`: subscribers' `FetchByOffset` skips rows where `visible_after > now`. Default 0 means immediately visible, so existing rows continue to behave as before — the column is back-compatible.

See `schema/` for full SQL definitions. See the [RFC](../../../doc/rfc/sql-queue-rfc.md#database-schema) for field-level documentation.

### Store Architecture

Each table is backed by an internal store interface defined in `stores.go`. Stores:
- Query only their own table (no cross-table JOINs)
- Return errors via `fmt.Errorf` (no logging, no error classification)
- Use `metrics.Begin`/`Complete` for latency and success/failure tracking

The subscriber layer orchestrates cross-store operations (e.g., watermark advancement queries both `messageStore` and `deliveryStateStore`) and owns all logging and error classification.

### Goroutine Model

Each subscription has a **supervisor goroutine** (`managePartitions`) that discovers partitions, acquires leases, sends heartbeats, rebalances, and reconciles per-partition worker goroutines.

```
Subscribe()
  └── managePartitions (supervisor)       ← tracked by sub.wg
        ├── partitionWorker("part-1")     ← tracked by sub.workerWg
        ├── partitionWorker("part-2")
        └── partitionWorker("part-3")
```

Each partition worker runs independently — polls the DB on a ticker, checks deliverability via `GetDeliveryState` per message, and sends deliveries to the shared channel. A slow or blocked partition does not affect other partitions.

### Shutdown Sequence

When `Close()` is called:
1. Subscription context is cancelled
2. `managePartitions` calls `stopAllWorkers` — cancels each worker's context, waits up to 30s
3. Partition leases are released (fresh context, not cancelled)
4. Subscriber heartbeat is deregistered
5. `workerWg.Wait()` — blocks until all workers have fully exited
6. `deliveryCh` is closed — safe because no senders remain after step 5
7. `managePartitions` returns → `wg.Done()` → `Close()` unblocks

The `workerWg.Wait()` before `close(deliveryCh)` prevents a race where a slow worker could send on a closed channel.

### Worker Stop Behavior

When a partition worker is stopped (lease lost or shutdown):
- Immediately removed from workers map and context cancelled
- Caller waits up to 30s for exit confirmation (warning logged on timeout)
- `workerWg` tracks the goroutine regardless — `Close()` always waits for full exit
- Reconciliation can start a replacement immediately — brief overlap is harmless with at-least-once semantics

### Logger Hierarchy

`sql.go` creates a root `queue_mysql` logger and passes named children to each component:

```
queue_mysql
  ├── .publisher
  ├── .subscriber
  ├── .message_store
  ├── .delivery_state_store
  ├── .offset_store
  ├── .partition_lease_store
  └── .subscriber_heartbeat_store
```

Stores do not log errors — they return them. The subscriber propagates all errors to the top call site (`managePartitions` or `run`), which logs once with full context (`topic`, `consumer_group`, `subscriber_name`).

## Testing

### Unit Tests

```bash
bazel test //platform/extension/messagequeue/mysql:mysql_test --test_output=streamed
bazel test //platform/extension/messagequeue/mysql/ctl/...:all --test_output=streamed
```

### Integration Tests

Requires Docker running:

```bash
bazel test //test/integration/extension/messagequeue/... --test_output=streamed
```

Integration tests cover: publish/subscribe, partition isolation, ordering, visibility timeout, nack with delay, idempotent publish, concurrent publishers, crash recovery, multiple consumer groups, rebalancing, DLQ, graceful shutdown, non-blocking nack, strict serialization (`BatchSize=1`), and independent consumer group state.
