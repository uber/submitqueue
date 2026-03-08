# RFC: SQL-Based Distributed Queue

## Metadata

| Field | Value                            |
|-------|----------------------------------|
| **Author** | Preetam Dwived<preetam@uber.com> |
| **Status** | In Review                        |
| **Created** | 2026-02-16                       |
| **Updated** | 2026-03-08                       |

## Summary

MySQL-based distributed message queue with immutable message log, per-consumer-group delivery state, partition leasing, and at-least-once delivery. Workers coordinate via database-native primitives without external systems.

## Background

### Motivation

SubmitQueue needs a reliable message queue for coordinating asynchronous workflows:
- **Orchestrator** publishes merge jobs and speculative build requests to workers
- **Workers** need distributed coordination without duplicate processing
- **Crash recovery** must preserve exactly where processing stopped

### Existing Solutions

We evaluated several approaches:

1. **External Message Brokers** (Kafka, RabbitMQ)
   - Additional operational overhead and infrastructure
   - Network hops increase latency
   - Battle-tested and highly scalable

2. **Watermill Library** (github.com/ThreeDotsLabs/watermill)
   - Database-backed queue with mature abstractions
   - Built-in middleware (retry, poison queue, metrics)
   - Generic interface hides database-specific optimizations
   - Additional dependency and learning curve
   - Less control over exact SQL queries and behavior

3. **dbqueue-go** (github.com/yunussandikci/dbqueue-go)
   - Lightweight, simple FIFO queue over SQL (MySQL, PostgreSQL, SQLite)
   - Basic features: priority, deduplication, visibility timeout
   - No distributed worker coordination or partition leasing
   - No built-in retry mechanism or DLQ
   - Designed for single-worker scenarios, not multi-worker distribution

4. **Database-Backed Queue** (Custom implementation)
   - Reuses existing MySQL infrastructure
   - Full control over queries and behavior
   - No additional services or dependencies
   - More code to maintain

### Decision

We chose **custom database-backed queue** because:
- Full control over SQL queries for optimal performance
- No additional libraries - direct use of database/sql
- Simpler to understand and debug (no abstraction layers)
- Can optimize for our specific use case (partition ordering, delivery state tracking)
- Watermill adds valuable abstractions but we need fine-grained control

## Requirements

### Functional Requirements

1. **Publish/Subscribe** - Standard pub/sub with topics
2. **Partitioning** - Messages with same key processed in order by single worker
3. **At-Least-Once Delivery** - Guaranteed delivery, duplicates possible
4. **Crash Recovery** - Workers resume from last committed offset
5. **Distributed Workers** - Multiple workers coordinate without duplicate processing
6. **Dead Letter Queue** - Failed messages isolated after max retries
7. **Delivery State Tracking** - Per-consumer-group visibility, retry count, and ack state
8. **Multi-Consumer-Group** - Independent consumption of the same message log

### Non-Functional Requirements

1. **Operational Simplicity** - No additional infrastructure
2. **Observability** - Metrics and logging for debugging
3. **Testability** - In-memory testing without external MySQL
4. **Performance** - Sub-second latency for typical workloads
5. **Scalability** - Handle hundreds of workers, thousands of partitions

### Non-Goals

1. **Exactly-Once Delivery** - Application must handle duplicates
2. **Kafka-Scale Throughput** - Not optimizing for millions of messages/sec
3. **Cross-Datacenter Replication** - Single MySQL instance only
4. **Message Ordering Across Partitions** - Only within partition
5. **Real-Time Streaming** - Polling introduces configurable latency

## Design Overview

### High-Level Architecture

```
┌─────────────┐
│  Publisher  │───┐
└─────────────┘   │
                  ▼
┌─────────────────────────────────────┐
│          MySQL Database             │
│  ┌───────────────────────────────┐  │
│  │     queue_messages            │  │
│  │  (immutable append-only log)  │  │
│  │  - topic, partition_key       │  │
│  │  - offset, payload            │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │  queue_delivery_state         │  │
│  │  (per-consumer-group)         │  │
│  │  - invisible_until, retry     │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │  queue_partition_leases       │  │
│  │  - consumer_group, topic      │  │
│  │  - partition_key, leased_by   │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │     queue_offsets             │  │
│  │  - consumer_group, topic      │  │
│  │  - partition_key, offset      │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │ queue_subscriber_heartbeats   │  │
│  │  - consumer_group, topic      │  │
│  │  - subscriber_name, heartbeat │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│         Subscriber Workers          │
│  ┌──────────┐  ┌──────────┐         │
│  │ Worker-1 │  │ Worker-2 │  ...    │
│  │(part-A,B)│  │(part-C,D)│         │
│  └──────────┘  └──────────┘         │
│  ┌──────────┐  ┌──────────┐         │
│  │ Worker-3 │  │ Worker-N │         │
│  │(part-E,F)│  │(part-X,Y)│         │
│  └──────────┘  └──────────┘         │
└─────────────────────────────────────┘
```

### Core Concepts

**Immutable Message Log:** Messages are append-only in `queue_messages`. No per-message mutation occurs during delivery — the log is shared across all consumer groups.

**Delivery State Tracking:** Per-consumer-group delivery state in `queue_delivery_state` tracks ack state, visibility timeout, and retry count independently. Each row has an explicit `acked` boolean and an `invisible_until` timestamp. When `acked = TRUE`, the message is done and never redelivered. When `acked = FALSE`, `invisible_until` controls visibility: past/zero = ready for delivery, future = in-flight or nack delay.

**Watermark-Based Offset:** On ack, the subscriber computes the contiguous acked watermark by scanning forward from the current `offset_acked` through delivery state. The watermark advances to the highest contiguous acked offset, and delivery state rows behind it are cleaned up.

**Partition Leasing:** Workers coordinate using database-native leases. Each partition leased by exactly one worker. Stale leases automatically stolen on crash.

**Fair Partition Distribution:** Subscribers send periodic heartbeats. Each subscriber calculates `ceil(totalPartitions / activeSubscribers)` to cap lease acquisitions and releases excess partitions during rebalance.

**Persistent Retry Tracking:** `retry_count` incremented atomically on delivery via `ON DUPLICATE KEY UPDATE`, survives crashes, triggers DLQ after `MaxAttempts`.

## Database Schema

### Messages Table (Immutable Log)

Messages are append-only. No per-message mutation during delivery.

**Key Fields:**
- `offset` (PK): Auto-incrementing global offset for message ordering
- `topic`, `partition_key`: Message routing and partitioning
- `id`: Unique message identifier
- `payload`, `metadata`: Message content
- `failed_at`, `failure_count`, `last_error`, `original_topic`: DLQ-specific fields (zero values for normal messages, populated when message is moved to DLQ topic)
- `created_at`, `published_at`: Timestamps

**Indexes:**
- `(topic, partition_key, offset)`: Core fetch query — poll messages in partition ordered by offset
- `(topic, partition_key, id)`: Unique constraint and idempotent publish

See `extension/queue/mysql/schema/queue_messages.sql` for full schema.

### Delivery State Table

Per-consumer-group delivery tracking with explicit ack state.

**Key Fields:**
- `consumer_group`, `topic`, `partition_key`, `message_offset` (PK): Identifies delivery state per consumer group per message
- `acked`: Whether this consumer group has successfully processed this message
- `invisible_until`: Visibility timeout in epoch milliseconds (only meaningful when `acked = FALSE`)
- `retry_count`: Number of times message has been redelivered to this consumer group

See `extension/queue/mysql/schema/queue_delivery_state.sql` for full schema.

### Partition Leases Table

**Key Fields:**
- `consumer_group`, `topic`, `partition_key` (PK): Identifies which partition is leased
- `leased_by`: Worker that owns the lease
- `leased_at`, `lease_renewed_at`: Lease timestamps for staleness detection

**Indexes:**
- `(leased_by)`: Find all partitions owned by a worker
- `(lease_renewed_at)`: Detect stale leases across workers

See `extension/queue/mysql/schema/queue_partition_leases.sql` for full schema.

### Consumer Offsets Table

**Key Fields:**
- `consumer_group`, `topic`, `partition_key` (PK): Identifies offset position
- `offset_acked`: Contiguous acked watermark — all messages at or below this offset are fully processed
- `updated_at`: Last update timestamp

**Indexes:**
- `(consumer_group)`: Monitor all offsets for a consumer group
- `(topic)`: Find all consumers for a topic

See `extension/queue/mysql/schema/queue_offsets.sql` for full schema.

### Subscriber Heartbeats Table

**Key Fields:**
- `consumer_group`, `topic`, `subscriber_name` (PK): Identifies the subscriber
- `heartbeat_at`: Unix timestamp in milliseconds of last heartbeat
- `deregistered_at`: Soft-delete timestamp (0 = active, >0 = deregistered during graceful shutdown)

See `extension/queue/mysql/schema/queue_subscriber_heartbeats.sql` for full schema.

### Dead Letter Queue

DLQ messages are stored in the same `queue_messages` table under a different topic name (original topic + DLQ suffix, e.g., `merge_queue_dlq`). This allows DLQ messages to be consumed using the normal subscriber with the DLQ topic name. DLQ-specific fields (`failed_at`, `failure_count`, `last_error`, `original_topic`) are populated when a message is moved to DLQ; they are zero/empty for normal messages.

## Message Flow

**1. Publish** — Insert messages into `queue_messages` with AUTO_INCREMENT offset

**2. Lease Acquisition** — `INSERT ... ON DUPLICATE KEY UPDATE` with stale lease detection

**3. Fetch** — Read from immutable log: `SELECT ... WHERE topic=? AND partition_key=? AND offset > ?`

**4. Delivery Check** — Check `queue_delivery_state` for per-consumer-group deliverability:
   - No row (never delivered) → deliverable
   - `acked = TRUE` → skip (already processed)
   - `acked = FALSE`, `invisible_until <= now` (visibility expired) → deliverable (redelivery)
   - `acked = FALSE`, `invisible_until > now` (in-flight/nack delay) → skip

**5. Mark Delivered** — `INSERT ... ON DUPLICATE KEY UPDATE` in delivery state: set `invisible_until = now + timeout`, increment `retry_count` on redelivery (only if `acked = FALSE`)

**5a. Extend Visibility** — `UPDATE invisible_until` only, without incrementing `retry_count`. Used by `ExtendVisibilityTimeout()` for long-running work.

**6. Ack** — Set `acked = TRUE`, advance contiguous watermark, update `offset_acked`, clean up delivery state behind watermark. All operations are idempotent — errors propagate to the caller for retry.

**7. Nack** — Set `invisible_until = now + delay` for retry after backoff

**8. DLQ** — If `retry_count >= MaxAttempts`: atomically move message to DLQ topic (INSERT with DLQ topic + DELETE from original topic in transaction). MoveToDLQ must succeed before marking acked — otherwise the message would be lost from both main queue and DLQ.

**9. Garbage Collection** — Delete messages where `offset <= MIN(offset_acked)` across all consumer groups

## Consumer Group Isolation

All per-message state is scoped to `(consumer_group, topic, partition_key)`. Nothing is global. Each consumer group has:

- **Independent delivery state** — visibility timeout, retry count, and ack state per message are tracked separately in `queue_delivery_state`. Consumer group A nacking a message has no effect on consumer group B's view of the same message.
- **Independent offsets** — each group maintains its own `offset_acked` watermark in `queue_offsets`. Group A can be ahead or behind group B.
- **Independent partition leases** — each group has its own set of leases in `queue_partition_leases`. Workers in group A do not compete with workers in group B.
- **Independent heartbeats** — subscriber heartbeats are scoped to `(consumer_group, topic)` for fair share computation within a group.
- **Shared immutable log** — `queue_messages` is the only shared table. It is append-only and never mutated by consumers. All consumer groups read from the same log but track their own position and delivery state.

Garbage collection is the only cross-group operation: `GarbageCollect` computes `MIN(offset_acked)` across all consumer groups for a partition, ensuring a message is only deleted after every group has acked past it.

## Ordering and Serialization

### Default: Concurrent Delivery Within Partition

By default, the poll loop fetches a batch of messages (`BatchSize`, default 10) from the immutable log and delivers each one that passes the `IsDeliverable()` check. Multiple messages from the same partition can be in-flight concurrently. Messages are delivered in offset order but may be acked out of order.

### Non-Blocking Nack

When a message is nacked, its `invisible_until` is set to a future timestamp. On the next poll, the nacked message is skipped via `IsDeliverable()` while subsequent messages are still delivered normally. A nacked message does not block, starve, or delay any other message in the partition.

Example with 5 messages at offsets 1-5, all delivered:
- Message 3 is nacked with 30s delay
- Messages 1, 2, 4, 5 can be acked independently
- Watermark advances to 2 (contiguous from head), stops at 3 (not acked)
- After 30s, message 3 becomes deliverable again, is redelivered
- Once message 3 is acked, watermark jumps from 2 to 5

### Strict Serialization (Opt-In)

For use cases requiring strict in-order processing (e.g., ordered state machine transitions), set `BatchSize = 1`. This ensures:

1. Only one message is fetched per poll cycle
2. Only one message is in-flight at a time per partition
3. The next message is not fetched until the current one is acked or nacked and becomes invisible

**Requirement:** `VisibilityTimeoutMs` must exceed the maximum processing time. If the timeout expires before the consumer acks, the message becomes deliverable again and may be delivered concurrently with the next poll — violating serialization. Set a generous timeout and use `ExtendVisibilityTimeout()` for long-running work.

### Watermark Advancement

The `offset_acked` watermark represents the highest contiguous acked offset — all messages at or below this offset are fully processed. On each ack, the subscriber:

1. Fetches message offsets above the current watermark from `queue_messages`
2. Batch-fetches delivery state for those offsets from `queue_delivery_state`
3. Walks offsets in order: advances while contiguous acked, stops at the first non-acked or undelivered message
4. Updates `offset_acked` to the new watermark
5. Cleans up delivery state rows behind the new watermark (eventual consistency — stale rows are harmless and retried on next call)

The two-query approach avoids cross-table JOINs. Each store queries only its own table; the subscriber orchestrates both.

All watermark operations are idempotent. If any step fails, the error propagates to the caller for retry. The ack is not considered complete until the watermark is advanced — this ensures the caller retries the full sequence rather than silently losing watermark progress.

## Message Durability

Messages are never silently lost. Every deletion path has explicit guards:

**Garbage Collection:** Only deletes messages where `offset <= MIN(offset_acked)` across all consumer groups. If any consumer group has not acked past a message, it is retained. Safe under concurrent reads — a consumer processing a message at offset N has not yet acked it, so `MIN(offset_acked)` stays below N.

**Move to DLQ:** Atomic transaction: INSERT into DLQ topic, then DELETE from original topic. If the transaction fails at any point, the original message is preserved via ROLLBACK. The message never exists in zero tables.

**Delivery State Cleanup:** The `AdvanceWatermark` cleanup DELETE only removes delivery state rows (not messages) behind the contiguous watermark. Stale rows are harmless (never queried — all reads use offset > watermark) and cleaned up on the next call.

**No Silent Deletions:** There is no code path that deletes a message without either (a) all consumer groups having acked past it, or (b) an atomic move to DLQ. The `Delete()` method exists on the store interface but is not called in any production flow.

## Crash Recovery

**Scenario:** Worker crashes while processing a message

**What happens:**
1. Delivery state has `invisible_until = crash_time + VisibilityTimeout`
2. After timeout expires, message becomes deliverable again via `IsDeliverable()`
3. Another worker detects stale lease and steals partition
4. Message is redelivered (at-least-once guarantee)
5. `retry_count` increment on redelivery prevents infinite retries

**Scenario:** Worker crashes after ack but before watermark update

**What happens:**
1. Message is marked acked in delivery state (`acked = TRUE`)
2. Watermark was not advanced (crash interrupted the flow)
3. On next ack of any message in this partition, watermark scans forward and catches up
4. No message loss — the acked message is simply not re-delivered

**Key properties:** Automatic failover, no data loss, configurable retry delay, eventual watermark convergence.

## Distributed Processing

**Same Consumer Group:** Workers distribute partitions via fair leasing. Each partition processed by one worker. Heartbeats enable `ceil(totalPartitions / activeSubscribers)` fair share computation. Rebalance releases excess partitions when new subscribers join.

**Different Consumer Groups:** Independent consumption with separate delivery state and offsets. Same immutable message log consumed by all groups. One group's nacks, retries, and DLQ moves have no effect on other groups.

## Fair Partition Distribution

Subscribers send periodic heartbeats to `queue_subscriber_heartbeats`. The fair share algorithm:

1. Count active subscribers (heartbeat within `LeaseDurationMs`)
2. Count total partitions (union of owned leases + discovered from messages table)
3. Compute `maxPartitions = ceil(totalPartitions / activeSubscribers)`
4. Cap lease acquisitions at `maxPartitions`
5. On each lease renewal tick, if this subscriber holds more than `maxPartitions`, release excess (sorted deterministically so the same partitions are released across runs)

**Graceful shutdown:** Subscribers deregister their heartbeat and release all leases, enabling immediate redistribution.

**Failure:** If fair share computation fails (heartbeat store error, discovery error), the subscriber falls back to unlimited acquisition — ensuring availability over perfect fairness.

## Alternatives Considered

### Watermill Library

**Evaluation:** We prototyped a full implementation using `github.com/ThreeDotsLabs/watermill-sql`

**Pros:**
- Mature abstractions for pub/sub
- Built-in middleware (poison queue, retry, metrics)
- Multi-backend support (MySQL, PostgreSQL, Kafka)
- Well-tested and documented

**Cons:**
- **No partition key support** - can't guarantee ordering within partitions
- **Requires table per topic** - schema migration for every new topic, not friendly for dynamic topics
- Generic interface hides database-specific optimizations
- Less control over exact SQL queries (e.g., can't optimize delivery state logic)
- Additional dependency to maintain and version
- More infrastructure to maintain (separate tables, schema management)
- Learning curve for team (new library semantics)

**Example of Watermill's limitations:**

For our use case, we need ordering per repository. With Watermill:
- Creates one table per topic (table explosion for dynamic partitions)
- To get per-repo ordering: either one table per repo OR multiple offset tables per consumer group
- Schema migrations required for each new repository

With our custom implementation:
- Single `queue_messages` table for all topics and partitions
- Rows like `('merge_events', 'repo-123', offset, ...)` provide ordering within partition
- No schema migrations for new repos or topics
- Ordering guaranteed within `(topic, partition_key)`

**Decision:** Custom implementation gives us partition ordering guarantees, immutable log design, and single-table simplicity. Watermill is valuable for complex multi-backend scenarios but doesn't fit our partition-based ordering requirements.

### Single-Table Per Topic

**Pros:** Better isolation, easier to drop topics

**Cons:** Schema migration per topic. Not friendly for dynamic topic creation.

**Decision:** Single-table design for operational simplicity.

### Mutable Message State (Previous Design)

The original design tracked visibility timeout and retry count directly on `queue_messages` rows. This worked but had limitations:
- Messages were mutated on every delivery, creating write contention
- Multiple consumer groups couldn't independently track delivery state for the same message
- Garbage collection required complex coordination since messages could be in different states per consumer group

The current design separates the immutable message log from per-consumer-group delivery state, enabling independent consumption and cleaner GC (delete when all groups have acked past a message).

## Trade-offs

**Polling vs Push**
- Simpler (no connection management), natural backpressure
- Higher latency (configurable via PollInterval)
- Mitigation: Tune PollInterval (default 100ms, tests 20ms)

**Immutable Log + Delivery State vs Mutable Messages**
- Multiple consumer groups consume independently
- Cleaner GC (min watermark across groups)
- Extra table for delivery state tracking
- Mitigation: Delivery state rows cleaned up behind watermark; two separate queries (no JOIN) for watermark advancement

**Non-Blocking Nack vs Head-of-Line Blocking**
- Nacked messages don't starve the partition — later messages flow normally
- Out-of-order acks mean watermark only advances past contiguous acked blocks
- More delivery state rows to track (cleaned up behind watermark)
- Mitigation: Watermark advancement cleans up rows; GC deletes messages behind min watermark

**Strict Serialization (BatchSize=1) vs Concurrent Delivery**
- Strict ordering guaranteed within partition when needed
- Lower throughput (one message at a time per partition)
- Requires correct VisibilityTimeoutMs configuration (must exceed max processing time)
- Mitigation: Use for ordering-sensitive topics only; concurrent delivery for throughput-sensitive topics

**Visibility Timeout vs Heartbeat**
- No heartbeat protocol per message, automatic retry
- Full timeout delay even on immediate crash
- Mitigation: ExtendVisibilityTimeout() for long tasks

**Database Leasing vs External Coordinator**
- No ZooKeeper/etcd, transactional consistency
- Lease renewal overhead
- Mitigation: Tunable renewal interval (default 10s)

**Fair Share via Heartbeats vs Static Assignment**
- Dynamic rebalancing as subscribers join/leave
- Eventually consistent (brief imbalance during transitions)
- Mitigation: Rebalance on every lease renewal tick; deterministic partition release order

**At-Least-Once vs Exactly-Once**
- Simpler, better performance
- Applications must handle duplicates
- Mitigation: Idempotency keys (e.g., merge request ID)

## Observability

### Metrics (via tally)

All metrics use `metrics.NamedCounter`, `metrics.NamedGauge`, and `metrics.NamedTimer` from `core/metrics`. Store-level operations use `metrics.Begin`/`Complete` for latency and success/failure tracking, tagged with `topic`, `consumer_group`, and `partition_key` where applicable.

**Publisher:**
- `messages_published`, `publish_errors`

**Subscriber — Poll:**
- `poll.messages_delivered`, `poll.message_age`, `poll.latency`

**Subscriber — Lifecycle:**
- `subscribe.active_subscriptions`

**Stores (via `metrics.Begin`/`Complete`):**
- Each operation has `.succeeded`, `.failed`, `.latency` (e.g., `message_store.insert.succeeded`, `delivery_state_store.mark_acked.latency`, `delivery_state_store.extend_visibility.latency`)
- All tagged with `topic`, `consumer_group`, `partition_key` where the method has those parameters
- `message_store.gc.messages_deleted`
- `delivery_state_store.advance_watermark.cleanup_errors`

No duplicate metrics between subscriber and stores — stores are the single source of truth for per-operation tracking.

### Logging (via zap)

Logger hierarchy: `queue_mysql.{component}` (e.g., `queue_mysql.subscriber`, `queue_mysql.message_store`).

**Log Severity Guidelines:**

| Level | When Used | Examples |
|-------|-----------|---------|
| Debug | Normal operational details | Message fetch counts, partition worker start/stop, subscription creation, nack |
| Info | Significant state changes | Publish success, partition rebalance releases, subscriber open/close |
| Warn | Swallowed errors with documented reason | Discover partitions fallback, worker stop timeout, message exceeded retry limit |
| Error | All propagated errors logged at call site | Poll failure, lease renewal failure, heartbeat failure, rebalance failure |

**Structured fields:** `topic`, `partition_key`, `message_id`, `offset`, `retry_count`, `error`, `watermark`, `consumer_group`, `subscriber_name`

### Error Handling Architecture

**Stores** return errors via `fmt.Errorf` — they do not log errors themselves (no double-logging). The only exceptions are documented swallowed errors:
- `RowsAffected()` driver failures after a successful DELETE (diagnostic only)
- Per-partition lease acquisition failures in `DiscoverAndAcquirePartitions` (one partition's failure shouldn't block others)
- Watermark cleanup DELETE failure (stale rows are harmless, retried on next call)
- `DiscoverPartitions` failure in fair share (graceful degradation to owned-only)

Each swallowed error has an inline comment explaining why it cannot be propagated.

**Subscriber** propagates all errors to the top call site (`managePartitions` or `run`). Intermediate methods like `renewLeases`, `sendHeartbeat`, `rebalance`, and `pollAndDeliver` return errors — they never log internally. The call site logs once with full context (`topic`, `consumer_group`, `subscriber_name`).

**Ack, Reject** propagate all errors to the caller. All operations are idempotent — callers can safely retry. No errors are swallowed.

**`run` (ticker loop)** is the terminal call site — there is no upstream caller. Errors from `pollAndDeliver` are logged and the next tick retries automatically.

## Performance

- **Throughput:** ~1k-5k msg/sec publish, ~500-2k msg/sec consume (single MySQL)
- **Latency:** Best case = PollInterval (100ms), Retry after crash = VisibilityTimeout (60s)
- **Bottlenecks:** MySQL write throughput, lease renewal overhead, polling overhead

## Appendix

### References

**Database & Locking:**
- [MySQL InnoDB Locking](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html) - Understanding MySQL transaction isolation and row-level locking
- [MySQL AUTO_INCREMENT](https://dev.mysql.com/doc/refman/8.0/en/example-auto-increment.html) - AUTO_INCREMENT behavior for offset generation

**Queue Patterns:**
- [Amazon SQS Visibility Timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) - Inspiration for visibility timeout mechanism
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumergroups) - Consumer group and partition assignment patterns
- [RabbitMQ Dead Letter Exchanges](https://www.rabbitmq.com/dlx.html) - Dead letter queue concepts

**Alternative Implementations:**
- [Watermill Documentation](https://watermill.io/) - Go library for message streaming (evaluated alternative)
- [PostgreSQL SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) - Alternative database queue pattern
