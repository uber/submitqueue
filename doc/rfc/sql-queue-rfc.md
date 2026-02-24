# RFC: SQL-Based Distributed Queue

## Metadata

| Field | Value                            |
|-------|----------------------------------|
| **Author** | Preetam Dwived<preetam@uber.com> |
| **Status** | In Review                        |
| **Created** | 2026-02-16                       |
| **Updated** | 2026-02-16                       |

## Summary

MySQL-based distributed message queue with partition leasing, visibility timeout, and at-least-once delivery. Workers coordinate via database-native primitives without external systems.

## Background

### Motivation

SubmitQueue needs a reliable message queue for coordinating asynchronous workflows:
- **Orchestrator** publishes merge jobs to workers
- **Speculator** publishes speculative build requests
- **Workers** need distributed coordination without duplicate processing
- **Crash recovery** must preserve exactly where processing stopped

### Existing Solutions

We evaluated several approaches:

1. **External Message Brokers** (Kafka, RabbitMQ)
   - ❌ Additional operational overhead and infrastructure
   - ❌ Network hops increase latency
   - ✅ Battle-tested and highly scalable

2. **Watermill Library** (github.com/ThreeDotsLabs/watermill)
   - ✅ Database-backed queue with mature abstractions
   - ✅ Built-in middleware (retry, poison queue, metrics)
   - ❌ Generic interface hides database-specific optimizations
   - ❌ Additional dependency and learning curve
   - ❌ Less control over exact SQL queries and behavior

3. **dbqueue-go** (github.com/yunussandikci/dbqueue-go)
   - ✅ Lightweight, simple FIFO queue over SQL (MySQL, PostgreSQL, SQLite)
   - ✅ Basic features: priority, deduplication, visibility timeout
   - ❌ No distributed worker coordination or partition leasing
   - ❌ No built-in retry mechanism or DLQ
   - ❌ Designed for single-worker scenarios, not multi-worker distribution

4. **Database-Backed Queue** (Custom implementation)
   - ✅ Reuses existing MySQL infrastructure
   - ✅ Full control over queries and behavior
   - ✅ No additional services or dependencies
   - ❌ More code to maintain

### Decision

We chose **custom database-backed queue** because:
- Full control over SQL queries for optimal performance
- No additional libraries - direct use of database/sql
- Simpler to understand and debug (no abstraction layers)
- Can optimize for our specific use case (partition ordering, visibility timeout)
- Watermill adds valuable abstractions but we need fine-grained control

## Requirements

### Functional Requirements

1. **Publish/Subscribe** - Standard pub/sub with topics
2. **Partitioning** - Messages with same key processed in order by single worker
3. **At-Least-Once Delivery** - Guaranteed delivery, duplicates possible
4. **Crash Recovery** - Workers resume from last committed offset
5. **Distributed Workers** - Multiple workers coordinate without duplicate processing
6. **Dead Letter Queue** - Failed messages isolated after max retries
7. **Visibility Timeout** - Messages invisible during processing, visible if worker crashes

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
│  │  - topic, partition_key       │  │
│  │  - offset, invisible_until    │  │
│  │  - retry_count, payload       │  │
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

**Partition Leasing:** Workers coordinate using database-native leases. Each partition leased by exactly one worker. Stale leases automatically stolen on crash.

**Visibility Timeout:** Messages invisible during processing. Auto-retry on crash when timeout expires.

**Persistent Retry Tracking:** `retry_count` incremented atomically on fetch, survives crashes, triggers DLQ.

**Offset Tracking:** Per-partition offsets enable crash recovery from last acked message.

## Database Schema

### Messages Table

**Key Fields:**
- `offset` (PK): Auto-incrementing global offset for message ordering
- `topic`, `partition_key`: Message routing and partitioning
- `id`: Unique message identifier
- `payload`, `metadata`: Message content
- `retry_count`: Persistent retry tracking (survives worker crashes)
- `invisible_until`: Visibility timeout (epoch ms) for crash recovery
- `created_at`, `published_at`: Timestamps

**Indexes:**
- `(topic, partition_key, invisible_until, offset)`: Core fetch query - find visible messages in partition ordered by offset
- `(topic, partition_key, id)`: Unique constraint and fast lookup for Ack/Nack

See `extension/queue/mysql/schema/queue_messages.sql` for full schema.

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
- `offset_acked`: Last successfully processed offset
- `updated_at`: Last update timestamp

**Indexes:**
- `(consumer_group)`: Monitor all offsets for a consumer group
- `(topic)`: Find all consumers for a topic

See `extension/queue/mysql/schema/queue_offsets.sql` for full schema.

### Dead Letter Queue Table

**Key Fields:**
- `offset` (PK): Auto-incrementing offset for DLQ ordering
- `topic`, `partition_key`, `id`: Original message identification
- `payload`, `metadata`: Original message content
- `failed_at`: When message moved to DLQ
- `failure_count`, `last_error`: Failure diagnostics

**Indexes:**
- `(topic, partition_key, failed_at)`: Query DLQ by topic/partition, ordered by failure time
- `(failed_at)`: Time-based queries and cleanup
- `(topic, partition_key, id)`: Unique constraint, prevents duplicates

See `extension/queue/mysql/schema/queue_dlq.sql` for full schema.

## Message Flow

**1. Publish** - Insert messages with AUTO_INCREMENT offset

**2. Lease Acquisition** - `INSERT ... ON DUPLICATE KEY UPDATE` with stale lease detection

**3. Fetch** - Atomic UPDATE sets `invisible_until` and increments `retry_count`

**4. Ack** - Transaction: DELETE message + UPDATE offset_acked

**5. Nack** - UPDATE `invisible_until` for retry after delay

**6. DLQ** - If `retry_count >= MaxAttempts`: DELETE from messages + INSERT into dlq

## Crash Recovery

**Scenario:** Worker crashes while processing message

**What happens:**
1. Message has `invisible_until = crash_time + VisibilityTimeout`
2. After timeout expires, message becomes visible
3. Another worker detects stale lease and steals partition
4. Message redelivered (at-least-once guarantee)
5. `retry_count` incremented prevents infinite retries

**Key properties:** Automatic failover, no data loss, configurable retry delay

## Distributed Processing

**Same Consumer Group:** Workers distribute partitions via leasing. Each partition processed by one worker.

**Different Consumer Groups:** Independent consumption with separate offsets. Same messages delivered to all groups.

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
- Less control over exact SQL queries (e.g., can't optimize visibility timeout logic)
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

**Decision:** Custom implementation gives us partition ordering guarantees and single-table design. Watermill is valuable for complex multi-backend scenarios but doesn't fit our partition-based ordering requirements.

### Single-Table Per Topic

**Pros:** Better isolation, easier to drop topics

**Cons:** Schema migration per topic. Not friendly for dynamic topic creation.

**Decision:** Single-table design for operational simplicity.

## Trade-offs

**Polling vs Push**
- ✅ Simpler (no connection management), natural backpressure
- ❌ Higher latency (configurable via PollInterval)
- Mitigation: Tune PollInterval (default 100ms, tests 20ms)

**Visibility Timeout vs Heartbeat**
- ✅ No heartbeat protocol, automatic retry
- ❌ Full timeout delay even on immediate crash
- Mitigation: ExtendVisibilityTimeout() for long tasks

**Database Leasing vs External Coordinator**
- ✅ No ZooKeeper/etcd, transactional consistency
- ❌ Lease renewal overhead
- Mitigation: Tunable renewal interval (default 10s)

**At-Least-Once vs Exactly-Once**
- ✅ Simpler, better performance
- ❌ Applications must handle duplicates
- Mitigation: Idempotency keys (e.g., merge request ID)


## Observability

**Metrics (via tally):**
- Publisher: `messages_published`, `publish_errors`
- Subscriber: `messages_acked`, `messages_nacked`, `messages_moved_to_dlq`, `message_age`, `leases_acquired`
- Stores: `insert.latency`, `fetch.latency`, `ack_message.latency`, `renew_lease.latency`

**Logging (via zap):**
- Debug: Message fetch, lease operations
- Info: Publish success, DLQ moves, partition acquisition
- Error: Database errors, unrecoverable failures
- Structured fields: `topic`, `partition_key`, `message_id`, `offset`, `retry_count`

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