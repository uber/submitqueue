# RFC: Per-Tenant Sharding for the MySQL Message Queue

## Summary

The platform MySQL message queue gains a domain-agnostic `tenant` column on every table. Sharded MySQL routes on `tenant`; every primary key and hot-path query leads with it. SubmitQueue, Stovepipe, and Runway map their `queueName` onto `tenant` at the wiring boundary. `partition_key` remains the ordering unit within a tenant and is not the shard key.

## Background

Domain storage (`request`, `batch`, `counter`, …) already shards by a leading `queue` column. The message queue backend was deliberately excluded from `tool/linter/queueshard` because it keyed rows by `(consumer_group, topic, partition_key)` with a global `AUTO_INCREMENT offset` — fine for a single MySQL instance, not for sharded MySQL.

SubmitQueue's business queue name already flows through publish metadata (`queue_name`) and consumer context (`WithQueueName`). Several pipeline stages use a different `partition_key` (build ID, request ID) so those deliveries serialize independently while still belonging to one business queue. Sharding on `partition_key` would split one queue across shards; the MQ needs a dedicated isolation column.

## Naming

| Layer | Column / field | Meaning |
|-------|----------------|---------|
| Platform MQ schema | `tenant` | Shard key; opaque to the backend |
| Platform `Message` | `Tenant` | Persisted shard identity |
| SubmitQueue domain | `queue` / `queueName` | Same string as `tenant` at wiring |
| Platform MQ schema | `partition_key` | Ordering unit within `(tenant, topic)` |

The MQ schema does not use `queue` — that word is overloaded (SubmitQueue domain, `Queue` interface, `queue_*` table prefix).

## Schema

Every table's primary key leads with `tenant`. Tenant, topic, consumer-group, and subscriber identifiers use `VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin`; partition keys and message IDs use explicit `VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`. This keeps operational identifiers readable, preserves unrestricted UTF-8 ordering keys and IDs, and keeps the largest composite key within InnoDB's 3072-byte limit. The backend validates each contract before database access. Secondary indexes that do not lead with `tenant` are removed except `queue_messages.idx_offset`, which InnoDB requires because the `AUTO_INCREMENT offset` column must be leftmost in an index.

### `queue_messages`

- PK: `(tenant, topic, partition_key, offset)`
- Unique: `(tenant, topic, partition_key, id)`
- Required InnoDB index: `idx_offset (offset)` for the `AUTO_INCREMENT` column
- `offset` is allocated from a shard-wide monotonic sequence and used as an ordering cursor within each partition; fetch is `WHERE tenant=? AND topic=? AND partition_key=? AND offset>? ORDER BY offset`

### `queue_delivery_state`

- PK: `(tenant, consumer_group, topic, partition_key, message_offset)`

### `queue_offsets`

- PK: `(tenant, topic, partition_key, consumer_group)`
- Drop `idx_topic`

### `queue_partition_leases`

- PK: `(tenant, consumer_group, topic, partition_key)`
- Drop `idx_lease_renewed`; purge is scoped to `(tenant, consumer_group, topic)`

### `queue_subscriber_heartbeats`

- PK: `(tenant, consumer_group, topic, subscriber_name)`

DLQ moves rewrite `topic` to `original + suffix` and keep `tenant` + `partition_key` on the same shard.

## Subscriber discovery

Today partition discovery runs `SELECT DISTINCT partition_key FROM queue_messages WHERE topic=?`, which scatter-gathers across all shards.

The subscriber takes an explicit configured tenant list from `MQ_TENANTS`. Consumer processes reject an empty list at startup; Stovepipe also rejects ingest requests for names outside the list. Discovery becomes:

```sql
SELECT DISTINCT partition_key FROM queue_messages
WHERE tenant = ? AND topic = ?
ORDER BY partition_key
```

Fair-share, orphan sweep, and idle-lease release run per `(tenant, topic)`, not across all tenants on a topic. Discovery and shutdown attempt every configured tenant and aggregate errors so one unavailable shard does not block unrelated tenants.

## Publish

Every `platform/publish` call supplies tenant explicitly. The package stamps `Message.Tenant` and mirrors it into `queue_name` delivery metadata, rejecting empty tenants and conflicting caller metadata. `PartitionKey` is unchanged.

## Wiring

One `extqueue.Queue` and one sharded MySQL DSN per service. `NewQueue` / subscriber `Params` carry `Tenants []string`. Consumer service wiring parses the authoritative comma-separated `MQ_TENANTS` list once and passes it to the backend and any ingress validation.

## Operational tooling

Fleet-wide admin reads must be explicit. The topic, offset, active-lease, and stale-lease listing commands require exactly one of a single tenant or all tenants; omitting both never falls back to a scatter query. Message inspection, deletion, and DLQ requeue identify a row by the complete unique identity `(tenant, topic, partition_key, id)`.

## Out of scope

Live migration of existing Stovepipe prod queue databases (expand/contract, backfill, dual-write). This RFC describes a breaking greenfield schema; prod cutover is a separate exercise.

## Related

- [SQL-Based Distributed Queue](sql-queue-rfc.md)
- [Modular Queue Wiring](submitqueue/modular-queue-wiring.md)
