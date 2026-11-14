# MySQL Schema

## Queue-leading primary keys

Every table leads its primary key with `queue`: `request` and `batch` on `(queue, id)`, `build` on `(queue, id)`, `batch_dependent` on `(queue, batch_id)`, `request_batch` on `(queue, request_id, batch_id)`, `change` on `(queue, uri, request_id)`, `queue_batch_state` on `(queue, state, batch_id)`, and `speculation_path_set` on `(queue, head)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and every table is shardable by queue. `//tool/linter/queueshard` enforces this, and also rejects any secondary index that does not itself lead with `queue`, since such an index would reintroduce a cross-queue access path.

The `build` key also removes a cross-queue uniqueness assumption: build IDs are runner-minted, so two queues sharing one CI pipeline may legitimately mint the same identifier. `speculation_path_set` relies on the same property for its head: a batch ID is unique only within its queue.

Because the queue is part of every key, the request identifier alone no longer addresses a row — the read APIs take the queue alongside the sqid or change URI, and the stores are resolved per queue through `storage.Factory`. No identifier is parsed to recover a queue.

## batch table

The `batch` table is keyed by `(queue, id)` and carries no secondary index. Listing a queue's batches by state goes through the `queue_batch_state` table instead, so batch reads and writes stay pure primary-key operations.

## queue_batch_state table

### Composite primary key: `(queue, state, batch_id)`

`queue_batch_state` holds the queue's advisory per-state membership records (see `entity.QueueBatchState`): one row per batch per state bucket, no payload and no version column. The key leads with `queue` so a state-bucket listing is a primary-key-prefix scan and the table is shardable by queue. Rows are moved between buckets by the shared transition protocol in `submitqueue/orchestrator/core/batch`; writes are idempotent (`INSERT IGNORE`, keyed `DELETE`). The `batch` row remains authoritative — readers hydrate each candidate and classify by the batch's own state.

#### Future: Prune job

Terminal-state records (and their batches) accumulate as the queue processes work. A prune job should periodically delete records and batches in terminal states (`succeeded`, `failed`, `cancelled`) older than a configurable retention period, keeping both tables bounded so query and write performance stay consistent over time.

## change table

### Composite primary key: `(queue, uri, request_id)`

The `change` table records per-URI claims by in-flight requests. `request_id` is part of the primary key so that concurrent claims on the same URI by different requests coexist as distinct rows — a same-request retry collides on the PK and is a no-op (`INSERT IGNORE`), while a different-request claim is a new row that `GetByURI` surfaces for overlap detection. `queue` leads the key so queue-scoped lookups are primary-key-prefix scans and the table is shardable by queue.

## dlq_reconciliation table

`dlq_reconciliation` stores final-message reconciliation snapshots under `(queue, source_topic, source_partition_key, source_message_id)`, with no secondary indexes or lookup by the generated record ID. The record ID and source identity are immutable; mutable fields are replaced under the record ID and explicit version guard. The store rejects another queue's snapshot and reads from the primary under the storage read-after-write contract.

Queue and logical topic use `VARBINARY(255)` to preserve exact bytes, including trailing spaces. Opaque partition and message IDs use `VARCHAR(255)` with MySQL 8's `utf8mb4_0900_bin` NO PAD collation; unlike legacy binary collations, it does not equate an identifier with its trailing-space variant. The primary key's maximum payload is 2,550 bytes (255 + 255 + 1,020 + 1,020), below InnoDB's 3,072-byte limit. Identifiers are stored directly, never hashed or re-encoded.

This additive table supplies persistence only; no final handler or planner uses it yet. Keep pending and completed rows independently of message-row garbage collection, including during rollout and rollback. Removing application access does not authorize dropping records: acknowledgment-loss replay needs the completed markers.
