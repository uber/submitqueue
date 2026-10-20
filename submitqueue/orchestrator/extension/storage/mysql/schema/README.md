# MySQL Schema

## Queue-leading primary keys

Every table leads its primary key with `queue`: `request` and `batch` on `(queue, id)`, `build` on `(queue, id)`, `batch_dependent` on `(queue, batch_id)`, `request_batch` on `(queue, request_id, batch_id)`, `change` on `(queue, uri, request_id)`, `queue_batch_state` on `(queue, state, batch_id)`, `queue_request_state` on `(queue, state, request_id)`, and `speculation_path_set` on `(queue, head)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and every table is shardable by queue. `//tool/linter/queueshard` enforces this, and also rejects any secondary index that does not itself lead with `queue`, since such an index would reintroduce a cross-queue access path.

The `build` key also removes a cross-queue uniqueness assumption: build IDs are runner-minted, so two queues sharing one CI pipeline may legitimately mint the same identifier. `speculation_path_set` relies on the same property for its head: a batch ID is unique only within its queue.

Because the queue is part of every key, the request identifier alone no longer addresses a row — the read APIs take the queue alongside the sqid or change URI, and the stores are resolved per queue through `storage.Factory`. No identifier is parsed to recover a queue.

## batch table

The `batch` table is keyed by `(queue, id)` and carries no secondary index. Listing a queue's batches by state goes through the `queue_batch_state` table instead, so batch reads and writes stay pure primary-key operations.

## queue_batch_state table

### Composite primary key: `(queue, state, batch_id)`

`queue_batch_state` holds the queue's advisory per-state membership records (see `entity.QueueBatchState`): one row per batch per state bucket, no payload and no version column. The key leads with `queue` so a state-bucket listing is a primary-key-prefix scan and the table is shardable by queue. Rows are moved between buckets by the shared transition protocol in `submitqueue/orchestrator/core/batch`; writes are idempotent (`INSERT IGNORE`, keyed `DELETE`). The `batch` row remains authoritative — readers hydrate each candidate and classify by the batch's own state.

#### Future: Prune job

Terminal-state records (and their batches) accumulate as the queue processes work. A prune job should periodically delete records and batches in terminal states (`succeeded`, `failed`, `cancelled`) older than a configurable retention period, keeping both tables bounded so query and write performance stay consistent over time.

## queue_request_state table

### Composite primary key: `(queue, state, request_id)`

`queue_request_state` files each in-queue request under its current state (see `entity.QueueRequestState`), so listing a queue's requests by state is a primary-key-prefix scan and the `request` table keeps no secondary index. Unlike `queue_batch_state`, the rows are exact rather than advisory: the MySQL request store writes a request's row in the same transaction as the `request` row, removing it from every other in-queue state and filing it under the new one (`ON DUPLICATE KEY UPDATE entered_at_ms = entered_at_ms`, so a version-only update keeps `entered_at_ms`). That transaction runs at `READ COMMITTED`: the removal usually matches no rows, and at the default `REPEATABLE READ` its gap locks would conflict with concurrent inserts into the same queue and state. A terminal request has no row, so the table only ever holds in-queue requests. The records are kept only by a storage built with `WithQueueRequestStateRecords`, which a deployment enables once the table exists.

### Rolling out to an existing deployment

Requests already in flight get a row only on their next write, and a stuck request never writes again. So after creating the table and enabling `WithQueueRequestStateRecords`, backfill once:

```sql
INSERT INTO queue_request_state (queue, state, request_id, entered_at_ms)
SELECT queue, state, id, <now in Unix ms> FROM request
WHERE state IN ('started', 'validated', 'batched', 'processing', 'cancelling')
ON DUPLICATE KEY UPDATE entered_at_ms = entered_at_ms;
```

Run it after enabling the records, not before: a request that changes state between a backfill and the enable would keep its backfilled row under the old state. Rows the store has already written keep their own `entered_at_ms`. For backfilled rows, `entered_at_ms` is the backfill time rather than when the request entered its state, so time-in-state for those requests is understated until their next transition.

## change table

### Composite primary key: `(queue, uri, request_id)`

The `change` table records per-URI claims by in-flight requests. `request_id` is part of the primary key so that concurrent claims on the same URI by different requests coexist as distinct rows — a same-request retry collides on the PK and is a no-op (`INSERT IGNORE`), while a different-request claim is a new row that `GetByURI` surfaces for overlap detection. `queue` leads the key so queue-scoped lookups are primary-key-prefix scans and the table is shardable by queue.
