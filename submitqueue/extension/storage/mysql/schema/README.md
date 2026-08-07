# MySQL Schema

## Queue-leading primary keys

Every queue-scoped table leads its primary key with `queue`: `request` and `batch` on `(queue, id)`, `build` on `(queue, id)`, `batch_dependent` on `(queue, batch_id)`, `request_batch` on `(queue, request_id, batch_id)`, `change` on `(queue, uri, request_id)`, `queue_batch_state` on `(queue, state, batch_id)`, and `request_summary_by_queue` on `(queue, received_at_ms, request_id)`. A queue-bound store instance prefixes every read and stamps every write with its bound queue, so one queue's rows are unreachable through another queue's binding and the tables are shardable by queue. The `build` key also removes a cross-queue uniqueness assumption: build IDs are runner-minted, so two queues sharing one CI pipeline may legitimately mint the same identifier.

The global read-model tables (`request_summary`, `request_log`, `change_uri_request_mapping`) keep queue-free keys — their lookups start from identifiers that arrive without queue context.

## batch table

The `batch` table is keyed by `(queue, id)` and carries no secondary index. Listing a queue's batches by state goes through the `queue_batch_state` table instead, so batch reads and writes stay pure primary-key operations.

## queue_batch_state table

### Composite primary key: `(queue, state, batch_id)`

`queue_batch_state` holds the queue's advisory per-state membership records (see `entity.QueueBatchState`): one row per batch per state bucket, no payload and no version column. The key leads with `queue` so a state-bucket listing is a primary-key-prefix scan and the table is shardable by queue. Rows are moved between buckets by the shared transition protocol in `submitqueue/core/batch`; writes are idempotent (`INSERT IGNORE`, keyed `DELETE`). The `batch` row remains authoritative — readers hydrate each candidate and classify by the batch's own state.

#### Future: Prune job

Terminal-state records (and their batches) accumulate as the queue processes work. A prune job should periodically delete records and batches in terminal states (`succeeded`, `failed`, `cancelled`) older than a configurable retention period, keeping both tables bounded so query and write performance stay consistent over time.

## change table

### Composite primary key: `(queue, uri, request_id)`

The `change` table records per-URI claims by in-flight requests. `request_id` is part of the primary key so that concurrent claims on the same URI by different requests coexist as distinct rows — a same-request retry collides on the PK and is a no-op (`INSERT IGNORE`), while a different-request claim is a new row that `GetByURI` surfaces for overlap detection. `queue` leads the key so queue-scoped lookups are primary-key-prefix scans and the table is shardable by queue.

## Gateway request read model

The gateway request read model uses three additive tables and requires no alteration of existing tables. Deployments create these tables empty and populate them only for requests received after rollout; historical request logs and orchestrator working tables are intentionally not backfilled.

### `request_summary`

`request_summary` is keyed by `request_id` and serves direct Status lookup. It stores immutable receipt context plus the current materialized request-log winner and its optimistic-lock projection version.

### `request_summary_by_queue`

`request_summary_by_queue` is keyed by `(queue, received_at_ms, request_id)`. This key covers the List predicate, descending sort, and keyset continuation for one bounded receipt-time window without a secondary index. The row duplicates the complete List response so one page is served by one range scan rather than one follow-up read per request ID.

### `change_uri_request_mapping`

`change_uri_request_mapping` is keyed by `(change_uri, received_at_ms, request_id)` and serves bounded newest-first Status lookup by change URI. The gateway reads at most 101 mappings to enforce the API maximum of 100 results without silently truncating.

### JSON collections

`change_uris` and `metadata` are non-null application values. MySQL JSON columns can contain the JSON value `null` despite `NOT NULL`, so stores normalize nil slices and maps to empty values on both write and read.
