# MySQL Schema

## batch table

### Secondary index: `idx_queue_state (queue, state)`

The `batch` table has a composite secondary index on `(queue, state)`. This index supports the `GetByQueueAndStates` query, which retrieves batches filtered by queue and one or more states. Without this index, the query would require a full table scan.

#### Trade-offs

- **Write overhead**: Every `INSERT` and `UPDATE` to the `batch` table must also update the secondary index, adding latency to write operations.
- **Storage cost**: The index consumes additional disk space proportional to the number of rows in the table.
- **Lock contention**: Under high write concurrency, index maintenance can increase lock contention on the affected index pages.

#### Future: Prune job

As the `batch` table grows, the secondary index will grow with it, increasing storage costs and degrading write performance. To mitigate this, a prune job should be introduced to periodically delete batches in terminal states (`succeeded`, `failed`, `cancelled`) that are older than a configurable retention period. This keeps the table and its indexes bounded in size, ensuring consistent query and write performance over time.

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
