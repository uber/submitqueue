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

