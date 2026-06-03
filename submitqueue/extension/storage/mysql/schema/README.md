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
