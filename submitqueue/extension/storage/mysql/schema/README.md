# MySQL Schema

## batch table

The `batch` table is reachable only by its primary key (`id`). It carries no secondary index — every access pattern is expressed as a primary-key get or as a key-prefix scan over a companion membership table (see `active_batch` below). This keeps the access patterns portable to a key-value / document store, where a server-maintained secondary index over a mutable, non-key column (such as `state`) is not a primitive every backend offers cheaply.

## active_batch table

`active_batch` is the membership index that answers "which batches in this queue are still active?" — the only queue-scoped query the pipeline needs (the batch controller uses it to find conflict dependencies; the cancel controller uses it to find the batch holding a request). A row is intended to exist per non-terminal batch, so the table stays bounded by the live speculation window rather than full batch history. The correspondence is best-effort, not exact: readers treat membership as a hint and resolve each batch by primary key — see *Maintenance and self-healing* below.

`queue` leads the composite primary key `(queue, batch_id)`, so listing a queue's active batches is a primary-key-prefix scan and the table is shardable by queue. On a key-value store the same shape maps directly onto a partition key (`queue`) and sort key (`batch_id`) with no secondary index.

### Maintenance and self-healing

`BatchStore.Create` writes the membership row before the batch row, so a batch row is never visible to `ListActive` without its membership. If the batch insert then fails, `Create` leaves the membership row in place: a returned error doesn't prove the row wasn't written (an ambiguous failure can commit it and still error), so deleting could permanently hide a live batch. A dangling row is the safe direction.

On read, `ListActive` resolves each member by primary key. A **terminal** batch's membership is best-effort removed (race-free — its id is never reused). A **missing** batch is skipped but not removed, since it may belong to an in-flight `Create` that hasn't written its batch row yet. Cleanup failures are swallowed, so reads never fail on index maintenance and terminal-state writers (merge, speculate, dlq) never touch the index. Because the two writes are independent (no transaction), the design tolerates partial failure via idempotent retries and read-time reconciliation.

### Future: prune / reconcile job

Read-time reconciliation only removes terminal memberships, so two kinds of stale row need a periodic sweep: dangling memberships whose batch never landed (a failed or crashed create), and memberships of batches that are stuck in a non-terminal state (e.g. an orphan stuck in `created` after a mid-process failure). A reconcile job should sweep both, keeping the table bounded independently of read traffic.

## change table

### Composite primary key: `(queue, uri, request_id)`

The `change` table records per-URI claims by in-flight requests. `request_id` is part of the primary key so that concurrent claims on the same URI by different requests coexist as distinct rows — a same-request retry collides on the PK and is a no-op (`INSERT IGNORE`), while a different-request claim is a new row that `GetByURI` surfaces for overlap detection. `queue` leads the key so queue-scoped lookups are primary-key-prefix scans and the table is shardable by queue.

