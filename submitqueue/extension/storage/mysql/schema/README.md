# MySQL Schema

## batch table

The `batch` table is reachable only by its primary key (`id`). It carries no secondary index over mutable columns. Queue/state access patterns are expressed through an app-maintained companion table (see `batch_state_membership` below), and callers resolve batch IDs back through the authoritative `batch` row before making state decisions.

## batch_state_membership table

`batch_state_membership` is the app-maintained lookup that answers "which batch IDs are recorded for this queue and state?" The table's primary key is `(queue, state, batch_id)`, so reads by queue/state are primary-key-prefix scans. The same shape ports to a key-value/document store that supports ranged scans over a partition/sort key, and it avoids a server-maintained secondary index over mutable `batch.state`.

The table is not authoritative. The orchestrator resolves every listed `batch_id` with `BatchStore.Get` and filters by the current persisted `Batch.State`. This keeps storage generic: storage owns primitive membership records, while the orchestrator owns app concepts such as "active", "terminal", and "eligible for conflict analysis".

### Maintenance

The orchestrator writes the target non-terminal membership row before creating a batch or before CASing a batch into a new non-terminal state. After a successful CAS, it best-effort removes the previous non-terminal membership row. Terminal transitions do not add a target membership row; after the CAS succeeds, the previous non-terminal membership row is best-effort removed.

Because membership writes and batch writes are independent, stale rows are expected in failure windows. Readers skip missing batch rows and filter stale state rows against the authoritative batch row. A terminal stale row may be removed on read because batch IDs are never reused.

### Future: prune / reconcile job

A reconcile job can periodically sweep dangling rows whose batch never landed and stale rows whose authoritative batch state no longer matches the membership state. This keeps the table bounded independently of read traffic.

## change table

### Composite primary key: `(queue, uri, request_id)`

The `change` table records per-URI claims by in-flight requests. `request_id` is part of the primary key so that concurrent claims on the same URI by different requests coexist as distinct rows — a same-request retry collides on the PK and is a no-op (`INSERT IGNORE`), while a different-request claim is a new row that `GetByURI` surfaces for overlap detection. `queue` leads the key so queue-scoped lookups are primary-key-prefix scans and the table is shardable by queue.
