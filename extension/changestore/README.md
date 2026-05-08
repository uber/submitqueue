# ChangeStore

Vendor-agnostic interface for tracking per-URI claims by in-flight land requests.

Each record asserts that a specific URI (e.g., a GitHub PR) was claimed by a specific request, scoped to a queue. The store is consulted by the orchestrator's `start` controller to detect duplicate requests — submissions whose URIs overlap with another in-flight request's URIs in the same queue.

## Semantics

- **Identity is immutable.** A record is keyed by `(Queue, URI, RequestID)`; once written, that triple is never mutated.
- **Queue leads the key.** Backends should make `Queue` the leading column of the primary key (or partition key, in shardable stores). All reads are queue-scoped, so this turns lookups into PK-prefix scans and keeps the table shardable.
- **`RequestID` in the key is intentional.** Concurrent claims by different requests on the same URI coexist as distinct rows. Same-request retries collide on the PK and are absorbed idempotently; cross-request collisions show up as additional rows that callers detect via `FindOverlapping`.
- **Metadata is required and mutable.** The `Metadata` field is JSON. The store treats `'{}'` as the canonical "no metadata yet" value — callers that pass an empty Go string get `'{}'` written. Downstream enrichment can update it; `UpdatedAt` reflects the last update.
- **Idempotent writes, atomic batches.** `Create` ignores primary-key conflicts so queue-redelivery of the same request is a safe no-op. The whole batch is one underlying multi-row INSERT — partial success is not exposed.
- **No filtering at the store layer.** `FindOverlapping` returns every matching row, including ones owned by the caller's own request. Callers that want to skip self filter the result by `RequestID` themselves. Liveness is also the caller's job — consult `RequestStore` to skip terminal owners. The store boundary is intentionally one query, one table, no joins.
- **Append-only by design.** Records are not deleted when the owning request reaches a terminal state; the historical claim is preserved for audit. Duplicate detection filters terminals out at query time via the controller-side liveness check.

## Implementing a Backend

1. Create `extension/changestore/{backend}/` directory.
2. Implement the `ChangeStore` interface.
3. Add schema files under `extension/changestore/{backend}/schema/` if the backend requires them.
