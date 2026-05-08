# ChangeStore

Vendor-agnostic interface for tracking per-URI claims by in-flight land requests.

Each record asserts that a specific URI (e.g., a GitHub PR) was claimed by a specific request, scoped to a queue. The store is read by the orchestrator's `validate` controller to detect duplicate requests — submissions whose URIs overlap with another in-flight request's URIs in the same queue.

The interface is intentionally per-record / per-URI so any backend (SQL, DynamoDB, Bigtable, …) can implement it without needing batch atomicity or multi-key query support. Callers that have multiple URIs to claim or check loop over them; the typical request has a small number of URIs (a single PR or a short stack), so the loop overhead is negligible.

## Semantics

- **Identity is immutable.** A record is keyed by `(Queue, URI, RequestID)`; once written, that triple is never mutated.
- **Queue leads the key.** Backends should make `Queue` the leading column of the primary key (or partition key, in shardable stores). All reads are queue-scoped, so this turns lookups into PK-prefix scans and keeps the table shardable.
- **`RequestID` in the key is intentional.** Concurrent claims by different requests on the same URI coexist as distinct rows. Same-request retries collide on the PK and are absorbed idempotently; cross-request collisions show up as additional rows that callers detect via `GetByURI`.
- **Metadata is required and mutable.** The `Metadata` field is JSON. The store treats `'{}'` as the canonical "no metadata yet" value — callers that pass an empty Go string get `'{}'` written. Downstream enrichment can update it; `UpdatedAt` reflects the last update.
- **Per-record writes, idempotent.** `Create` writes a single record. A primary-key conflict is silently ignored, which makes queue-redelivery of the same request a safe no-op. There is no batch atomicity in the contract — callers with multiple URIs loop and rely on idempotency to converge under partial failure / retry.
- **Per-URI reads, no filtering.** `GetByURI` returns every record for a given `(queue, uri)`. The store does not filter by `request_id` or by the owning request's state. Callers that want to skip self filter by `RequestID`; callers that want only live owners consult `RequestStore` for liveness.
- **Append-only by design.** Records are not deleted when the owning request reaches a terminal state; the historical claim is preserved for audit. Duplicate detection filters terminals out at query time via the controller-side liveness check.

## Implementing a Backend

1. Create `extension/changestore/{backend}/` directory.
2. Implement the `ChangeStore` interface.
3. Add schema files under `extension/changestore/{backend}/schema/` if the backend requires them.
