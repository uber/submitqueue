# ChangeStore

Vendor-agnostic interface for tracking per-URI claims by in-flight land requests.

Each record asserts that a specific URI (e.g., a GitHub PR) was claimed by a specific request, scoped to a queue. The store is consulted by the orchestrator's `start` controller to detect duplicate requests — submissions whose URIs overlap with another in-flight request's URIs in the same queue.

## Semantics

- **Identity is immutable.** A record is keyed by `(URI, RequestID)`; once written, that pair is never mutated.
- **Metadata is mutable.** The `Metadata` field is intended for provider-specific information about the change (PR title, author, mergeability, etc.) that may be enriched after the record is first written. `UpdatedAt` reflects the last metadata change; `CreatedAt` is fixed at write time.
- **Idempotent writes.** `Create` ignores primary-key conflicts so queue-redelivery of the same request is a safe no-op.
- **No liveness filter.** `FindOverlapping` returns records regardless of whether the owning request is still in flight. Callers must check liveness against `RequestStore` themselves — the store boundary is intentionally one query, one table, no joins.
- **Append-only by design.** Records are not deleted when the owning request reaches a terminal state; the historical claim is preserved for audit. Duplicate detection filters terminals out at query time via the controller-side liveness check.

## Implementing a Backend

1. Create `extension/changestore/{backend}/` directory.
2. Implement the `ChangeStore` interface.
3. Add schema files under `extension/changestore/{backend}/schema/` if the backend requires them.
