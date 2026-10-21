# Storage

Pluggable persistence interfaces for Stovepipe entities (`RequestStore`, `RequestURIStore`, `QueueStore`, `BuildStore`, `BuildIssueStore`, `OpenIssueStore`). Implementations live under `extension/storage/<impl>/`.

The aggregate is resolved per queue through a factory keyed by queue name, mirroring the extension contract and the [submitqueue storage seam](../../../submitqueue/extension/storage/README.md#queue-scoped-resolution): a resolved instance is bound to its queue, entity arguments whose queue disagrees with the binding are rejected, and reads never surface another queue's records. Stovepipe has no cross-queue read paths, so every store — including `QueueStore`, whose row key is the queue name itself — lives inside the queue-scoped aggregate; there is no global remainder.

This is a separate contract from `submitqueue/extension/storage` — same shape and conventions by design, but its own interfaces and its own `ErrNotFound`/`ErrAlreadyExists`/`ErrVersionMismatch` sentinels, since Stovepipe and SubmitQueue are independent domains. `ErrVersionMismatch` is declared as a retryable infrastructure error so callers can return it without reclassifying it.

## Optimistic locking contract

Entities that support concurrent mutation (`Request`, `Build`) carry an `int32 Version` field. `Update` methods take both `oldVersion` (the where-clause guard) and `newVersion` (the value to write) — the store performs a pure conditional write and never computes `oldVersion + 1` itself. Version arithmetic is owned by the controller: it computes `newVersion`, calls `Update`, and only assigns `entity.Version = newVersion` after the call succeeds. See [AGENTS.md](../../../AGENTS.md) and the [submitqueue storage README](../../../submitqueue/extension/storage/README.md#optimistic-locking-contract) for the full rationale and the caller pattern — the convention is identical here.

## Read-after-write consistency

A `Get` immediately following a successful write (`Create`/`Update`) — by the same caller, or a causally-dependent one such as a queue consumer processing a message published after the write committed — must return that write. This is a requirement on every storage implementation, not a condition callers negotiate around.

**Controllers must not treat `ErrNotFound` as "not visible yet, retry."** The store interface is intentionally general enough to run over any backend, so a controller has no way to know whether a missing row will appear shortly or does not exist at all — retrying on that assumption just reintroduces, in business logic, the consistency gap the storage contract exists to close. If a `Get` misses a row that a causally-prior write should already have produced (e.g. `build` loading the `Request` that `process` published its message for, or `process` loading the `Queue` row `ingest` get-or-created before publishing), that is a storage implementation defect: let the error surface as a normal (non-retryable, per [`platform/errs`](../../../platform/errs/README.md)'s default) failure rather than absorbing it with a retryable wrapper. See [build.md](../../../doc/rfc/stovepipe/steps/build.md#error-classification) and [process.md](../../../doc/rfc/stovepipe/steps/process.md) for the pipeline stages this applies to.

## Key-value contract

Same design space as [`submitqueue/extension/storage`](../../../submitqueue/extension/storage/README.md#key-value-contract): every method must be satisfiable by a plain key-value backend as cheaply as by MySQL — get/put/conditional-update by primary key only, no query-by-attribute or server-side filtering. `RequestURIStore` is the reverse-lookup example here (which request owns a given commit URI), kept as its own store rather than a secondary index on `RequestStore`.

`RequestAcceptanceStore` is the ordered lookup mapping for List: immutable `(queue, accepted_at_ms, request_id)` keys with bounded primary-key range scans, not a secondary index or a second status projection. The request-log materializer ensures each known acceptance has a mapping after summary persistence, including retries where the summary is unchanged. Unknown acceptance times are not mapped; historical backfill is separate.

`QueueStore` owns internal enablement alongside pipeline coordination. `QueueSummaryStore` persists the current read model projected from immutable `QueueLogStore` occurrences. Enablement sequences, operational row versions and projection versions are distinct. These stores use primary-key operations; version arithmetic, duplicate reconciliation and projection repair belong to business logic. See [queue state](../../../doc/rfc/stovepipe/queue-state.md).

`ProjectFactByDegreeStore` is the ordered lookup mapping for named project facts. Its immutable `(queue, URI, degree DESC, project ASC)` primary key stores the original degree and orders by descending degree and ascending bytewise project ID, so listing is a bounded key-range read. It holds no duplicate validation payload; readers hydrate facts by their original keys and verify the mapping. The record controller ensures mappings from persisted winning facts before recording project completion, including retries after partial writes. Create the table before deploying writers. The table starts empty: only results processed by index-aware writers receive mappings, with no historical population or read-time fallback.

## Build issue observations

`BuildIssueStore` accumulates immutable observations identified by build, task, and unit within the bound queue. The build ID is supplied separately from the detected issue. Repeated observations return `ErrAlreadyExists`, which callers may treat as successful persistence. Empty unit IDs represent task-wide observations.

Each observation is stored separately, not in a serialized collection. Observations are historical evidence; they do not track whether an issue is currently open or resolved. No update, deletion, or absence-based recovery is implied by this store.

The MySQL primary key contains the queue, build ID, task ID, and unit ID directly as binary strings, preserving byte-exact identity. Their limits are 255, 1020, 255, and 1024 bytes respectively, for a combined maximum of 2554 bytes. Creates and reads reject identifiers exceeding these limits before querying the database; identifiers are never truncated.

## Currently open issues

`OpenIssueStore` contains only currently open issues, keyed by task and unit within the bound queue. Removing an open row never removes immutable per-build observations. There is no open/resolved flag; a resolved issue has no row here.

An open row is immutable. Its first-seen build identifies the current opening, not its latest observation. Repeated observations remain in the per-build store. Reopening after removal uses a different first-seen build ID; no last-observed build, version counter, or update operation is stored here.

Deletion matches the task, unit, and first-seen build within the bound queue. It is safe to repeat: an absent opening is a no-op, and a stale deletion never removes a different opening of the same condition. Database failures are returned for retry.

MySQL identifiers use the same binary byte limits as per-build observations. Build IDs are opaque context, not ordering tokens. The store does not choose resolution, enumerate issues, write lifecycle history, or reconcile analysis responses. History and source-order reconciliation belong to later controllers, not this membership store.
