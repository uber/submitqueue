# Storage

Pluggable persistence interfaces for Stovepipe entities (`RequestStore`, `RequestURIStore`, `QueueStore`, `BuildStore`). Implementations live under `extension/storage/<impl>/`. This is a separate contract from `submitqueue/extension/storage` — same shape and conventions by design, but its own interfaces and its own `ErrNotFound`/`ErrAlreadyExists`/`ErrVersionMismatch` sentinels, since Stovepipe and SubmitQueue are independent domains.

## Optimistic locking contract

Entities that support concurrent mutation (`Request`, `Build`) carry an `int32 Version` field. `Update` methods take both `oldVersion` (the where-clause guard) and `newVersion` (the value to write) — the store performs a pure conditional write and never computes `oldVersion + 1` itself. Version arithmetic is owned by the controller: it computes `newVersion`, calls `Update`, and only assigns `entity.Version = newVersion` after the call succeeds. See [CLAUDE.md](../../../CLAUDE.md) and the [submitqueue storage README](../../../submitqueue/extension/storage/README.md#optimistic-locking-contract) for the full rationale and the caller pattern — the convention is identical here.

## Read-after-write consistency

A `Get` immediately following a successful write (`Create`/`Update`) — by the same caller, or a causally-dependent one such as a queue consumer processing a message published after the write committed — must return that write. This is a requirement on every storage implementation, not a condition callers negotiate around.

**Controllers must not treat `ErrNotFound` as "not visible yet, retry."** The store interface is intentionally general enough to run over any backend, so a controller has no way to know whether a missing row will appear shortly or does not exist at all — retrying on that assumption just reintroduces, in business logic, the consistency gap the storage contract exists to close. If a `Get` misses a row that a causally-prior write should already have produced (e.g. `build` loading the `Request` that `process` published its message for, or `process` loading the `Queue` row `ingest` get-or-created before publishing), that is a storage implementation defect: let the error surface as a normal (non-retryable, per [`platform/errs`](../../../platform/errs/README.md)'s default) failure rather than absorbing it with a retryable wrapper. See [build.md](../../../doc/rfc/stovepipe/steps/build.md#error-classification) and [process.md](../../../doc/rfc/stovepipe/steps/process.md) for the pipeline stages this applies to.

## Key-value contract

Same design space as [`submitqueue/extension/storage`](../../../submitqueue/extension/storage/README.md#key-value-contract): every method must be satisfiable by a plain key-value backend as cheaply as by MySQL — get/put/conditional-update by primary key only, no query-by-attribute or server-side filtering. `RequestURIStore` is the reverse-lookup example here (which request owns a given commit URI), kept as its own store rather than a secondary index on `RequestStore`.
