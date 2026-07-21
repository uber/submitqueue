# Storage

Pluggable persistence interfaces for SubmitQueue entities (requests, batches, dependents, logs, etc.). Implementations live under `extension/storage/<impl>/`.

## Optimistic locking contract

Entities that support concurrent mutation carry an `int32 Version` field. Updates are conditional on the version: the write only succeeds if the persisted version matches the caller's expected version. On mismatch, the implementation returns `errs.ErrVersionMismatch`.

**Version arithmetic is owned by the controller, not the store.** Update methods take both `oldVersion` (the where-clause guard) and `newVersion` (the value to write):

```go
UpdateState(ctx, id, oldVersion, newVersion int32, newState entity.RequestState) error
```

The store performs a pure conditional write — it does not compute `oldVersion + 1` internally. This keeps the in-memory entity and the persisted row in sync without the storage layer mutating values the caller didn't supply.

### Caller pattern

```go
newVersion := entity.Version + 1
if err := store.UpdateState(ctx, entity.ID, entity.Version, newVersion, newState); err != nil {
    return err // entity.Version unchanged on failure — safe to retry
}
entity.Version = newVersion // only after the write succeeded
```

The post-success assignment matters whenever the entity is read again later in the same flow. Pre-incrementing in memory before the call is a bug pattern: if the call fails and the caller swallows the error, the in-memory version is now ahead of the database and subsequent updates will fail with `ErrVersionMismatch` for non-obvious reasons.

## Key-value contract

Store interfaces are designed for the storage technology *space*, not for SQL (see the Extensions section of the repo `CLAUDE.md`): every method must be satisfiable by a plain key-value backend (DynamoDB, Bigtable, an in-memory map) as cheaply as by MySQL. Concretely, a store exposes only get/put/conditional-update **by primary key**. No lookups by other attributes, no listings filtered server-side, no joins.

**The smell test is the index.** If implementing a proposed store method in MySQL requires adding a secondary index (`KEY idx_*`) to the schema, the method is a query-by-attribute in disguise and the contract has left the key-value space — a KV backend would need a global secondary index or a hand-maintained index table to fake it. Treat a new `KEY` line in a schema diff as a design review flag, not a tuning detail.

**Reach for the derived-key pattern instead.** When callers need "all X belonging to Y", encode the relationship in the primary key rather than querying for it: derive the key deterministically from the composite identity the caller already holds — for example `{parentID}/{hash(child identity)}`. Every caller that wants the children can recompute the keys and issue per-key reads; creation under a deterministic key is naturally idempotent (a redelivery finds the existing row); and "at most one row per identity" holds by construction instead of by query discipline.

**Domain state is often already the index.** Before adding any lookup, check whether an entity the caller already loads enumerates the children — an aggregate that references its parts by ID (e.g. a tree whose paths record their build identities) is the batch→children index, persisted and versioned as domain state. Duplicating that relationship as a database index adds a second source of truth for something the domain already owns.

**When neither applies, the reverse lookup is real — give it its own mapping store.** In the KV space there is no third mechanism: the only way to look up by an attribute is to make that attribute a primary key somewhere. So promote the relationship to a first-class mapping entity — keyed by the lookup attribute, written by the same flow that creates the source entity with idempotent puts, and rebuildable as a projection if it drifts. `ChangeRecord` is the in-repo example: it exists so "which requests claimed this change URI" is a by-key read on (queue, URI). Unlike a `KEY idx_*`, the relationship is visible in the contract and portable to any backend.

### Decision path

Take the first branch that applies:

1. **Derive** — the caller already holds the composite identity → encode it in the primary key. No new state.
2. **Enumerate** — an entity already on the caller's path references the children by ID → that aggregate is the index. Escalate to 3 if the list would grow unbounded or take appends from many concurrent writers (a version-contention hotspot under optimistic locking).
3. **Map** — a pipeline controller needs the lookup at runtime → a dedicated mapping store keyed by the attribute.

The bar for 3 is a hot-path need: one mapping per access path, never per attribute, and never for ops/debug queries — run those against SQL replicas directly. But don't contort 1–2 to dodge a legitimate 3; a primary key that hashes half the entity's fields, or an aggregate bloated into listing everything, is the same duplication hidden in a worse place.
