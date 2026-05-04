# Storage

Pluggable persistence interfaces for SubmitQueue entities (requests, batches, dependents, logs, etc.). Implementations live under `extension/storage/<impl>/`.

## Optimistic locking contract

Entities that support concurrent mutation carry an `int32 Version` field. Updates are conditional on the version: the write only succeeds if the persisted version matches the caller's expected version. On mismatch, the implementation returns `storage.ErrVersionMismatch`.

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
