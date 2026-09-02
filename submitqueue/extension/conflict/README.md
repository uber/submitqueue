# Conflict

Vendor-agnostic interface for detecting conflicts between a candidate batch and the batches already in flight.

## Interface

`Analyzer` exposes a single `Analyze` method that takes the candidate batch and the list of in-flight batches it might conflict with. It returns the subset of in-flight batches that conflict with the candidate, each paired with a `ConflictType` describing the kind of conflict. An empty result means the candidate is free to advance independently.

Callers are responsible for filtering out the candidate itself and any terminal batches from the in-flight list before invoking the analyzer. The analyzer itself stays free of lifecycle-transition knowledge. A non-nil error reports that analysis could not be completed; implementations return plain errors and the configured error classifiers decide retryability.

The analyzer does not mutate batch inputs and may be called concurrently. Implementations resolve the batch contents they need through injected dependencies. For example, `pathoverlap` uses a `changeset.Resolver`, whose store-backed implementation reads queue-scoped request and change records.

## Implementations

- [`all/`](all/) — pessimistic stub: reports every in-flight batch as a `ConflictTypeConservative` conflict. Useful as a worst-case baseline and for wiring tests where speculation must serialize.
- [`fake/`](fake/) — wraps another analyzer and optionally injects configured failures for tests and example wiring.
- [`none/`](none/) — optimistic stub: reports no conflicts. Useful as a best-case baseline and for wiring tests where speculation should run all batches in parallel.
- [`pathoverlap/`](pathoverlap/) — resolves changed files and reports overlap conflicts by whole file or parent directory.

## Adding a new backend

1. Create `extension/conflict/{backend}/` with an `Analyzer` implementation.
2. Resolve each `entity.Batch` into whatever signal the backend needs (e.g. changed build targets, files touched, dependency graphs).
3. Emit one `Conflict` per (in-flight batch, detected conflict type). Pick the most specific `ConflictType` your backend can determine; use `ConflictTypeConservative` only when the backend cannot prove the absence of a conflict and falls back to a pessimistic default. Introduce a new `ConflictType` constant when you can classify the conflict more precisely.
4. Return plain errors and let the consumer's configured classifiers determine retry behavior.
