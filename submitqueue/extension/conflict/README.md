# Conflict

Vendor-agnostic interface for detecting conflicts between a candidate batch
and the batches already in flight.

## Interface

`Analyzer` exposes a single `Analyze` method that takes the candidate batch
and the list of in-flight batches it might conflict with. It returns the
subset of in-flight batches that conflict with the candidate, each paired
with a `ConflictType` describing the kind of conflict. An empty result means
the candidate is free to advance independently.

Callers are responsible for filtering out the candidate itself and any
terminal batches from the in-flight list before invoking the analyzer. The
analyzer itself stays free of lifecycle knowledge. A non-nil error reports
an infrastructure failure of the analysis and should be treated as
retryable by the caller.

The analyzer is intentionally pure with respect to batch state: it does not
mutate inputs, does not read storage, and may be called concurrently. Real
implementations are expected to resolve the batch contents (e.g. changed
build targets, modified files) via whichever upstream system they depend
on, and to return as much classification detail as that system supports.

## Implementations

- [`all/`](all/) — pessimistic stub: reports every in-flight batch as a
  `ConflictTypeConservative` conflict. Useful as a worst-case baseline and
  for wiring tests where speculation must serialize.
- [`none/`](none/) — optimistic stub: reports no conflicts. Useful as a
  best-case baseline and for wiring tests where speculation should run all
  batches in parallel.

## Adding a new backend

1. Create `extension/conflict/{backend}/` with an `Analyzer` implementation.
2. Resolve each `entity.Batch` into whatever signal the backend needs
   (e.g. changed build targets, files touched, dependency graphs).
3. Emit one `Conflict` per (in-flight batch, detected conflict type). Pick
   the most specific `ConflictType` your backend can determine; use
   `ConflictTypeConservative` only when the backend cannot prove the absence
   of a conflict and falls back to a pessimistic default. Introduce a new
   `ConflictType` constant when you can classify the conflict more precisely.
4. Return a plain error for transient infrastructure failures so callers
   can classify and retry.
