# Conflict

Vendor-agnostic interface for detecting conflicts between a candidate batch
and the batches already in flight.

## Interface

`Analyzer` exposes a single `Analyze` method that takes the candidate batch's
changes and the list of in-flight batches' changes it might conflict with.
Inputs are `entity.BatchChanges` — each batch's flattened change facts (URIs +
provider details), assembled by the caller — so the analyzer sees the actual
changes rather than just batch IDs, and can reason about changed targets/files
without reading storage itself. It returns the subset of in-flight batches that
conflict with the candidate, each paired with a `ConflictType` describing the
kind of conflict (referenced by `BatchChanges.BatchID`). An empty result means
the candidate is free to advance independently.

Callers are responsible for assembling `entity.BatchChanges` (see
`submitqueue/core/batchchanges`), and for filtering out the candidate itself and
any terminal batches from the in-flight list before invoking the analyzer. The
analyzer itself stays free of lifecycle knowledge. A non-nil error reports an
infrastructure failure of the analysis and should be treated as retryable by the
caller.

The analyzer is intentionally pure with respect to storage: it does not mutate
inputs, does not read storage, and may be called concurrently. A real
implementation (e.g. one backed by uber/tango) derives changed build
targets/edges from the change set — `Changes[].URI` carries the repo + head
commit SHA, and the target branch is injected per queue at construction — and
returns as much classification detail as that system supports.

## Implementations

- [`all/`](all/) — pessimistic stub: reports every in-flight batch as a
  `ConflictTypeConservative` conflict. Useful as a worst-case baseline and
  for wiring tests where speculation must serialize.
- [`none/`](none/) — optimistic stub: reports no conflicts. Useful as a
  best-case baseline and for wiring tests where speculation should run all
  batches in parallel.

## Adding a new backend

1. Create `extension/conflict/{backend}/` with an `Analyzer` implementation.
2. Derive whatever signal the backend needs from each `entity.BatchChanges`
   (e.g. changed build targets, files touched, dependency graphs) — the change
   URIs and provider details are in hand; resolve the rest via your upstream
   system.
3. Emit one `Conflict` per (in-flight batch, detected conflict type). Pick
   the most specific `ConflictType` your backend can determine; use
   `ConflictTypeConservative` only when the backend cannot prove the absence
   of a conflict and falls back to a pessimistic default. Introduce a new
   `ConflictType` constant when you can classify the conflict more precisely.
4. Return a plain error for transient infrastructure failures so callers
   can classify and retry.
