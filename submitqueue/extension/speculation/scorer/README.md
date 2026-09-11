# scorer

A `Scorer` returns how likely a batch is to reach `Succeeded` with its changes landed, as a number between 0.0 and 1.0. `Score(ctx, batch, paths)` is handed the batch identity and that batch's own `SpeculationPathSet` — zero-valued when nothing has speculated on it yet. Callers pass a snapshot they already hold; a scorer must not load the path-set store.

Callers may score every batch a queue is waiting on, so implementations should be cheap. A speculation run scores each batch at most once, but it does not carry results across runs; anything expensive belongs behind the implementation's own cache.

The default `bestfirst` generator ranks on this number. The default scorer is **evidence** wrapping a **base**: heuristic or composite prices the change and ignores `paths`; evidence revises that price from the path set and batch state.

Like the other extensions, a `Scorer` is selected **per queue** by the wiring layer through the `Config` (queue name) and `Factory` interface.

See [doc/rfc/submitqueue/outcome-predictor.md](../../../../doc/rfc/submitqueue/outcome-predictor.md) for the GLM, factor contract, evidence rules, and configuration shape.

## Implementations

**`evidence`** revises a nested base scorer with YAML-configured factors for `pathPassed`, `pathFailed`, `merging`, and `cancelling`. A factor of `1` leaves the base price alone; every factor defaults to `1` until someone sets one. Only paths that assume every dependency succeeds count as path evidence.

**`heuristic`** scores a batch by extracting one number from its changes and matching that against ordered buckets, each mapping a `[Min, Max]` range to a probability. The extraction is a caller-supplied `ValueFunc` over the resolved `entity.BatchChanges`, so the same bucketing works for files touched, lines changed, or any other metric. It ignores `paths`.

**`composite`** runs several named scorers and reduces their scores to one. The reduce function receives the scores keyed by scorer name, so it can weigh sources differently rather than treating them as interchangeable; `Min`, `Max`, and `Avg` are provided. It ignores `paths` except to forward them to children.

## Adding a backend

Create a package under `scorer/<backend>/` whose `New(...)` returns a `scorer.Scorer`, injecting whatever it needs at construction — a nested `Scorer` for evidence, a `changeset.Resolver` to reach the batch's changes, a metrics scope, any client. Do not add a `Config` or `Factory` implementation here; per-queue routing and the factory adapter live in the wiring layer.
