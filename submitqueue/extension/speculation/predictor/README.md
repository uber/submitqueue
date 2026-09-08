# predictor

A `Predictor` returns how likely a batch is to reach `Succeeded` with its changes landed, given both what it changes and what this speculate run has already observed. It is built over the queue's `Scorer`, which prices the change from content signals; the predictor revises that price with path-set evidence and batch state.

`Predict` is handed the batch identity and that batch's own `SpeculationPathSet` — zero-valued when nothing has speculated on it yet. Callers may predict every unresolved dependency a queue waits on, so anything expensive belongs behind the implementation's own cache.

Like the other extensions, a `Predictor` is selected **per queue** by the wiring layer through the `Config` (queue name) and `Factory` interface. The default `standard` `Speculator` composes its `Generator` over the queue's predictor, which in turn composes over the queue's scorer.

See [doc/rfc/submitqueue/outcome-predictor.md](../../../../doc/rfc/submitqueue/outcome-predictor.md) for the factor contract, evidence rules, and configuration shape.

## Implementations

**`evidence`** revises the scorer's price with YAML-configured factors for `pathPassed`, `pathFailed`, `merging`, and `cancelling`. A factor of `1` leaves the scorer's price alone; every factor defaults to `1` until someone sets one. Only paths that assume every dependency succeeds count as path evidence.

## Adding a backend

Create a package under `predictor/<backend>/` whose `New(...)` returns a `predictor.Predictor`, injecting whatever it needs at construction — typically the queue's `Scorer`, factor configuration, and a metrics scope. Do not add a `Config` or `Factory` implementation here; per-queue routing and the factory adapter live in the wiring layer.
