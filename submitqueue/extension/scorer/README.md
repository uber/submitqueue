# scorer

A `Scorer` returns the probability that a batch ultimately succeeds — reaches its terminal `Succeeded` state with its changes landed, not merely a passing build — as a number between 0.0 and 1.0. It is handed the batch identity and resolves the batch's changes itself through an injected `changeset.Resolver`, so callers pass an `entity.Batch` and nothing more.

Callers may score every batch a queue is waiting on, so implementations should be cheap. A speculation run scores each batch at most once, but it does not carry results across runs; anything expensive belongs behind the implementation's own cache.

Like the other extensions, a `Scorer` is selected **per queue** by the wiring layer through the `Config` (queue name) and `Factory` interface.

## Implementations

**`heuristic`** scores a batch by extracting one number from its changes and matching that against ordered buckets, each mapping a `[Min, Max]` range to a probability. The extraction is a caller-supplied `ValueFunc` over the resolved `entity.BatchChanges`, so the same bucketing works for files touched, lines changed, or any other metric.

**`composite`** runs several named scorers and reduces their scores to one. The reduce function receives the scores keyed by scorer name, so it can weigh sources differently rather than treating them as interchangeable; `Min`, `Max`, and `Avg` are provided.

## Adding a backend

Create a package under `scorer/<backend>/` whose `New(...)` returns a `scorer.Scorer`, injecting whatever it needs at construction — a `changeset.Resolver` to reach the batch's changes, a metrics scope, any client. Do not add a `Config` or `Factory` implementation here; per-queue routing and the factory adapter live in the wiring layer.
