# Observability

Interface through which Stovepipe reports the **current health of a queue**. The pipeline already persists everything it decides — validation facts, the last-green bookmark — but persisted state answers questions only when someone queries the store. A `Reporter` samples that state and emits it as metrics, so queue health is visible on a dashboard and alertable without a query.

A `Reporter` is **bound to a single queue** when its `Factory` constructs it from a `Config`, so `Report` takes no queue argument. Per the repository's extension rules, this package holds the `Reporter` interface, its `Config`, and the `Factory` *interface* only — concrete implementations and the per-queue routing that picks one for a `Config.QueueName` live in the wiring layer.

## Behavior

- **Report** observes the queue and emits one sample of its current state. It is called wherever an observation is worth taking: after `record` handles a message, and on whatever schedule the integrator's wiring runs.

## Errors

`Report` returns nothing. Reporting is best-effort by contract: an observation that cannot be made must never change what the pipeline records or decides, so implementations emit their own error metrics instead of returning. Callers can therefore `defer` a report without handling a result. `Factory.For` does return an error, because failing to resolve a queue's dependencies is a wiring or configuration fault rather than a missed sample.

## Implementations

- **lastgreen** — reports how old the queue's last-known-green commit is, the staleness of the newest commit callers are allowed to ship.

To add a reporter, create `observability/{name}/`, implement the `Reporter` and `Factory` interfaces, and return them from `New(...)` / `NewFactory(...)` constructors.
