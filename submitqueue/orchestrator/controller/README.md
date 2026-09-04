# Controller Correctness

SubmitQueue controllers reconcile durable state under at-least-once delivery. Messages may be duplicated, delayed, or replayed after only part of an earlier attempt completed, so each controller must classify the state it loads and make its writes and fan-out safe to repeat.

There is no single checkpoint algorithm that applies to every stage. A controller's ordering depends on which facts a downstream consumer requires and whether a replay can reconstruct an output that was lost.

## Reconciliation pattern

A queue controller normally:

1. Decodes the message's thin identity or cross-service payload.
2. Resolves queue-scoped dependencies and reloads authoritative state.
3. Classifies the current state as actionable, already handled, superseded, or invalid.
4. Performs retry-safe preparation and conditional writes.
5. Replays or emits the required fan-out with stable logical identities.
6. Returns `nil` to acknowledge, an error for consumer classification, or calls `delivery.Hold(delayMs)` and returns `nil` to postpone polling work.

States beyond a controller's work are not handled uniformly. Some stages acknowledge because a downstream owner has taken over; others repair advisory records or repeat fan-out. The controller package and tests must document which behavior is correct for each state.

## Persistence and publishing

Persist any fact a downstream consumer must reload before publishing the message that exposes it. For example, a newly accepted speculation path is stored before the build-stage dispatch, and a build record is stored before its build-signal message.

When a durable transition acts as a replayable checkpoint, a controller may write it and then publish. Redelivery can observe the checkpoint and repeat the complete fan-out with stable message identities.

Publish first when the later write would erase the only evidence that an announcement is needed. Request-status and build-status observations use this ordering in selected paths: a failed publish leaves state unchanged so redelivery observes and republishes the same event. Such exceptions must be explicit and idempotent; they are not permission to publish arbitrary downstream work before its prerequisites exist.

## Optimistic locking

Optimistic locking answers whether an entity changed since it was read. It does not decide whether a lifecycle transition is valid.

A controller must write only from states it owns. It computes `newVersion := oldVersion + 1`, passes both versions to storage, and updates its local copy only after the conditional write succeeds. When modifying slices or maps, use a candidate copy whose reference fields are cloned so a failed write cannot mutate the caller's original value.

See the [storage optimistic-locking contract](../../extension/storage/README.md).

## Fan-out and external effects

Every replayed output must preserve the logical identity needed by its consumer: topic, partitioning, correlation ID, and payload semantics. Use a stable intent ID when repeats represent the same hand-off; use a distinct ID only for a deliberate repeat-until-effective repair.

An external effect whose outcome was not recorded cannot be made safe by queue deduplication alone:

```text
provider accepts operation
controller fails before recording the result
```

Such effects require a provider-supported idempotency key, a stable operation identity that can be queried, or an explicit acceptance that duplicate or orphaned work is harmless.

## Speculation example

The speculate topic is a dirty signal for a queue, not a command to transition only the named batch. A run reloads the queue's in-flight batches, dependency outcomes, path sets, and build results; admits any batches still in `Created`; commits outcomes to a fixed point; asks the configured speculator for new proposals; persists changed path sets; and dispatches pending builds.

A batch can advance to land when one passed path matches all settled dependency outcomes. It can also advance while dependencies remain unsettled when passed paths cover every possible outcome of those dependencies, proving that the head passed regardless of how they finish.

Path-set changes are persisted before build dispatch. Terminal outcomes are committed before later decisions derive from them. Selected request-log announcements are intentionally published before their corresponding state write so replay cannot lose the observation.

## Review checklist

For each controller, make these answers clear:

1. What authoritative state is reloaded?
2. Which durable states are actionable, already handled, superseded, or invalid?
3. Which writes and external calls are safe to repeat?
4. Which facts must be durable before each publish?
5. Can lost fan-out be reconstructed with the correct logical identities?
6. Which controller or DLQ path owns terminal recovery?
7. Does a non-terminal poll use `Hold` rather than consuming retry budget?

See the [consumer error contract](../../../platform/consumer/README.md) and the orchestrator's current [`pipeline.go`](../pipeline.go) topology.
