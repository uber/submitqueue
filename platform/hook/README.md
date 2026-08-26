# Hook dispatch

The consumer side of the hooks framework: the stage that turns hook events on a queue into `hook.Hook` calls, and the reconciler for the events that never made it. See [the hooks framework RFC](../../doc/rfc/hook-framework.md) for the design, [`api/base/hook`](../../api/base/hook) for the event contract, and [`platform/extension/hook`](../extension/hook) for the hooks it invokes.

## Why a stage at all

Side effects must never stall or fail the pipeline, and "fire and forget" must not mean lossy — a merge-failure comment that silently never posts is a support ticket. A durable queue between the two resolves the tension: the producer's obligation ends once the event is enqueued, which is fast and local, and everything after that gets real retry semantics and a dead-letter queue.

Calling hooks inline would give up both halves. It couples pipeline latency to whatever an integration talks to, and a crash between the state write and the call drops the notification with nothing to replay.

## The controller

Decode, validate, resolve, invoke. That is the whole stage, and it is the same in every domain — "per-domain" in the RFC is about the topic and the wiring, not the logic. The domain-specific parts are the topic name the host maps `hook` to and the hooks its resolver returns.

Resolution is a `hook.Hooks` call per event, so two queues in one host can run different integrations without running different consumers. The controller itself has no notion of a queue: it hands the resolver the event and runs whatever comes back, which keeps the routing decision in the wiring layer that knows the queue topology.

Resolved hooks run concurrently, and every one of them runs even after another fails — they touch separate systems, so neither a slow nor a broken integration holds up the rest. An event's latency is its slowest hook rather than the sum of all of them. Failures are attributed to the hook that raised them and returned as an `errs.Group`, so the classifier weighs each one on its own merits — a transient failure alongside a permanent one still retries.

Each hook receives the delivery's context and owns the bounds on its own work — a timeout on the call it makes to its own backend. The controller adds no deadline: a context deadline cancels, it does not kill, so it would only constrain hooks that already honor `ctx` — exactly the hooks that can constrain themselves.

The consequence is worth knowing. A hook that blocks forever holds a goroutine, and because the delivery waits for all of them it holds the delivery too, along with every later event for that subject — deliveries for one partition key are handled in order. The event is redelivered once the visibility timeout expires and eventually dead-letters, while the original attempt is still running. That is why honoring `ctx` is part of the `Hook` contract rather than advice. Abandoning the goroutine to free the delivery is the other option and is not taken: it turns a hang into a leak that no longer surfaces as a failed event.

A host with no integrations still registers the stage. Opting in is a topic-key registration, and a registered host never skips an event, so "hooks are off" and "an event was lost" stay distinguishable.

Outcomes:

| Situation | Result |
|---|---|
| Every resolved hook returns nil, or none resolve | Ack. Also how a hook ignores an event — there is no filter API. |
| Any resolved hook returns an error | Nack, retry, and dead-letter past the budget. |
| Payload does not decode, or the envelope is missing `id`/`source`/`type` | Non-retryable, so it dead-letters rather than being silently acked. |

Ordering is per subject only, since the partition key is the subject id. Hook outcomes never write pipeline state.

## The DLQ reconciler

Every other DLQ reconciler in the repo repairs something: a stuck request driven to a terminal `failed`, a batch failed and fanned out. This one repairs nothing, because there is nothing it may touch. A hook never writes pipeline state, so an undelivered hook event leaves no half-finished transition behind. What is lost is the side effect itself, and only a person can decide how to recover it.

So it makes the loss impossible to miss and hands it over: it logs the complete event (the raw protojson, which survives even when the event is here *because* it would not decode) along with the failure attribution, counts it on `reconcile.events_dropped`, and acks so the event does not sit in the DLQ unnoticed. Republishing the logged event recovers it.

`reconcile.events_dropped` is the metric to alert on — it is the only signal that a side effect was lost, since nothing else in the system notices a comment that never posted. That is a deliberate step up from the log topic's DLQ, which warns and moves on: dropping an observability row costs a gap in a read model, which the next write repairs.

The reconciler never returns an error. A DLQ consumer has no DLQ of its own and treats everything as retryable, so anything but an ack loops forever.

## Wiring a host

Register two topics and two controllers:

- the primary `hook` topic, mapped to a topic name unique to this domain if the queue backend is shared, with `NewController` on the regular consumer;
- the derived `hook_dlq` topic, with `NewDLQController` on the DLQ consumer (`DLQSubscriptionConfig` plus `errs.AlwaysRetryableProcessor`, like every other DLQ consumer).

A service assembled by `platform/pipeline` gets the pairing, the derived DLQ key, and the retry configuration from the stage table; Stovepipe and Runway wire their consumers by hand and register both controllers directly.

## Known limit: one retry budget for all hooks

The hooks an event resolves to run under one consumer, so they share one retry budget and one dead-letter fate. A retry re-delivers the event to all of them, including the ones that already succeeded — which the hook contract's idempotency requirement covers — and one chronically failing integration eventually dead-letters events the others handled fine.

Per-hook isolation wants a consumer group per hook on the shared topic, which the queue cannot express today: `NewTopicRegistry` rejects a duplicate topic key and `Consumer.Register` admits one controller per key, so a second group fails at construction; and a rejection moves the shared `queue_messages` row to the DLQ for every group rather than only the one that rejected it. Both have to change before the shared budget can be replaced.
