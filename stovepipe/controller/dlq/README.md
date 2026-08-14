# DLQ Reconciliation Controllers

This package drains each primary pipeline topic's `{topic}_dlq` companion and drives the affected request to a terminal `failed` state. The controllers are wired alongside the primary ones in `service/stovepipe/server/main.go`.

Callers gate deployments on greenness, so the failure this package exists to prevent is a request that can never finish and leaves a commit with no recorded greenness while still holding its queue's build slot. See [workflow.md](../../../doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work) for the fail-closed posture and [submitqueue/orchestrator/controller/dlq/README.md](../../../submitqueue/orchestrator/controller/dlq/README.md) for the shared reconcile-only design this follows.

## Controller mapping

Every stage that consumes a topic has a reconciler; `ingest` is an RPC entry point with no topic, so it has none.

| Primary stage | DLQ topic | Payload | Reconciler |
|---|---|---|---|
| `process` | `process_dlq` | `ProcessRequest` (request id) | `NewRequestController` + `DecodeProcessRequest` |
| `build` | `build_dlq` | `BuildRequest` (request id) | `NewRequestController` + `DecodeBuildRequest` |
| `buildsignal` | `buildsignal_dlq` | `BuildSignal` (build id) | `NewBuildSignalController` |
| `record` | `record_dlq` | `Record` (request id) | `NewRequestController` + `DecodeRecord` |

Two shapes cover the four topics. The request-scoped stages carry the affected request's id directly, so one reconciler serves all three with the decoder for its stage's payload injected at construction. `buildsignal` carries a build id instead and takes one extra step: read the build to recover its request, then reconcile that.

The three request-scoped payloads are distinct proto messages that currently carry identical fields, which is why the decoder rather than the reconciliation step varies per topic. They are decoded as their own types rather than interchangeably, so that a field added to one stage's contract cannot be misread at another.

## What reconciliation is worth at each stage

The steps are the same everywhere — decode the id, load the request, release its slot if it holds one, CAS it to `failed` — but how much of that is load-bearing depends on how far the request got.

**`process` and `build` are where the slot is at stake.** A request dead-lettering here is `accepted` or `processing`, and a `processing` one holds one of its queue's `in_flight_count` build slots. With `max_concurrent` defaulting to 1, a slot never returned wedges the whole queue, not just the one request. `build` in particular never writes `Request.State` at all, so nothing else in the pipeline will ever look at that request again.

**`record` is the thin case, and deliberately so.** `buildsignal` releases the slot and stamps the build's outcome on the request before it publishes here, so a request reaching `record` is already terminal and already past holding a slot. Reconciliation finds a terminal request and skips it. What it still buys is convergence and visibility: the dead-letter is logged with its originating topic and error, counted, and acked rather than accumulating in a topic nothing consumes. It also closes the one path that can leave a request non-terminal at this stage — `record` rejects a non-terminal request as a broken producer invariant, and that rejection dead-letters.

A `record` dead-letter can therefore leave a validated commit with no fact behind it. That is safe without any action here: absence of a fact is not the same as a green fact, and callers must treat "not yet recorded" as not-green (see [workflow.md](../../../doc/rfc/stovepipe/workflow.md#greenness--a-degree-not-a-boolean)). Writing a broken fact to make that explicit was rejected — facts are immutable and first-writer-wins, so a build that genuinely passed would be permanently recorded as broken, and re-running the stage's work is exactly what a reconciler must not do.

## Convergence guarantee

DLQ subscriptions are configured with `DLQ.Enabled = false` and a very high `Retry.MaxAttempts`, and the DLQ consumer is wired with `errs.AlwaysRetryableProcessor`. Any non-nil error from a reconciler is therefore forced retryable and redelivered rather than dropped, including a decode failure — the recoverable cause there is deployment skew, where a newer producer's payload decodes fine once the rollout finishes.

The residual risk is operational: a genuinely poison message loops until it exhausts the attempt cap, and while it does, the queue it belongs to stays wedged. Monitor and alert on these topics.

Two conditions are treated as success rather than retried: a request that no longer exists (`storage.ErrNotFound`) never persisted and has nothing to reconcile, and a request already in a terminal state is left as it is. `storage.ErrVersionMismatch` on the CAS is returned and redelivered, which is what lets a late successful pipeline write win over the conservative one.

## Slot release ordering

`Queue` and `Request` are separate entities with no cross-entity transaction, so the two writes cannot be atomic and their order picks the crash failure mode. The slot is released first, then the request is marked terminal, because a terminal request is skipped by redelivery and by this reconciler alike — marking it terminal first would leak the slot permanently. Releasing first means a crash between the writes leaves the request non-terminal and redelivery decrements again, transiently over-admitting by one slot until the zero clamp reconverges. Over-admission is the failure mode this pipeline prefers, in the same words `buildsignal` uses for the same ordering; see [process.md](../../../doc/rfc/stovepipe/steps/process.md#in_flight_count-integrity).
