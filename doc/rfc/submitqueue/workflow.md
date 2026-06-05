# Orchestrator Workflow

The orchestrator processes land requests through a queue-driven pipeline of small, single-purpose controllers. The gateway accepts a request over RPC and hands it off asynchronously; from there each controller consumes one topic, advances the request or batch, and publishes to the next topic. Most hops carry only an ID — the controller fetches the entity from storage — while a few entry points (`start`, `buildsignal`, `log`) carry the full payload because there is no row to fetch yet.

The pipeline has two cycles: `speculate → build → buildsignal → speculate` (CI feedback loop) and `merge → speculate` (advance the next batch). `conclude` is the only stage that transitions a request to a terminal state; `log` is an append-only sink that any controller can publish to via `submitqueue/core/request.PublishLog`.

## Diagram

```
                    ┌──────────────────────────────────┐
                    │ gateway:Land  (RPC entry)        │
                    │ Accept, mint ID, hand off async  │
                    └────────────────┬─────────────────┘
                                     │ LandRequest
                                     ▼
   ┌──────────────────────┐    ┌──────────────────────────────────┐
   │ log  (terminal sink) │◄───│ start                            │
   │ Append RequestLog    │    │ Persist Request, emit Started    │
   └──────────────────────┘    └────────────────┬─────────────────┘
              ▲                                 │ RequestID
              │                                 ▼
              │                 ┌──────────────────────────────────┐
              │                 │ validate                         │
              │                 │ Check mergeability + change meta │
              │                 └────────────────┬─────────────────┘
              │                                  │ RequestID
              │                                  ▼
              │                 ┌──────────────────────────────────┐
              │                 │ batch                            │
              │                 │ Group request into a Batch       │
              │                 └────────────────┬─────────────────┘
              │                                  │ BatchID
              │                                  ▼
              │                 ┌──────────────────────────────────┐
              ├─────────────────│ score                            │
              │   RequestLog×N  │ Score the batch, persist score   │
              │                 └────────────────┬─────────────────┘
              │                                  │ BatchID
              │                                  ▼
              │            ┌──────────────────────────────────┐
              │       ┌───►│ speculate  (stub)                │◄────┐
              │       │    │ Decide CI verify vs. land        │     │
              │       │    └──────┬─────────────────┬─────────┘     │
              │       │  BatchID  │                 │ BatchID       │
              │       │           ▼                 ▼               │
              │       │  ┌──────────────────┐  ┌──────────────────┐ │
              │       │  │ build            │  │ merge            │ │
              │       │  │ Trigger CI build │  │ Merge + advance  │─┤
              │       │  └────────┬─────────┘  └────────┬─────────┘ │
              │       │   Build   │                     │ BatchID   │
              │       │           ▼                     │           │
              │       │  ┌──────────────────┐           │           │
              │       └──│ buildsignal      │           │           │
              │  BatchID │ Feed CI result   │           │           │
              │          │ back to spec.    │           │           │
              │          └──────────────────┘           │           │
              │                  ▲                      │ BatchID   │
              │                  │ Build (ext. CI)      ▼           │
              │                  │            ┌──────────────────┐  │
              │                  │            │ conclude         │  │
              │                  │            │ Map batch state  │  │
              │                  │            │ → request state  │  │
              │                  │            └──────────────────┘  │
              │                  │                                  │
              └─── any controller via submitqueue/core/request.PublishLog ──────┘
```

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **gateway/Land** | RPC | start | Accept request, mint ID, log Accepted, hand off async |
| **start** | LandRequest | validate, log | Persist Request and emit Started log |
| **validate** | RequestID | batch | Check mergeability and fetch change metadata |
| **batch** | RequestID | score | Group request into a Batch with dependencies |
| **score** | BatchID | speculate, log | Score the batch (∏ per-request scores), persist score |
| **speculate** | BatchID | build, merge | (stub) Decide whether to verify via CI or land |
| **build** | BatchID | buildsignal | Trigger CI build for the batch |
| **buildsignal** | Build | speculate | Feed CI result back into speculation |
| **merge** | BatchID | conclude, speculate | Merge the batch and advance the queue |
| **conclude** | BatchID | — | Map terminal batch state to request state |
| **log** | RequestLog | — | Gateway-owned sink: persists request log events to storage |

## DLQ reconciliation

Every *consumed* primary pipeline topic above is paired with a `{topic}_dlq` subscription consumed by a dedicated DLQ controller. The `log` topic is the exception: the orchestrator only publishes to it (the gateway is the sole consumer that persists the request log), so it has no orchestrator-side subscription and therefore no DLQ. The consumer framework moves a message to its DLQ once the primary controller returns a non-retryable error or exhausts retries on a retryable one; without the DLQ side the affected request would stay in a non-terminal state forever and the gateway would still report it as "in progress".

The DLQ controllers do not re-attempt the failed work. They decode the payload to recover the affected `RequestID` (start, validate, batch, cancel) or `BatchID` (score, speculate, build, buildsignal, merge, conclude) and drive the entity to a terminal failed state — `RequestStateError` for requests, `BatchStateFailed` for batches with fan-out to the member requests. State writes use the same optimistic-locking CAS as the primary pipeline, so a late primary-pipeline update wins cleanly and a version mismatch is asked back for redelivery.

DLQ consumers are wired with `errs.AlwaysRetryableProcessor` and a very high `Retry.MaxAttempts`, with their own DLQ disabled. That combination makes reconciliation effectively non-droppable: any failure is forced retryable rather than escalating to a second-level dead-letter that nobody consumes. The trade-off is that a genuinely unprocessable DLQ message — typically a malformed payload — must be removed by an operator.

See `submitqueue/orchestrator/controller/dlq/README.md` for the design constraints (simplest possible implementation, reconcile-only, no recovery) and the per-topic controller mapping.
