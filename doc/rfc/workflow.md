# Orchestrator Workflow

The orchestrator processes land requests through a queue-driven pipeline of small, single-purpose controllers. The gateway accepts a request over RPC and hands it off asynchronously; from there each controller consumes one topic, advances the request or batch, and publishes to the next topic. Most hops carry only an ID — the controller fetches the entity from storage — while a few entry points (`start`, `buildsignal`, `log`) carry the full payload because there is no row to fetch yet.

The pipeline has two cycles: `speculate → build → buildsignal → speculate` (CI feedback loop) and `merge → speculate` (advance the next batch). `conclude` is the only stage that transitions a request to a terminal state; `log` is an append-only sink that any controller can publish to via `core/request.PublishLog`.

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
              └─── any controller via core/request.PublishLog ──────┘
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
| **log** | RequestLog | — | Append-only sink for request log events |
