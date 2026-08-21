# RFCs (Request for Comments)

Design documents and technical proposals, grouped by scope. Shared/cross-cutting RFCs live at this level; service-specific RFCs live under a per-service subdirectory (e.g. `submitqueue/`).

## Shared

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery (used by SubmitQueue, Stovepipe, and other repo-local services)
- [Message Queue Contract](messagequeue-contract.md) - How queue payloads are defined (Protobuf, serialized as protobuf JSON), located by audience (external in `api/{domain}/messagequeue/`, internal in `{domain}/core/messagequeue/`), bound to topics (the `topics` proto option), and enforced by Bazel visibility
- [Consumer Gate](consumer-gate.md) - Stopping and starting individual queue controllers at runtime via a consumer-side check: blocked deliveries are recorded as parked and postponed back to the queue (re-checked on redelivery), gate state as a separate extension with a file-based first implementation shared by tests and operators
- [Consumer Hold](consumer-hold.md) - Fourth delivery outcome letting a controller postpone its delivery: the message becomes a partition barrier that pauses consumption for a chosen delay, redelivers in order, and does not count as a failure toward dead-lettering
- [Change URIs](change-uri.md) - Identity of a code change: `scheme://{host[:port]}/{path}` per provider (GitHub PR, Phabricator Diff, git ref/commit) and canonical-form rules
- [Hooks Framework](hook-framework.md) - Fire-and-forget side effects off pipeline lifecycle events: one shared `HookEvent` contract (`api/base/hook/`) published to a durable per-domain hook topic, dispatched by a per-domain stage to a pluggable hook extension (`platform/extension/hook/`) for integrations like warehouse export and code-review notifications

## SubmitQueue

- [Orchestrator Workflow](submitqueue/workflow.md) - Queue-driven controller pipeline from gateway entry through batching, scoring, build, merge, and conclude
- [Gateway History APIs](submitqueue/history-api.md) - Request lifecycle history exposed through separate request ID and change ID endpoints
- [Build Runner](submitqueue/build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage
- [Extension Contract](submitqueue/extension-contract.md) - When extensions take orchestrator identity (request/batch) and resolve granular content themselves vs. take controller-resolved data; revises the BuildRunner base/head contract
- [Gateway Status and List APIs](submitqueue/status-list-api.md) - Gateway-owned request context, materialized current status, sqid or change-URI status lookup, and queue admission listing
- [Speculation](submitqueue/speculation.md) - Why SubmitQueue speculates, the path/tree model, and the two pluggable seams: speculation-tree enumeration and path selection
- [Outcome Predictor](submitqueue/outcome-predictor.md) - How likely a batch is to succeed: a predictor built with a Scorer that multiplies its price by what the pipeline has observed (a build passed, the batch is merging), factors written by hand first and fitted later
- [Best-First Speculation Path Generation](submitqueue/speculation-generator-best-first.md) - The default Generator: per-head lazy streams of flip subsets merged best-first across heads, log-probability ranking, and the strict snapshot contract
- [Modular Queue Wiring](submitqueue/modular-queue-wiring.md) - Declare-don't-assemble engine (`pipeline.Construct`) that unifies topic registry, controller registration, DLQ pairing, and lifecycle ordering into one typed call; services self-declare via Deps struct + Stages slice, hosts own per-queue profiles and transport

## Stovepipe

- [Stovepipe Workflow](stovepipe/workflow.md) - Post-merge validation pipeline overview: ingest, process, build, record greenness, analyze projects, notify downstream
- [Process stage](stovepipe/steps/process.md) - Build-strategy decision, per-queue concurrency gate, backlog coalescing, entity model, platform prerequisites
- [Build stage](stovepipe/steps/build.md) - Trigger-only stage and Stovepipe's URI-based BuildRunner contract
- [Buildsignal stage](stovepipe/steps/buildsignal.md) - Build polling, terminal status persistence, and the handoff to record
- [Record stage](stovepipe/steps/record.md) - Immutable validation facts keyed by `(queue, uri, project)`, monotonic last-green bookmark advancement and ref promotion, and the deferred hook-event and analyze handoffs

## Runway

- [Runway Workflow](runway/workflow.md) - Landing service: merge-conflict checking and merging on behalf of SubmitQueue
