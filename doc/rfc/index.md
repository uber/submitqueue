# RFCs (Request for Comments)

Design documents and technical proposals, grouped by scope. Shared/cross-cutting RFCs live at this level; service-specific RFCs live under a per-service subdirectory (e.g. `submitqueue/`).

## Shared

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery (used by SubmitQueue, Stovepipe, and other repo-local services)
- [Message Queue Tenant Sharding](messagequeue-tenant-sharding.md) - Per-tenant shard key on the platform MySQL message queue; SubmitQueue maps `queueName` to `tenant` at wiring
- [Message Queue Contract](messagequeue-contract.md) - How queue payloads are defined (Protobuf, serialized as protobuf JSON), located by audience (external in `api/{domain}/messagequeue/`, internal in `{domain}/core/messagequeue/`), bound to topic keys (the `topic_keys` proto option), and enforced by Bazel visibility
- [Consumer Gate](consumer-gate.md) - Stopping and starting individual queue controllers at runtime via a consumer-side check: blocked deliveries are recorded as parked and postponed back to the queue (re-checked on redelivery), gate state as a separate extension with a file-based first implementation shared by tests and operators
- [Consumer Hold](consumer-hold.md) - Fourth delivery outcome letting a controller postpone its delivery: the message becomes a partition barrier that pauses consumption for a chosen delay, redelivers in order, and does not count as a failure toward dead-lettering
- [Change URIs](change-uri.md) - Identity of a code change: `scheme://{host[:port]}/{path}` per provider (GitHub PR, Phabricator Diff, git ref/commit) and canonical-form rules
- [Scoped Sequential Resource IDs](scoped-resource-ids.md) - Queue-scoped positive numeric IDs allocated by durable per-domain, per-kind counters, stored without queue/kind prefixes, and rendered directly in resource URL segments
- [Hooks Framework](hook-framework.md) - Implemented fire-and-forget side effects: one shared `HookEvent` contract (`api/base/hook/`) on a durable per-domain hook topic, dispatched by `platform/hook` to `platform/extension/hook`. Stovepipe `process` and `record` publish repository events; the SubmitQueue orchestrator registers the stage and does not publish events yet
- [Service-Scoped Extensions](service-scoped-extensions.md) - Implemented for SubmitQueue storage: gateway and orchestrator aggregates, schemas, and the core packages that serve one service have moved, while store contracts stay at `submitqueue/extension/storage`. Domain-level `buildrunner`, `conflict`, and `speculation` have not moved, and `changeset` still declares its own store slice

## SubmitQueue

- [Orchestrator Workflow](submitqueue/workflow.md) - Queue-driven orchestrator pipeline: start, cancel, validate, Runway conflict check, batch, dependency analysis, speculate, build, Runway land, conclude, and a hook stage with no orchestrator publisher yet
- [Gateway History APIs](submitqueue/history-api.md) - Request lifecycle history exposed through separate request ID and change ID endpoints
- [Build Runner](submitqueue/build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage
- [Extension Contract](submitqueue/extension-contract.md) - When extensions take orchestrator identity (request/batch) and resolve granular content themselves vs. take controller-resolved data; revises the BuildRunner base/head contract
- [Gateway Status and List APIs](submitqueue/status-list-api.md) - Gateway-owned request context, materialized current status, sqid or change-URI status lookup, and queue admission listing
- [Speculation](submitqueue/speculation.md) - Why SubmitQueue speculates, the path/tree model, and the two pluggable seams: speculation-tree enumeration and path selection
- [Outcome Scorer](submitqueue/outcome-scorer.md) - How likely a batch is to reach Succeeded: `Score(ctx, batch, paths)` as a logit-linear model — a base content price plus YAML weights on path and batch evidence (`pathPassed`, `pathFailed`, `landing`, `cancelling`)
- [Best-First Speculation Path Generation](submitqueue/speculation-generator-best-first.md) - The default Generator: per-head lazy streams of flip subsets merged best-first across heads, log-probability ranking, and the strict snapshot contract
- [Modular Queue Wiring](submitqueue/modular-queue-wiring.md) - `pipeline.Construct` for the SubmitQueue orchestrator (`Deps` + `Stages` in `submitqueue/orchestrator/pipeline.go`, host in `service/submitqueue/orchestrator/server`). Stovepipe, and the gateway and Runway servers, remain hand-wired

## Stovepipe

- [Stovepipe Workflow](stovepipe/workflow.md) - Implemented post-land pipeline: ingest, process, build, buildsignal, record, hook. `record` resolves project results inline. An analyze stage and topic are design only and are not built
- [Process stage](stovepipe/steps/process.md) - Build-strategy decision, per-queue concurrency gate, backlog coalescing, entity model, platform prerequisites
- [Build stage](stovepipe/steps/build.md) - Trigger-only stage and Stovepipe's URI-based BuildRunner contract
- [Buildsignal stage](stovepipe/steps/buildsignal.md) - Build polling, terminal status persistence, and the handoff to record
- [Record stage](stovepipe/steps/record.md) - Immutable validation facts keyed by `(queue, uri, project)`, monotonic last-green bookmark advancement and ref promotion, and the repository hook event. The analyze-stage handoff in that doc is design only
- [Request Log](stovepipe/request-log.md) - Append-only request lifecycle log, durable source context, idempotent storage, and reliable write and repair paths
- [Request History API](stovepipe/request-history-api.md) - Queue-scoped request-ID and URI lookup, public projection, materialization decision, ordering, and retention
- [List API](stovepipe/list-api.md) - Proposed request-ID and acceptance-time listing, immutable acceptance time in summaries, cursor pagination, and a portable time-to-request mapping
- [GetProjectStatusByURI API](stovepipe/get-project-status-by-uri-api.md) - Queue-scoped current validation lookup for a commit, with repository and future project-level results

## Runway

- [Runway Workflow](runway/workflow.md) - Merge service: merge-conflict checking and merging on behalf of SubmitQueue
