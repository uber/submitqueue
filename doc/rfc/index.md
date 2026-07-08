# RFCs (Request for Comments)

Design documents and technical proposals, grouped by scope. Shared/cross-cutting RFCs live at this level; service-specific RFCs live under a per-service subdirectory (e.g. `submitqueue/`).

## Shared

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery (used by SubmitQueue, Stovepipe, and other repo-local services)
- [Message Queue Contract](messagequeue-contract.md) - How queue payloads are defined (Protobuf, serialized as protobuf JSON), located by audience (external in `api/{domain}/messagequeue/`, internal in `{domain}/core/messagequeue/`), bound to topics (the `topics` proto option), and enforced by Bazel visibility
- [Change URIs](change-uri.md) - Identity of a code change: `scheme://{host[:port]}/{path}` per provider (GitHub PR, Phabricator Diff, git ref/commit) and canonical-form rules

## SubmitQueue

- [Orchestrator Workflow](submitqueue/workflow.md) - Queue-driven controller pipeline from gateway entry through batching, scoring, build, merge, and conclude
- [Gateway History APIs](submitqueue/history-api.md) - Request lifecycle history exposed through separate request ID and change ID endpoints
- [Build Runner](submitqueue/build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage
- [Extension Contract](submitqueue/extension-contract.md) - When extensions take orchestrator identity (request/batch) and resolve granular content themselves vs. take controller-resolved data; revises the BuildRunner base/head contract
- [Gateway Status and List APIs](submitqueue/status-list-api.md) - Gateway-owned request context, materialized current status, sqid or change-URI status lookup, and queue admission listing
- [Speculation](submitqueue/speculation.md) - Why SubmitQueue speculates, the path/tree model, and the two pluggable seams: speculation-tree enumeration and path selection

## Stovepipe

- [Stovepipe Workflow](stovepipe/workflow.md) - Post-merge validation pipeline overview: ingest, process, build, record greenness, analyze projects, notify downstream
- [Process stage](stovepipe/steps/process.md) - Build-strategy decision, per-queue concurrency gate, backlog coalescing, entity model, platform prerequisites

## Runway

- [Runway Workflow](runway/workflow.md) - Landing service: merge-conflict checking and merging on behalf of SubmitQueue
