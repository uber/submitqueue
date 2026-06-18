# RFCs (Request for Comments)

Design documents and technical proposals, grouped by scope. Shared/cross-cutting RFCs live at this level; service-specific RFCs live under a per-service subdirectory (e.g. `submitqueue/`).

## Shared

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery (used by SubmitQueue, Stovepipe, and other repo-local services)
- [Message Queue Contract](messagequeue-contract.md) - How queue payloads are defined (Protobuf, serialized as protobuf JSON), located by audience (external in `api/{domain}/messagequeue/`, internal in `{domain}/core/messagequeue/`), bound to topics (the `topics` proto option), and enforced by Bazel visibility

## SubmitQueue

- [Orchestrator Workflow](submitqueue/workflow.md) - Queue-driven controller pipeline from gateway entry through batching, scoring, build, merge, and conclude
- [Build Runner](submitqueue/build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage
- [Extension Contract](submitqueue/extension-contract.md) - When extensions take orchestrator identity (request/batch) and resolve granular content themselves vs. take controller-resolved data; revises the BuildRunner base/head contract


## Stovepipe

- [Stovepipe Workflow](stovepipe/workflow.md) - Post-merge trunk-validation pipeline: ingest trunk push events (webhook + fallback poll), batch since last green, build to validate, record per-commit health, bisect to the offending commit, hand off to a remediation extension

## Runway

- [Runway Workflow](runway/workflow.md) - Landing service: merge-conflict checking and merging on behalf of SubmitQueue
