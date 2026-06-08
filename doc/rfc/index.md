# RFCs (Request for Comments)

Design documents and technical proposals, grouped by scope. Shared/cross-cutting RFCs live at this level; service-specific RFCs live under a per-service subdirectory (e.g. `submitqueue/`).

## Shared

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery (used by SubmitQueue, Stovepipe, and other repo-local services)

## SubmitQueue

- [Orchestrator Workflow](submitqueue/workflow.md) - Queue-driven controller pipeline from gateway entry through batching, scoring, build, merge, and conclude
- [Build Runner](submitqueue/build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage

## Stovepipe

- [Stovepipe Workflow](stovepipe/workflow.md) - Post-merge trunk-validation pipeline: ingest trunk push events (webhook + fallback poll), batch since last green, build to validate, record per-commit health, bisect to the offending commit, hand off to a remediation extension
