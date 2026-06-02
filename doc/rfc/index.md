# RFCs (Request for Comments)

Design documents and technical proposals for SubmitQueue.

## Index

- [SQL-Based Distributed Queue](sql-queue-rfc.md) - MySQL-based distributed message queue with partition leasing and at-least-once delivery
- [Orchestrator Workflow](workflow.md) - Queue-driven controller pipeline from gateway entry through batching, scoring, build, merge, and conclude
- [Build Runner](build-runner.md) - Vendor-agnostic BuildRunner interface, provider-neutral BuildStatus lifecycle, and how the orchestrator wires it into the build stage
