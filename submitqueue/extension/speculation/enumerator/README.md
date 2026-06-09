# Speculation Tree Enumerator

Vendor-agnostic interface for enumerating the **speculation tree** of a batch — the set of candidate speculation paths the orchestrator may build.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how enumeration fits into the orchestrator pipeline.

## Enumerator

An enumerator is deliberately **dumb** and purely **structural**: *given a batch and its active dependency batches, it mechanically lists the candidate paths.* It does **not** score paths — that is the [scorer](../scorer)'s job, which the controller re-runs on every respeculate — it does **not** decide which paths to build — that is the [selector](../selector)'s job — it does **not** set path status, and it does **not** decide how far back to speculate. The dependency limit is the controller's responsibility: the controller gates a batch on the limit and hands the enumerator exactly the active dependencies to speculate over, which it then enumerates over verbatim.

Each candidate is a path: an assumed-good prefix of predecessor batches (the base) on top of which the batch under verification (the head) is built. The base maps directly onto the build stage's base changes and the head onto the changes being validated.

Enumeration is **pure and deterministic**: the same batch and dependency list always produce the same tree. This lets the controller regenerate a tree whenever the dependency graph changes without tracking incremental state in the enumerator. Keeping enumeration tractable for a very wide dependency list is the enumerator's only real concern.

The returned paths carry structure only — a Base/Head split, with `Score` and `Status` left unset. The controller stamps `Status` when it persists the tree and calls the scorer to fill `Score`; enumeration produces neither.

## Factory

A per-queue factory returns the enumerator for a queue, following the repo's extension contract. It is handed only the queue identity and nothing else; everything an implementation needs — including behavioral knobs like enumeration breadth — is injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. Enumeration itself stays config-free.
