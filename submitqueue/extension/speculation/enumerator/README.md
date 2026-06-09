# Speculation Tree Enumerator

Vendor-agnostic interface for enumerating the **speculation tree** of a batch — the set of candidate speculation paths the orchestrator may build, each scored with its predicted probability of success.

See the [Speculation RFC](../../../../doc/rfc/submitqueue/speculation.md) for the end-to-end design and how enumeration fits into the orchestrator pipeline.

## Enumerator

An enumerator is deliberately **dumb**: *given a batch and its dependency batches, it mechanically lists the candidate paths and scores them.* It does **not** decide which paths to build — that is the [selector](../selector)'s job — it does **not** set path status, and it does **not** decide how far back to speculate. Speculation depth is the controller's responsibility: the controller trims the dependency list before calling the enumerator, which then enumerates over exactly the list it is handed.

Each candidate is a path: an assumed-good prefix of predecessor batches (the base) on top of which the batch under verification (the head) is built. The base maps directly onto the build stage's base changes and the head onto the changes being validated.

Enumeration is **pure and deterministic**: the same batch and dependency list always produce the same tree. This lets the controller regenerate a tree whenever the dependency graph changes without tracking incremental state in the enumerator. Keeping enumeration tractable for a very wide dependency list is the enumerator's only real concern.

Scores ride in on the inputs. Each dependency is passed as a full `entity.Batch`, which already carries its per-batch success probability (`Batch.Score`) from the score stage; the enumerator combines the scores of a path's base batches into the path's score. No separate scoring backend or injected probability source is needed, and tests just set `.Score` on literal batches. The head is passed as an ID — its score is constant across all of its own paths.

## Factory

`Factory.For(Config) (Enumerator, error)` returns the enumerator for a queue, following the repo's extension contract (`conflict.Analyzer` is the reference shape). `Config` carries only the queue identity (`QueueName`); the system hands the factory nothing else. Everything an implementation needs — including behavioral knobs like speculation depth — is injected at construction by the integrator in the wiring layer, which resolves per-queue settings through `queueconfig`. `Enumerate` itself stays config-free.
