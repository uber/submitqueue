# Lenses

Paste one brief to one reviewer. A brief is the whole assignment: the reviewer has not seen the conversation that wrote the change, and it does not edit files.

Generated files (`protopb/`, `mock/`, `*_mock.go`, `*.pb.go`, `*.pb.yarpc.go`) are not reviewed line by line. Say only whether they look stale relative to their source.

Report a finding only when the changed code creates a reachable material impact. For each item give severity (`blocker`, `should-fix`, or `nit` when requested), confidence (`high`, `medium`, or `low`), changed `path:line`, observable impact, the shortest reproducing scenario or static caller trace, supporting locations, the smallest safe fix in this change, and a specific verification step. Cite the `AGENTS.md` section when it explains the engineering constraint, but a rule violation without behavioral, contract, verification, or maintenance impact is a nit and is omitted unless requested.

When impact or reachability cannot be established, report a question with the exact missing fact. A material question makes review coverage incomplete. Inspect the surrounding implementation and tests needed to support the claim; the changed line alone is not evidence of a system-level defect. Do not restate a mechanical failure (formatting, gazelle, mocks, lint, or a failed test). If nothing is wrong, answer `No findings`.

Confirm or drop every candidate smell the prompt forwards. A smell is not a finding until the code supports it.

## Architecture and boundaries

Covers Design and Separation of Concern. Read `AGENTS.md` on versioned state, controllers, queue payloads, and extensions.

- Versioned snapshots. Version arithmetic stays in the controller: `newVersion = oldVersion + 1`, and `Version` is assigned only after the store write succeeds.
- Persist before publish. The exceptions are a status log of a transition, and an idempotent nudge whose consumer re-derives everything from current state. See `submitqueue/orchestrator/controller/speculate/finalize.go` and `submitqueue/orchestrator/controller/buildsignal/buildsignal.go`.
- An id on the queue when the consumer shares the producer's store. The full payload when the queue crosses a service boundary, with the client owning the correlation id.
- An extension contract a key-value store could implement. Report a new `KEY idx_` when it exists to answer a query the caller should already have the key for and therefore forces an avoidable backend capability. Batch atomicity, multi-key queries, and cross-entity transactions do not belong on the contract.
- A decision or action extension takes the thin reference entity (`entity.Request`, `entity.Batch`) and resolves changes, diffs, and targets itself. `conflict.Analyzer` is the reference shape.
- Factories and per-queue routing live in service wiring. `NewFactory` under an `extension/` package violates that ownership when it couples the extension to topology or implementation selection. A service-scoped aggregate, implementation, mocks, and schema live with the service that resolves them; the behavioral contracts stay shared.
- `platform/` does not import a domain. A domain `core/` does not import a service package. An implementation lives in a subpackage of its interface.

Trace both sides of a boundary before reporting it: producer and consumer for queue contracts, caller and implementation for interfaces, controller and store for versioned writes. State the concrete coupling, backend incompatibility, race, or rollout failure the boundary violation creates.

## Correctness and failure modes

Covers Correctness and Bugs. Read `AGENTS.md` on eventual consistency, idempotency, and `platform/errs`.

- The consumer is safe under at-least-once delivery. A redelivery converges; it does not apply a side effect twice. A stale read is tolerated.
- Extensions return plain errors. A classifier decides retry. A failed publish is not wrapped as retryable just so the handler runs again. `storage.ErrNotFound` is a user error only at a call site that knows the user asked for a missing resource.
- Races, lost updates, a wrong version guard, and a publish that can run before its write. Give the concrete interleaving. Context cancellation is material only when the call can block or leak work after cancellation.
- A concrete defect in the changed lines: nil or empty identity, a mishandled `""` enum sentinel, an off-by-one, an error that is dropped. A design disagreement is not a bug.
- Rolling compatibility. Proto field numbers stay stable and evolution stays additive. For schema or message findings, name the incompatible old/new binary combination and deployment order; do not infer a rollout failure from `NOT NULL` alone.

## Simplicity and clarity

Covers Simplicity. Read `AGENTS.md` on naming, comments, and code style.

- A name says what it acts on, at the call site, without opening the definition. Naming is a nit unless ambiguity can cause misuse or makes the changed behavior materially harder to verify.
- A rename left the old term in code, comments, or docs. Watch `merging` against `landing`, and `land` against `merge` in git-specific code.
- A false comment that changes a reader's understanding of behavior is should-fix. Narration and comment-budget violations are nits. Prefer a clearer name over a comment.
- Value types. `(T, bool)` for absence, not a pointer. An error is a failure, not a branch of normal control flow.
- Dead code that expands the maintenance or behavior surface. Treat an intent mismatch as a question unless it creates review risk or changes behavior outside the stated scope.

## Tests and hermeticness

Covers Testing and Hermeticness. Read `AGENTS.md` on testing and `doc/howto/TESTING.md`.

- Report missing coverage only when the change adds observable behavior or a material failure path that existing tests do not exercise. Name the exact scenario, why current tests miss it, the regression it could permit, and the smallest test that would prove it.
- Same-package tests, table-driven structure, testify, and generated mocks are repository conventions. Report them as nits unless the deviation causes a concrete coverage, maintenance, or generation failure.
- No `assert` or `require` on `err.Error()`. No `time.Sleep`. No timeout of the test's own.
- Integration and e2e inputs come from runfiles: `testutil.Runfile`, `testutil.WithBuildContext`, and Bazel `data`. Resolving the repo root (`show-toplevel`, `FindRepoRoot`) is a finding. A compose context is `{category}-{domain}-{name}`. Docker or network access has the `integration` and `requires-network` tags.
- Repository automation and validation run through Bazel with pinned toolchains and declared `srcs`/`data`; Make targets are thin entry points, not a second build graph. Report host-installed tools, undeclared files, runtime downloads, or direct source-tree assumptions when they make local, CI, or remote execution diverge.
- Nontrivial scripting is a Python `py_binary`/`py_test` or a Go binary like the repository linters, not Bash. A shell file is limited to an unavoidable `exec`-style launcher with no parsing, branching workflow, or domain logic. For a finding, identify the portability, quoting, dependency, or hermetic-execution failure and name the Python or Go target that should own the logic.

## Operability

Run when the change logs, records a metric, or returns an error to a caller. Read `AGENTS.md` on structured logging.

- A metric tag has a bounded value set. Estimate the expected cardinality before reporting it; names are not inherently unbounded.
- For a log-volume finding, identify the log level and estimate the event rate or loop amplification. Per-message debug logging is not automatically a defect.
- Logging uses `Debugw`, `Infow`, `Errorw`, or typed zap fields. `Infof`, `Errorf`, and the other formatted zap methods are a finding. `fmt.Errorf` is not.
- The log or error carries the identifiers needed to diagnose the demonstrated failure. Missing context is a finding only when the surrounding operation cannot otherwise be identified.

## Docs and RFCs

Run only when documentation changed. Read `AGENTS.md` on README files and markdown prose.

- A decision names material alternatives and why one was chosen when that context is needed to evaluate or safely change it. Missing alternatives are otherwise a question or nit.
- Terms match the code. A doc that still says `merging` where the code says `landing` is a finding.
- A README describes behavior in prose. It does not paste an interface or a type definition. A short example is fine only where the doc already says to include one.
- Prose is one line per paragraph and one line per list item. Hard wrapping is a nit unless it breaks rendering or generated processing. Code blocks, tables, and diagrams keep their own line breaks.
- A new or renamed RFC is linked from `doc/rfc/index.md`, and links to the old path are updated.
