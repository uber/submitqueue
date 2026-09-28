# Extension Contract

Design notes for what SubmitQueue's pluggable extensions accept: orchestrator **identity** they resolve themselves, versus **controller-resolved data**.

## Status

Implemented for the extensions named here. `changeprovider.Get` takes `entity.Request`. `conflict.Analyzer.Analyze` takes the batch under analysis and the in-flight batches. `buildrunner.Trigger` takes base `[]entity.Batch` and a head `entity.Batch`. `scorer.Score` takes `entity.Batch` and `entity.SpeculationPathSet`. There is no separate score stage; speculation asks the scorer. `changeset.Resolver` is the shared batch-to-changes reader and still declares its own `Stores` slice rather than importing an orchestrator aggregate. Runway still performs the asynchronous conflict check and the land. The verdict table's "Input now" column is these signatures.

## Problem

Before this contract, extension input granularity was inconsistent across the pipeline stages (see [workflow.md](workflow.md)). `conflict.Analyzer` took identity (`entity.Batch`); `scorer`, `changeprovider`, and `buildrunner` took a controller-resolved `entity.Change`. That split capped what an extension could do:

- `ConflictType` already named `target_overlap`, but a real target-overlap analyzer could not be written: the dependency-analysis stage handed it identity-level batches, with no changed targets, and the contract had nowhere to put them.
- `scorer` got a URIs-only `Change`, so a heuristic scorer could not see lines-changed or file-count.

Both unblock by taking the shape `conflict` already used: accept identity and resolve internally. The signatures in Status are that resolution.

## Principle

- **Decision/action extensions** take orchestrator identity at their stage granularity and resolve granular content through narrowly-injected dependencies. Request stage → `entity.Request`; batch stage → `entity.Batch` / `[]entity.Batch`. Both are thin reference entities (a `Request` carries URIs, not diffs; a `Batch` carries IDs, not changes).
- **Resolution targets** — `storage`, `changestore`, `queueconfig` — stay key/value-shaped. They are what the others resolve *through* (see [storage/README.md](../../../submitqueue/extension/storage/README.md) and [AGENTS.md](../../../AGENTS.md)). Refinement: the storage *aggregate* has since gained the same per-queue factory resolution every other seam has — the stores it hands back remain strictly key/value, bound to their queue, while the cross-queue read-model stores stay individually-injected singletons.
- **Output mirrors the input unit.** Each output element self-identifies with the input it corresponds to — `changeprovider`'s `ChangeInfo` carries its `URI`, `conflict`'s `Conflict` carries its `BatchID` — so a flat list suffices and the caller correlates results back to inputs without re-deriving boundaries. A *wrapper* entity (`entity.BatchChanges`) is introduced only to aggregate *up* to a coarser unit than the elements — the scorer needs batch-wide line/file totals, so the rollup earns its keep; no `RequestChanges` exists because nothing needs request-wide rollups.

### What each stage resolves today

| Stage | Loads | Resolves for the extension | Hands to the extension |
|---|---|---|---|
| `validate` | `entity.Request` | the provider reads `request.Change`; change-store reads here serve duplicate detection | `entity.Request` → `changeprovider` |
| `dependency` | `entity.Batch` + active `[]entity.Batch` | **nothing** — the batch it analyzes is already persisted, with `Contains` set to `[requestID]` | `entity.Batch`, `[]entity.Batch` → `conflict` |
| speculation | `entity.Batch` and its path set | the scorer does not receive a controller-resolved `Change` | `entity.Batch`, `entity.SpeculationPathSet` → `scorer` |
| `build` | head `entity.Batch` + path base `[]entity.Batch` | **nothing** — the build runner resolves each batch through its injected `changeset.Resolver` | base `[]entity.Batch`, head `entity.Batch` → `buildrunner` |

This grounds `conflict` as the baseline: it already resolves nothing because the controller passes the identity it needs.

## Verdict

| Extension | Stage | Input before | Input now | Output | Injected deps |
|---|---|---|---|---|---|
| `conflict.Analyzer` | batch | identity (`Batch`, `[]Batch`) | unchanged — **the baseline** | conflicting in-flight batches (`[]Conflict`, `BatchID`-tagged) — unchanged | request store + change provider |
| `scorer.Scorer` | speculation | flat `Change`, per request | `entity.Batch` and `entity.SpeculationPathSet` — resolve + reduce internally | one batch score (`float64`) — unchanged | request store + change provider |
| `changeprovider.ChangeProvider` | validate | `Change` | `entity.Request` | per-URI change info (`[]ChangeInfo`, `URI`-tagged) — unchanged | none — it *is* the resolver |
| `buildrunner.BuildRunner` | build | base/head `[]Change` | base `[]entity.Batch` + head `entity.Batch` | build id, then status/cancel (`BuildID`, `BuildStatus`) — unchanged | request store + change provider |
| `storage`, `changestore`, `queueconfig` | — | keys + entities | unchanged — resolution targets | entities | — |

**Outputs are unchanged.** This RFC moved the input to identity. The four return contracts — conflicts, score, change info, and build id/status — kept their shapes.

The validate-time landability **check** and the **land** itself both run **asynchronously and out-of-process** in Runway rather than as in-process extensions. SubmitQueue adapts its land request to Runway's shared `MergeRequest`/`MergeResult` contract, where a conflict check is a dry run of a merge. `validate` hands off directly to Runway (→ `merge-conflict-check`, result back via `landconflictsignal`); `land` hands the batch to Runway (→ `runway-merge`, result back via `landsignal`). See [workflow.md](workflow.md). SubmitQueue retains no parallel in-process checking or pushing contract.

Non-obvious points:

- **scorer** — owning the batch moves batch-level reduction (today the controller's multiplicative product) into the scorer, where the `composite` reduce step already lives.
- **buildrunner** — this **revises** [build-runner.md](build-runner.md), which deliberately kept batches out of the boundary. The base/head split survives, expressed as batches; the provider still operates on changes (the shared resolver produces them inside the extension). Cost: a `buildrunner` implementation now depends on a request store + change provider.

## Mechanism

Dependencies are injected per-extension at the existing `Factory.For` (wiring: `service/submitqueue/orchestrator/server/main.go`) — only the handles a contract justifies, never the whole storage aggregator. Batch→changes resolution is centralized in `changeset.Resolver`, while `buildrunner.ResolveBatches` shares the ordered flattening needed by build-runner backends. Controllers pass the identity entities they already load.

`entity.BatchChanges` is kept, not removed — it becomes the shared resolver's *detailed output* (URIs + provider details for a batch, what the scorer consumes) rather than a value the score controller assembles and passes in. Its line/file helpers move with it; only its producer changes.

## Rejected

- **Status quo (controller resolves).** Keeps extensions pure and trivially testable, but thickens controllers and caps every extension at what the controller chose to pre-compute — the two blocked features are that ceiling.
- **Literal string IDs.** An extra read per call when the controller already holds the entity; pass thin reference entities instead.
- **Per-implementation batch→changes traversal.** Duplicates storage and ordering rules across backends; use the shared resolver and build-runner helper instead.
- *Acknowledged:* decision extensions gain dependencies and are no longer pure functions — mitigated by their existing mock packages and `Factory` injection.
