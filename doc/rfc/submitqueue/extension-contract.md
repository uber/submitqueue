# Extension Contract

Design notes for what SubmitQueue's pluggable extensions accept: orchestrator **identity** they resolve themselves, versus **controller-resolved data**. Decisions and rationale only; the code changes land after this RFC is reviewed.

## Problem

Extension input granularity is inconsistent across the pipeline stages (see [workflow.md](workflow.md)). `conflict.Analyzer` takes identity (`entity.Batch`); `scorer`, `changeprovider`, `buildrunner`, `pusher` take controller-resolved `entity.Change`. The split caps what an extension can do:

- `ConflictType` already names `target_overlap`, but a real target-overlap analyzer **cannot be written** — the batch controller hands it identity-level batches (no changed targets) and the contract has nowhere to put them.
- `scorer` gets a URIs-only `Change`, so a heuristic scorer **cannot see** lines-changed / file-count.

Both unblock with the shape `conflict` already uses: accept identity, resolve internally.

## Principle

- **Decision/action extensions** take orchestrator identity at their stage granularity and resolve granular content through narrowly-injected dependencies. Request stage → `entity.Request`; batch stage → `entity.Batch` / `[]entity.Batch`. Both are thin reference entities (a `Request` carries URIs, not diffs; a `Batch` carries IDs, not changes).
- **Resolution targets** — `storage`, `changestore`, `queueconfig` — stay key/value-shaped. They are what the others resolve *through* (see [storage/README.md](../../../submitqueue/extension/storage/README.md) and CLAUDE.md).
- **Output mirrors the input unit.** Each output element self-identifies with the input it corresponds to — `changeprovider`'s `ChangeInfo` carries its `URI`, `conflict`'s `Conflict` carries its `BatchID` — so a flat list suffices and the caller correlates results back to inputs without re-deriving boundaries. A *wrapper* entity (`entity.BatchChanges`) is introduced only to aggregate *up* to a coarser unit than the elements — the scorer needs batch-wide line/file totals, so the rollup earns its keep; no `RequestChanges` exists because nothing needs request-wide rollups. And when the input is a *collection* of independently-actioned units, the output groups by them: `pusher`, fed `[]entity.Batch`, returns outcomes grouped per batch, the same way `conflict` already tags each `Conflict` with its in-flight `BatchID`.

### What each stage resolves today

| Stage | Loads | Resolves for the extension | Hands to the extension |
|---|---|---|---|
| `validate` | `entity.Request` | nothing — `request.Change` is already in hand (the change-store reads here serve duplicate detection) | `request.Change` → `changeprovider` |
| `batch` | `entity.Request` + active `[]entity.Batch` | **nothing** — builds a batch whose `Contains` is `[requestID]` | `entity.Batch`, `[]entity.Batch` → `conflict` |
| `score` | `entity.Batch`, then each `entity.Request` | batch → requests | `request.Change` per request, then multiplies the scores → `scorer` |
| `build` | `entity.Batch`, then `collectChanges` | batch → requests → changes, **flattening batch boundaries** | base `[]Change`, head `[]Change` → `buildrunner` |
| `merge` | `entity.Batch`, then `collectChanges` | batch → requests → changes | `[]Change` → `pusher` |

Two facts this grounds: `conflict` already resolves nothing (the baseline), and the batch→changes walk is **already duplicated** in `build`/`merge` `collectChanges` — the shared resolver below only consolidates it.

## Verdict

| Extension | Stage | Input today | Proposed input | Output | Injected deps |
|---|---|---|---|---|---|
| `conflict.Analyzer` | batch | identity (`Batch`, `[]Batch`) | unchanged — **the baseline** | conflicting in-flight batches (`[]Conflict`, `BatchID`-tagged) — unchanged | request store + change provider |
| `scorer.Scorer` | score | flat `Change`, per request | `entity.Batch` — resolve + reduce internally | one batch score (`float64`) — unchanged | request store + change provider |
| `changeprovider.ChangeProvider` | validate | `Change` | `entity.Request` | per-URI change info (`[]ChangeInfo`, `URI`-tagged) — unchanged | none — it *is* the resolver |
| `buildrunner.BuildRunner` | build | base/head `[]Change` | base `[]entity.Batch` + head `entity.Batch` | build id, then status/cancel (`BuildID`, `BuildStatus`) — unchanged | request store + change provider |
| `pusher.Pusher` *(removed)* | merge | — | **moved out-of-process to runway** (`merge` / `merge-signal`); see the note below the table | — | — |
| `storage`, `changestore`, `queueconfig` | — | keys + entities | unchanged — resolution targets | entities | — |

**Outputs are unchanged.** This RFC moves the *input* toward identity; the four live return contracts — conflicts, score, change info, build id/status — are exactly what they are today. (The `pusher` row is not an in-process extension: merge runs out-of-process in runway, so its output is not part of this catalog — see the note below.) No other output shape changes.

The validate-time mergeability **check** and the **merge** itself both run **asynchronously and out-of-process** in runway rather than as in-process extensions, over the one shared `MergeRequest`/`MergeResult` contract — a check is a dry run of a merge. `validate` hands off directly to runway (→ `merge-conflict-check`, result back via `mergeconflictsignal`); `merge` hands the batch to runway (→ `merge`, result back via `mergesignal`) rather than calling an in-process `pusher`. See [workflow.md](workflow.md). The in-process `mergechecker` and `pusher` packages are unused on the pipeline path.

Non-obvious points:

- **scorer** — owning the batch moves batch-level reduction (today the controller's multiplicative product) into the scorer, where the `composite` reduce step already lives.
- **buildrunner** — this **revises** [build-runner.md](build-runner.md), which deliberately kept batches out of the boundary. The base/head split survives, expressed as batches; the provider still operates on changes (the shared resolver produces them inside the extension). Cost: a `buildrunner` / `pusher` implementation now depends on a request store + change provider.
- **pusher** — a *list* of batches (not one) designs for a merge-train: land several ready batches, or a batch with not-yet-landed deps, in one atomic push. Today merge pushes a single batch because deps are already on trunk. Since the input is now a list, the output groups outcomes per batch (`BatchID`-tagged, with per-change commit detail kept underneath) instead of one flat per-change list — the only output shape this RFC changes. Push atomicity is unchanged (all-or-nothing across the whole call), so a per-batch *status* is intentionally omitted: a partial-landing train would be a separate, larger change to the atomicity contract.

## Mechanism

Dependencies are injected per-extension at the existing `Factory.For` (wiring: `example/submitqueue/orchestrator/server/main.go`) — only the handles a contract justifies, never the whole storage aggregator. The repeated batch→changes walk becomes one shared resolver (today's duplicated `collectChanges`, consolidated, and preserving the batch boundaries build's copy flattens). Controllers shrink to passing the identity entity they already load.

`entity.BatchChanges` is kept, not removed — it becomes the shared resolver's *detailed output* (URIs + provider details for a batch, what the scorer consumes) rather than a value the score controller assembles and passes in. Its line/file helpers move with it; only its producer changes.

## Rejected

- **Status quo (controller resolves).** Keeps extensions pure and trivially testable, but thickens controllers and caps every extension at what the controller chose to pre-compute — the two blocked features are that ceiling.
- **Literal string IDs.** An extra read per call when the controller already holds the entity; pass thin reference entities instead.
- **Per-implementation batch→changes resolution.** How the `build`/`merge` duplication arose; one shared resolver instead.
- *Acknowledged:* decision extensions gain dependencies and are no longer pure functions — mitigated by their existing mock packages and `Factory` injection.
