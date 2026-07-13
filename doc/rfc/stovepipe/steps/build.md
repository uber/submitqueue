# Build stage

`build` triggers the build-runner for the scope `process` already decided and hands the resulting build id to `buildsignal`. See [workflow.md](doc/rfc/stovepipe/workflow.md) for where it sits in the pipeline, and [process.md](doc/rfc/stovepipe/steps/process.md) for how the scope it reads (`BuildStrategy`, `BaseURI`) is chosen.

It handles only the trigger: it does not poll for completion, record greenness, or decide incremental-vs-full — those are `buildsignal`'s, `record`'s, and `process`'s jobs respectively.

`build` is structurally the same controller as `submitqueue/orchestrator/controller/build/build.go`, and [doc/rfc/submitqueue/build-runner.md](doc/rfc/submitqueue/build-runner.md) is the reference rationale for the trigger-then-poll shape this stage reuses. The `BuildRunner` contract's `Status`/`Cancel` half now carries over as literally shared code with SubmitQueue, via `platform/extension/buildrunner`; what does *not* carry over is `Trigger` — the two domains model different problems, so each keeps its own, following the [extension-contract.md](doc/rfc/submitqueue/extension-contract.md) "identity in, resolve internally" principle.

## Input, partitioning, and the single-writer property

`build` consumes a request id, published by `process` in Phase 1 (see [workflow.md](doc/rfc/stovepipe/workflow.md#workflow)). Both phases drive the same `build` → `buildsignal` machinery against the same `Request` row. The `process`/`analyze` → `build` topic is partitioned by **request id**; see [Partitioning](#partitioning) for the full rationale, including why `build` → `buildsignal` partitions finer (by build id) than SubmitQueue's equivalent topic.

**`build` is not the sole writer of the rows it touches, and its own writes are narrow.** On `Request`, `build` never writes at all — it only reads the `BuildStrategy`/`BaseURI`/`URI` fields `process` set at admit, and it leaves `Request.State` untouched at `processing` throughout (`process` and `record` are `Request.State`'s only writers). On `Build`, `build` is the sole creator — it calls `BuildStore.Create` exactly once, at step 6 — and never mutates the row again; `buildsignal` is the sole writer of `Build.Status`/`Build.Version` afterward (see [buildsignal.md](doc/rfc/stovepipe/steps/buildsignal.md#input-and-re-entrancy)). This division is why `build` never needs a CAS/version write of its own: `Create` is the only storage mutation in its algorithm.

`build` is phase-agnostic: it never asks "which phase is this?" It reads whatever scope is already persisted and immutable on the `Request` and acts on it. Phase 2's project-scoped invocation is expected to read project-scoped equivalents of the scope fields off the same `Request`; the exact shape of a project-scoped trigger is left to the `analyze` design, consistent with `workflow.md`'s "project mapping contract" open question — see [Project-scoped `Trigger`: reserved, not yet designed](#project-scoped-trigger-reserved-not-yet-designed) for the resulting gap in the `BuildRunner` contract itself.

In both phases `build` is strictly the trigger — `buildsignal` owns polling — so a crash between `Trigger` and the publish leaves a build no poller ever picks up; the redelivery triggers a replacement and the orphan is accepted waste, mirroring SubmitQueue (see [Idempotency](#idempotency)).

## Algorithm

For a delivery carrying request id `R`:

```
1. Load Request R from the request store.
   - ErrNotFound       -> retryable (process/analyze write not visible yet; redelivery converges).
   - other store error -> return raw; classifier decides.

2. If R.State is terminal (superseded / recorded-green / recorded-not-green): ack and return.
   - a redelivery after record already finished, or after process superseded R, must not start a
     fresh build.

3. Resolve the build-runner for R's Queue: buildRunner = Factory.For(Config{QueueName: R.Queue}).
   - lookup failure -> non-retryable (a queue with no builder is a config error).

4. Read the already-decided scope off R: R.BuildStrategy, R.BaseURI, R.URI.
   - build never re-derives incremental-vs-full; process decided it (see process.md).
   - BuildStrategy unset (process's CAS not visible on this reader yet) -> retryable, like step 1.
     Do NOT default to a full build; converge on redelivery once the write lands.
   - baseURI = R.BaseURI if R.BuildStrategy == incremental_since_green, else "" (full build).
   - (headURI = R.URI, baseURI) identify the scope; both are opaque SourceControl tokens.

5. Trigger: buildID, err := buildRunner.Trigger(ctx, R.URI, baseURI, metadata)
   - Trigger takes no caller-supplied id; the runner mints the build's identity,
     and buildID becomes Build.ID — SubmitQueue's exact convention (see
     "Alternatives considered" under the contract sketch).
   - there is deliberately no "already triggered?" pre-check: with a runner-minted
     id there is no key to check by, so a redelivery re-triggers and downstream
     idempotency absorbs the duplicate (see Idempotency).
   - Trigger is async: it returns promptly with the runner-assigned id, not an outcome.
   - metadata is empty for now; reserved for future caller annotations.
   - failure -> return raw; classifier decides (transient runner blip retryable, bad URI not).

6. Persist Build{ID: buildID.ID, RequestID: R.ID, URI: R.URI, BaseURI: baseURI,
               Status: accepted, Version: 1} via BuildStore.Create.
   - a crash between step 5 and this write orphans the triggered build (see Idempotency).
   - ErrAlreadyExists -> benign (reachable only with a backend that returns deterministic ids
     for retried triggers); continue to step 7.
   - other store error -> return raw; classifier decides.

7. Publish buildID to the buildsignal topic, partitioned by build id.
   - each build's poll loop then runs in its own partition (see [Partitioning](#partitioning)).
   - publish failure -> return raw (non-retryable by default); the Build row exists, and the DLQ
     path owns recovery (see Error classification).

8. ack.
```

`Build.ID` is **minted by the runner at `Trigger`**, exactly as in SubmitQueue's build controller: the runner returns its native id (a Buildkite build number, a CI-gateway job id), `build` adopts it as the `Build`'s key, and that same id travels on every downstream hop — `build` → `buildsignal` → `record` each carry the build id in the message, so every reader reaches the `Build` by a direct get on identity it was handed. No reader ever *derives* a build id or needs a reverse index; `Build.RequestID` covers the one navigation the pipeline needs in the other direction (`Build` → `Request`). Another approach is deriving the key from the Request (`buildKey(R)`) and/or passed a caller-supplied idempotency key to `Trigger`; see [Alternatives considered](#alternatives-considered-for-the-build-identity) for what each would buy and cost.

`build` writes only the `Build`, and only at creation; it never mutates `Request.State`. The Request stays `processing` (set by `process`) through `build` and `buildsignal` until `record` moves it terminal. `Build.Status` is the fine-grained build lifecycle; `Request.State` is the coarse pipeline lifecycle. This is also what keeps `process.md` step 3 correct: because `build` leaves the Request at `processing`, a redelivered `process` message still matches its "if processing, re-publish to build" guard.

## Idempotency

Every branch is safe under at-least-once redelivery — with SubmitQueue's posture on duplicates adopted wholesale: `build` has no pre-trigger dedup check (there is no caller-derivable key to check by; see [Alternatives considered](#alternatives-considered-for-the-build-identity)), so a redelivery that reaches step 5 starts a second, independent build, and safety comes from downstream idempotency rather than from preventing the duplicate:

- **Request not found / strategy not yet visible** — retryable; the producing stage's write is not visible on this reader yet.
- **Request already terminal** (step 2) — ack, no build. A redelivery after `record` finished, or after `process` superseded the head, never starts a stale build.
- **Redelivery while the Request is still in flight** (crash or failure anywhere in steps 5–8) — the redelivery re-runs from step 1, `Trigger` mints a fresh id, `Create` persists a second `Build` row, and a second poll loop starts. Harmless, in three layers: both builds target the identical `(headURI, baseURI)` scope; each `Build` polls in its own partition and `buildsignal` short-circuits the moment the Request goes terminal (its step 3); and `record`'s terminal transition is CAS-guarded, so the second verdict is a no-op. A build triggered but never persisted (crash between steps 5 and 6) is the same story minus the row: an orphan the runner finishes and nobody ever reads. Wasted CI compute, not a correctness risk — the same accepted trade as SubmitQueue.
- **Trigger / publish / other store failure** — nothing durable is left half-written that a redelivery can't reconcile; the error rejects to DLQ, and the fail-closed reconciler drives the Request terminal (see [workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work)).

## Edge cases

- **Head equals last-green** (`R.URI == R.BaseURI`). `process.md` calls this degenerate and leaves it to `build`. Resolution: run a normal incremental build with an empty delta — pass `baseURI = R.BaseURI` unchanged and let the runner apply zero changes on top of it. This needs no branch in `build`: the runner must already handle "no delta" (two adjacent commits) as valid input. Rejected: skipping the build and copying last-green's result forward, because that would force `build` to reason about whether last-green's *recorded greenness* (not just its URI) is still valid — a `record` concern `build` has no business making.
- **Phase 2 triggers another build for the same Request.** No collision by construction: every `Trigger` mints a fresh runner id, so the Phase-1 whole-repo build and each Phase-2 project-scoped build get distinct `Build` rows, all carrying the same `RequestID`. Each build's id travels in its own messages, so downstream navigation stays a direct get; how `analyze` obtains and forwards the ids of the project builds it triggers is part of the deferred project-scoped trigger design.

## Fail-closed interaction

A build that never reaches step 8 — `Trigger` failing repeatedly, the publish to `buildsignal` never landing, `BuildStore.Create` down — must not wedge its `Request`'s Queue slot forever: `process`'s per-Queue concurrency gate holds `in_flight_count` open until the Request reaches a terminal state (see [process.md](doc/rfc/stovepipe/steps/process.md#concurrency-lifecycle)). `build` does not implement the forcing function itself. Per [workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work), every non-retryable failure in the algorithm rejects to DLQ (see [Error classification](#error-classification)), and a Request stuck past `MaxAttempts` is driven to a conservative terminal not-green by the DLQ reconciler, which decrements `in_flight_count` and frees the slot. This is the same posture `buildsignal` relies on for its own poll loop (see [buildsignal.md](doc/rfc/stovepipe/steps/buildsignal.md#fail-closed-interaction)) — `build` and `buildsignal` are two links in the same fail-closed chain that keeps one bad Request from wedging its Queue.

One boundary is worth stating explicitly: this path fires only when `build` (or a downstream stage) *errors*. A `Trigger` call that returns successfully but the backend never actually runs — or a `Build` row created for a build the runner silently drops — has no protocol-level failure to escalate at the `build` stage; nothing here retries or dead-letters, because nothing failed. That gap surfaces one hop later, when `buildsignal` polls: either the runner reports an error (handled by `buildsignal`'s own classification) or it reports a non-terminal status forever, which is `buildsignal`'s fail-closed boundary to close, not `build`'s (see [buildsignal.md](doc/rfc/stovepipe/steps/buildsignal.md#fail-closed-interaction)). `build`'s liveness responsibility ends at a successful publish to `buildsignal`.

## Cancellation: defined, not yet called

`Cancel` is in the `BuildRunner` contract for parity with SubmitQueue, but no controller in this design calls it. SubmitQueue cancels from its `speculate`/cancel path when a batch is preempted mid-validation; stovepipe's `process` explicitly does **not** preempt an admitted `Request` ("a newer head does not preempt an in-flight validation" — [process.md](doc/rfc/stovepipe/steps/process.md#concurrency-lifecycle)). So there is currently no trigger for cancelling a running build. `Cancel` stays in the contract because a future change — the lease-based self-healing `in_flight_count`, or raising `max_concurrent` past 1, both floated in `process.md` — could need to abandon a build no longer worth finishing. Until then it is unused surface, not dead weight.

## Error classification

Per `platform/errs`' non-retryable-by-default rule (see [platform/errs/README.md](platform/errs/README.md)), the controller returns raw errors and lets the consumer's classifier decide; it opts into retryability explicitly only where it knows something the classifier cannot:

| Failure | Disposition | Why |
|---|---|---|
| `Request` not found (`storage.ErrNotFound`) | retryable (`errs.NewRetryableError`) | Producing stage's write not visible yet; redelivery converges — same as `process.go`. |
| Factory lookup | non-retryable | A queue with no registered builder is a config error, not transient. |
| Malformed message | non-retryable | The payload is broken; retrying can't fix it. |
| `Trigger` | raw error; classifier decides | Runner timeout/connection is transient (may be classified retryable); a bad URI is permanent. |
| Build store, other than `ErrAlreadyExists` | non-retryable | Storage down; DLQ reconciliation recovers. |
| Publish to buildsignal | non-retryable | Not wrapped retryable, per the errs rule against retryable publishes. The message dead-letters, the fail-closed reconciler forces a conservative not-green, and an operator republish can still resume polling later — a late green wins over the conservative verdict via CAS ([workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work)). |

Everything non-retryable rejects to DLQ, where the fail-closed path forces a conservative not-green so a Request never wedges its Queue's slot ([workflow.md](doc/rfc/stovepipe/workflow.md#fail-closed-on-unprocessable-work)).

## Orchestrator vs. Stovepipe build

Both domains have a `build` controller that triggers via a build-runner extension, and they share the *pattern*: id-only queue payloads (only the id travels; the entity is reloaded from the store), the swallow-`ErrAlreadyExists`-then-publish-anyway redelivery handling, and the `Name()`/`TopicKey()`/`ConsumerGroup()` consumer shape. They differ in the **`BuildRunner` contract**, because they model different problems.

### SubmitQueue

SubmitQueue validates **stacks of changes** before merging. Its `build` controller loads `base []entity.Batch` (ordered dependency batches) and `head entity.Batch` and triggers:

```go
buildID, err := buildRunner.Trigger(ctx, base, head, metadata)
```

The batches are **identity** — thin references carrying ids, not change content — and the runner resolves each batch's changes through injected dependencies. This is "identity in, resolve internally" applied to the batch domain.

### Stovepipe

Stovepipe validates **one commit** against a baseline (or in full). Its `build` controller reads two opaque URIs off the `Request` and triggers:

```go
buildID, err := buildRunner.Trigger(ctx, headURI, baseURI, metadata)
```

There is no batch, no dependency list, and nothing to resolve — the URIs *are* the identity, owned by `SourceControl`. `process` already decided incremental-vs-full; `build` just reads `R.BuildStrategy`/`R.BaseURI` and acts.

### Why separate contracts

1. **Conceptual mismatch — materialize vs. check out.** SubmitQueue's `Trigger` builds a state that does not exist yet: the runner resolves each batch's changes and applies their patches into composite base/head layers before running CI (see the Buildkite backend). Stovepipe's head already exists on the branch — trunk history is linear, and a landed commit *contains* every commit below it — so the runner checks out `headURI` and, for an incremental build, diffs against `baseURI`; no patch application, no composite commit. Squeezing stovepipe through the patch-list contract breaks in one of two ways for a history interval `a → b → c → d → e` (`a` = last green, `e` = head): modeling it as base `a` + patch `e` misses the intermediate commits `b..d` in the built state (**missing history**), while modeling it as heads `[b, c, d, e]` makes the runner cherry-pick four commits into a composite that is identical to just checking out `e` (**excessive work**). Forcing single-element batch lists is only the mild form of the same mismatch: abstraction with no benefit.
2. **Baseline semantics — and a mode batches can't express.** SubmitQueue's base batches are *stacked* into a dependency DAG validated together; stovepipe's baseline is a single reference point, and the build is either against it (incremental) or from scratch (full). Batches don't model this naturally — an empty base list means "no dependencies", not "ignore ancestry and build the whole repo", so the `full_monorepo` fallback `process` selects on a history rewrite ([process.md](doc/rfc/stovepipe/steps/process.md#build-strategy-decision)) has no faithful batch encoding at all.
3. **URI ownership.** Stovepipe's head and baseline are owned by `SourceControl`. Passing URIs directly keeps that boundary clean; passing batch objects would leak batch semantics into a runner that shouldn't know they exist.

The linearity assumption in point 1 is load-bearing and already guarded upstream: stovepipe assumes a linear trunk by default, and when `SourceControl` reports that last-green is no longer an ancestor of the head (history rewrite), `process` falls back to a full build rather than trusting the interval ([process.md](doc/rfc/stovepipe/steps/process.md#build-strategy-decision)). The URI-pair contract is exactly as expressive as that model — a valid `base..head` range, or a full build with an empty baseline — and nothing more.

So `build`'s `Trigger` gets its own shape under `stovepipe/extension/buildrunner`, still "identity in, resolve internally" — just with URI identity instead of batch identity. `Status` and `Cancel` don't have this mismatch — both domains poll and cancel by the same opaque, runner-minted id with the same async semantics — so they move to a shared `platform/extension/buildrunner.StatusCanceller` sub-interface instead of being duplicated (see the contract sketch below).

### Stovepipe `BuildRunner` contract (design sketch)

Not implemented here. `BuildID`, `BuildStatus`, and `BuildMetadata` move to `platform/base` (promoted from each domain's own `entity` package, now that `Status`/`Cancel` are genuinely shared — see [Alternatives considered for sharing the contract](#alternatives-considered-for-sharing-the-contract) for why); `stovepipe/extension/buildrunner` holds only `Trigger`, `Config`, and the `Factory` interface, per [CLAUDE.md](CLAUDE.md)'s extension rules.

```go
// platform/extension/buildrunner — shared with SubmitQueue
type StatusCanceller interface {
    // Status returns the current status. Takes the id Trigger returned
    // (Build.ID). May round-trip to the backend. BuildMetadata is
    // caller-supplied, provider-echoed; the runner must not depend on it, but
    // a controller may read it for its own purposes (e.g. round-tripping to
    // users, or a future short-circuit check) — buildsignal's own poll loop
    // doesn't need it to decide when to stop polling, in either domain.
    Status(ctx context.Context, buildID base.BuildID) (base.BuildStatus, base.BuildMetadata, error)

    // Cancel requests cancellation; a no-op on terminal builds. Takes the id
    // Trigger returned, like Status. Unused today in stovepipe (see
    // "Cancellation: defined, not yet called").
    Cancel(ctx context.Context, buildID base.BuildID) error
}

// package buildrunner (stovepipe/extension/buildrunner)
type BuildRunner interface {
    buildrunner.StatusCanceller

    // Trigger starts a new build every call and mints the build's identity —
    // there is no caller-supplied dedup input, matching SubmitQueue's contract
    // exactly (see "Alternatives considered for the build identity" below
    // for other shapes this doc considered). headURI is the commit
    // under validation; baseURI is the incremental baseline (empty for a full
    // build). metadata is caller annotations the runner may echo but must not
    // depend on. Runner-side work is async; callers learn progress via Status.
    // Returns the runner-assigned build id, which the caller adopts as Build.ID.
    Trigger(ctx context.Context, headURI, baseURI string, metadata base.BuildMetadata) (base.BuildID, error)
}

type Config struct{ QueueName string } // the only identity the system hands a Factory
type Factory interface{ For(cfg Config) (BuildRunner, error) }
```

#### Project-scoped `Trigger`: reserved, not yet designed

**TODO**, tracked pending the `analyze` design (see [workflow.md](doc/rfc/stovepipe/workflow.md#open-questions)'s "Project mapping contract" open question). The sketch above has only a whole-repo/incremental dimension (`headURI`, `baseURI`); it has no parameter for "build only this project," so as written it cannot express a Phase 2 invocation. `build` itself stays phase-agnostic (see [Input, partitioning, and the single-writer property](#input-partitioning-and-the-single-writer-property)) — it reads whatever scope is already decided and passes it through — but `Trigger` still needs a slot to read that scope from and forward to the runner.

The shape isn't decided here because project semantics belong to `analyze`, not `build`: how a project maps to a buildable scope (a Bazel target pattern, a directory, a service name) is implementer-specific per [workflow.md](doc/rfc/stovepipe/workflow.md#project---greenness-at-a-finer-grain). The expectation is that this stays an opaque token — following the same "identity in, resolve internally" shape already used for `headURI`/`baseURI` (owned and interpreted by `SourceControl`) — that `build` reads off the `Request`/message and hands to the runner uninterpreted, rather than a structured type `build` would have to understand:

```go
Trigger(ctx context.Context, headURI, baseURI string, projectScope entity.ProjectScope, metadata base.BuildMetadata) (base.BuildID, error)
```

`ProjectScope` lives in `stovepipe/entity` rather than `platform/base` — unlike `BuildID`/`BuildStatus`/`BuildMetadata`, projects have no SubmitQueue equivalent, so there is nothing to share. Its zero value covers Phase 1 (no project — whole-repo/incremental scope only, exactly today's sketch); `analyze` is what would populate a non-zero value for Phase 2. This mirrors the additive optional field already reserved on `BuildRequest` for the same purpose (see [Queue contract additions](#queue-contract-additions)) — the wire message and the extension contract need the same new dimension, and both are deferred to the same design.

Only `Trigger` differs between domains — batches vs. URIs, per [Why separate contracts](#why-separate-contracts). `Status` and `Cancel` don't have that mismatch: both domains poll and cancel by the same opaque, runner-minted id with the same async semantics, so duplicating them per domain (one copy in each domain's own `entity` package, mirroring the choice already made for `RequestID`) would be "shaped the same" without being shared code. Sharing them for real means `BuildID`/`BuildStatus`/`BuildMetadata` have to be the same Go types on both sides, which is why they move to `platform/base` here rather than staying duplicated — a one-time migration of SubmitQueue's already-shipped controllers, storage, and protobuf mappings onto the shared type, accepted as worth it for real reuse (a dual-implementing backend satisfies both `BuildRunner` interfaces through one embedded method set) instead of two interfaces that only look alike. [Alternatives considered for sharing the contract](#alternatives-considered-for-sharing-the-contract) below records the shapes that were weighed against this one.

There is exactly one build id: the runner mints it at `Trigger`, `build` adopts it as `Build.ID`, and every later call and message carries it verbatim — `Status`/`Cancel` take the same value `Trigger` returned, the queue payload is the same value, the store key is the same value. This is SubmitQueue's convention end to end. The id is opaque: no stovepipe reader parses it, derives it, or equates it with another entity's id — the trap SubmitQueue's speculate/cancel path falls into. And per the extension rules a runner keeps only transient local state, so the durable `Request` ↔ `Build` linkage lives in **our** store as `Build.RequestID`, never in the runner.

Supporting entity types: `BuildStatus`, `BuildMetadata`, and `BuildID` live in `platform/base`, shared verbatim with SubmitQueue rather than duplicated — `BuildStatus` is the narrow lowercase enum `"" (unknown) / accepted / running / succeeded / failed / cancelled` with an `IsTerminal()` predicate covering the last three, `BuildMetadata` is the free-form `map[string]string`, and `BuildID` is a `{ID string}` wire struct wrapping the one runner-assigned id everywhere it appears — `Trigger`'s return, `Status`/`Cancel`'s parameter, the queue payload. `stovepipe/entity/build.go` keeps what's stovepipe-specific: the `Build` entity itself (`RequestID`/`URI`/`BaseURI` alongside the shared `ID`/`Status`/`Version`), and, separately, `TargetGraph` — see [below](#target-graph-not-part-of-status-resolved-separately) for where that lives and why it isn't part of `Status`.

### Alternatives considered for sharing the contract

Three further shapes for sharing the `BuildRunner` contract across domains were also raised during the design discussion, weighed against the shared-`Status`/`Cancel`-plus-per-domain-`Trigger` split adopted above. Recorded here for context; none of these three were adopted:

- **One platform-level `BuildRunner` with both trigger verbs.** Move the interface to `platform/extension` and add `TriggerChanges(baseURI, headURI)` beside the batch-based `Trigger`:

  ```go
  // package platform/extension/buildrunner
  type BuildRunner interface {
      Trigger(ctx context.Context, base []entity.Batch, head entity.Batch, metadata entity.BuildMetadata) (entity.BuildID, error)
      TriggerChanges(ctx context.Context, headURI, baseURI string, metadata entity.BuildMetadata) (entity.BuildID, error)
      Status(ctx context.Context, buildID entity.BuildID) (entity.BuildStatus, entity.BuildMetadata, error)
      Cancel(ctx context.Context, buildID entity.BuildID) error
  }
  ```

  Trade-offs: the batch verb would drag SubmitQueue-only entities (`Batch`, its state machine) into `platform/`, which is reserved for genuinely cross-domain types — they would be "shared" in name with exactly one consumer. And a two-verb interface where every caller uses exactly one verb is a sign the contract is modeling two problems; every backend — Buildkite, a mock, a future CI-gateway client — would carry a stubbed or irrelevant half per domain. SubmitQueue's `build` controller would only ever call `Trigger`; stovepipe's would only ever call `TriggerChanges`.
- **Scope smuggled through `BuildMetadata`.** Keep one narrow `Trigger`, pass no scope arguments, and encode base/head (or job-configuring env cards) in metadata:

  ```go
  buildID, err := buildRunner.Trigger(ctx, entity.BuildMetadata{
      "head_uri": headURI,
      "base_uri": baseURI,
  })
  ```

  Trade-offs: this inverts the metadata contract — `BuildMetadata` is caller annotation the runner echoes but **must not depend on** ([build-runner.md](doc/rfc/submitqueue/build-runner.md#buildmetadata)). Routing the build's one load-bearing input through it would make the scope untyped, unvalidated, and invisible in the interface — a runner correctly honoring the "must not depend on metadata" rule would ignore `head_uri`/`base_uri` entirely and build the wrong scope.
- **Shared backend under `platform`, thin per-domain contracts.** House the Buildkite / CI-gateway implementation once under `platform/extension` and let each domain define its own contract over it:

  ```go
  // platform/extension/buildrunner/buildkite — shared HTTP client, auth, poll loop
  package buildkite

  type Client struct{ /* ... */ }

  // submitqueue/extension/buildrunner
  func NewBuildkiteRunner(c *buildkite.Client) submitqueuebuildrunner.BuildRunner { /* resolves batches into a patch list, then calls c */ }

  // stovepipe/extension/buildrunner
  func NewBuildkiteRunner(c *buildkite.Client) stovepipebuildrunner.BuildRunner { /* checks out headURI, diffs against baseURI, then calls c */ }
  ```

  Trade-offs: the shareable layer is thinner than it looks — the *checkout intent* differs at the CI-pipeline level: SubmitQueue's job applies patch lists into a composite commit, stovepipe's checks out an existing commit and diffs against a baseline — so the pipeline side must know which caller it serves either way. What this option gets right can still be had at the implementation layer without a shared contract: a concrete backend package can implement **both** domain interfaces and share its client/auth/poll plumbing internally (each service wires only the interface it needs — the "one backend, two interfaces" shape), and genuinely domain-free plumbing can live under `platform/`.

Contract-level reuse is not zero even for the parts that stayed separate: the `Trigger` async contract — returns a handle not an outcome, callers learn progress via `Status` — and the id model carry over verbatim between the two domains' `Trigger` methods, even though `Trigger` itself isn't shared code — see [Carries over vs. new](#carries-over-vs-new).

### Target graph: not part of `Status`, resolved separately

`Status` does not return a `TargetGraph` alongside `BuildStatus`. `buildsignal`'s poll loop runs for every build in the pipeline, including every Phase-2 project build, but only `analyze` ever needs a target graph, and only once, right after a Phase-1 build succeeds. Routing it through `Status`/`buildsignal` would mean every poll tick — and every Phase-2 build, which has no further target-to-project mapping to do — computes or shuttles a graph almost nothing reads. Per the extension-contract's "identity in, resolve internally" rule, `analyze` resolves the target graph itself from the identity it already has (`Build.ID`, arriving via `record`'s fan-out), rather than receiving it pre-resolved through a chain of controllers that don't need it. See [Alternatives considered for the target graph](#alternatives-considered-for-the-target-graph) for the shapes weighed for how `analyze` does that, and for the open question of what `TargetGraph` itself contains.

### Alternatives considered for the target graph

How the target graph gets from the runner to `analyze` has four shapes worth recording. The last one reflects where this doc leans, not a closed decision — the `analyze` design has the final say:

- **Inline in `Status`, alongside `BuildStatus`.** `Status(ctx, buildID) (BuildStatus, TargetGraph, error)`, with `buildsignal` persisting whatever comes back. Trade-offs: simplest wiring, but it puts a potentially large, `analyze`-only payload on every poll tick of every build — including every Phase-2 project build, which has no further target-to-project mapping to do — and it forces `Status` back into a stovepipe-only signature, undoing the shared `platform/extension/buildrunner.StatusCanceller` (see [Alternatives considered for sharing the contract](#alternatives-considered-for-sharing-the-contract)).
- **Runner-owned artifact, opaque handle returned from a call.** The runner writes the graph to its own storage (an artifact API, a blob keyed by build id) and returns only a small locator — from `Status`, or from the dedicated call below. Trade-offs: shrinks whatever crosses an RPC or queue to a handle-sized value regardless of graph size, and fits the "identity in, resolve internally" pattern this contract already uses for `Build.ID`. The cost only shows up with more than one concrete `BuildRunner` backend: a single backend can just own wherever it stores the artifact, but a second backend would need the handle format to be uniformly resolvable — a `platform`-level abstraction this doc otherwise avoids building ahead of a second consumer.
- **`buildsignal` fetches it once at the terminal poll and persists it for `analyze`.** `buildsignal` calls a target-graph-fetching method itself, but only on the poll that observes a terminal, successful status, storing the result (or a handle to it) on `Build` for `analyze` to read later. Trade-offs: bounds the fetch to once per build instead of every poll tick, unlike the inline-in-`Status` option — but `buildsignal`'s poll loop still runs for every Phase-2 project build, producing a graph nobody downstream reads, and it makes `buildsignal` a pass-through for data it has no use for itself — the "controller pre-resolves for someone else" shape the extension-contract's "identity in, resolve internally" rule warns against.
- **`analyze` resolves it directly via a dedicated call, keyed by `Build.ID`.** E.g. `TargetGraph(ctx, buildID) (entity.TargetGraph, error)` — either an additional stovepipe-only method on `BuildRunner`, or a separate extension `analyze` depends on; either way resolved through the same `Factory.For(Config{QueueName})` pattern `build`/`buildsignal` already use. Trade-offs: the only option where the party that needs the data is the party that fetches it, so nothing computes or carries a graph on `buildsignal`'s or a Phase-2 build's behalf. The cost is that `analyze` needs its own `Factory.For(Config{QueueName})` resolution — not a new capability, since `build`/`buildsignal` already do this, just one more caller of it.

### Alternatives considered for the build identity

Two alternatives to the runner-minted id are worth recording. They are independent knobs — the first changes what keys the `Build`, the second changes what `Trigger` accepts — and they compose (a combined variant would use both).

#### Alternative A: caller-derived build key

Key the `Build` by identity derived from the Request — `buildKey(R) = R.ID` for the Phase-1 whole-repo build, a `{R.ID}/{hash(project)}` composite for Phase 2 — and store the runner's id in a separate `Build.RunnerBuildID` field (with a distinct wire type, so the two ids can never be passed for each other).

| Pros | Cons |
|---|---|
| Redelivery dedup by direct get: checking `BuildStore.Get(buildKey(R))` before triggering means at-least-once delivery never starts a second build | A second id concept (`Build.ID` beside `Build.RunnerBuildID`) carried by every entity, signature, and reader forever |
| `Request` → `Build` navigation with no reverse index, per the KV key-derivation rule in CLAUDE.md | No current reader needs to *derive* a build id — the id travels in every message hop, so each consumer already holds the key it needs |
| Enforces (rather than assumes) the direct-navigation property SubmitQueue's speculate takes on faith | Diverges entity shape and controller flow from SubmitQueue, weakening the "structurally the same controller" claim and dual-implementing-backend symmetry |

Trade-offs: the dedup guards a rare event at a permanent modeling cost. The duplicate it prevents arises only from a redelivery inside the trigger window — rare, and already harmless (identical scope; `buildsignal`'s terminal-Request short-circuit and `record`'s CAS make the loser a no-op — see [Idempotency](#idempotency)). The prospective key-derivers — a future canceller, or `analyze` reaching back to the Phase-1 target graph — would need to be handed the id by their producing stage instead, if those designs land.

#### Alternative B: caller-supplied idempotency key on `Trigger`

Orthogonal to how the `Build` is keyed: give `Trigger` an extra parameter — a stable per-build token the caller already holds (`R.ID` in Phase 1) — that a runner supporting deduplication uses to **re-attach** a retried `Trigger` to the build it already started instead of spawning a second. The runner still returns its own native id; `Build.ID` stays runner-minted. Backends that cannot dedup ignore the token and degrade to today's behavior.

| Pros | Cons |
|---|---|
| Closes the duplicate-build window at the source (see [Idempotency](#idempotency)) instead of absorbing duplicates downstream | The one concrete backend in hand can't honor it: Buildkite's "create a build" endpoint takes no dedup key or idempotency header in its documented parameters (`author`, `clean_checkout`, `env`, `meta_data`, `pull_request_*`, …) — every call mints a new build number |
| Additive and degradable: a runner that can't dedup ignores the token, and behavior is exactly today's | Speculative surface until such a backend exists — an unused parameter every implementation must carry and document |
| The standard remote-create pattern for at-least-once callers (idempotency keys), so future backends plausibly support it | Diverges `Trigger`'s signature from SubmitQueue's on a parameter neither domain's current backend can act on |

Trade-offs: a parameter no backend can act on is speculative surface, not a working guarantee — and the failure it would prevent is already accounted for as accepted waste.

Either could be adopted independently: the idempotency token, if a backend that honors one lands (a purely additive `Trigger` parameter); the derived key, if a stage lands that genuinely must derive a build's key from a Request (moving the runner's id to a distinct `RunnerBuildID` field and wire type, since the two ids would then coexist and must not be confusable). The metric that would justify either is the same: duplicate-build waste showing up in practice.

### Carries over vs. new

- **Carries over as literally shared code**: the `BuildStatus` enum and `IsTerminal()` (nothing batch-specific — [build-runner.md](doc/rfc/submitqueue/build-runner.md#buildstatus)); `BuildMetadata` (caller-supplied, provider-echoed, controller-uninterpreted — [#buildmetadata](doc/rfc/submitqueue/build-runner.md#buildmetadata)); the async contract — `Trigger` returns a handle not an outcome, `Status` may round-trip, `Cancel` reaches the provider not the engine ([#async-vs-sync-contract](doc/rfc/submitqueue/build-runner.md#async-vs-sync-contract)); and the id model — no caller-supplied id, the runner mints the build's identity, and that one `base.BuildID` is the store key, queue payload, and `Status`/`Cancel` parameter (see [Alternatives considered](#alternatives-considered-for-the-build-identity)). These now live in `platform/base`/`platform/extension/buildrunner`, not duplicated per domain — see the contract sketch above.
- **New in stovepipe**: URI-based scope in `Trigger` instead of batch lists — the one part of the contract that stays domain-specific, per [Why separate contracts](#why-separate-contracts). Mapping targets to projects is still stovepipe-only, but it doesn't ride `Status`: `analyze` resolves a `TargetGraph` itself through a separate call, once, only when it needs one — see [Target graph: not part of `Status`, resolved separately](#target-graph-not-part-of-status-resolved-separately).

## Entity and storage additions needed

**`Build` entity** (`stovepipe/entity/build.go`), following the immutable-except-`Status`/`Version` shape of `entity.Request`; `ID` and `Status` reuse the shared `platform/base` types (`BuildID`, `BuildStatus`) now that `Status`/`Cancel` are shared with SubmitQueue (see the [contract sketch](#stovepipe-buildrunner-contract-design-sketch)), while `RequestID`/`URI`/`BaseURI` stay stovepipe-specific:

| Field | Role | Mutable? |
|---|---|---|
| `ID` | The build's own key — the runner-assigned id returned by `Trigger` (a Buildkite build number, a CI-gateway job id); opaque, never parsed or derived | no |
| `RequestID` | The `Request` this build validates (`Build`→`Request` navigation) | no |
| `URI` | Head URI being built (`== Request.URI`) | no |
| `BaseURI` | Incremental baseline; empty for full builds | no |
| `Status` | `accepted / running / succeeded / failed / cancelled` | **yes** — `buildsignal` |
| `Version` | `int32` optimistic-locking version | **yes** — with `Status` |

**States** (`Build.Status`):

| Status | Meaning | Terminal? |
|---|---|---|
| `` (unknown) | Zero value; never a valid stored status | no |
| `accepted` | Queued by the runner via `Trigger`; not yet started | no |
| `running` | Actively executing | no |
| `succeeded` | Build finished, all checks passed | **yes** |
| `failed` | Build finished, at least one check failed | **yes** |
| `cancelled` | Build stopped before finishing (see [Cancellation](#cancellation-defined-not-yet-called)) | **yes** |

`IsTerminal()` on `base.BuildStatus` covers exactly the three terminal rows. Once `buildsignal` persists one of them, that status is **write-once** — a later poll reporting a different terminal value never overwrites it (see [buildsignal.md](doc/rfc/stovepipe/steps/buildsignal.md#algorithm), step 6).

Plus the `BuildID{ID string}` wire type from `platform/base` (same "id only travels" convention as `RequestID`, but shared rather than duplicated — see the [contract sketch](#stovepipe-buildrunner-contract-design-sketch)), wrapping the one runner-assigned id everywhere it appears — `Trigger`'s return, the queue payload, `Status`/`Cancel`'s parameter. `buildsignal` and `record` reach a build by the id carried in their messages, so no reverse index from `Request` to its builds is ever needed.

**`BuildStore`** (new, added to the `Storage` aggregator via `GetBuildStore()`), matching stovepipe's existing `RequestStore` conventions — **generic `Update` with caller-owned version arithmetic, not SubmitQueue's field-specific `UpdateStatus`**:

- `Create(ctx, build entity.Build) error` — `ErrAlreadyExists` if the id is taken.
- `Get(ctx, id string) (entity.Build, error)` — `ErrNotFound` if absent.
- `Update(ctx, build entity.Build, oldVersion, newVersion int32) error` — pure conditional write; `ErrVersionMismatch` on a stale guard. The controller computes `newVersion = oldVersion + 1`, calls the store, and assigns `build.Version = newVersion` only on success (see [CLAUDE.md](CLAUDE.md) and the [storage README](submitqueue/extension/storage/README.md)).

Single-key reads/writes only — no list-by-request, no query-by-attribute — per the key/value-shaped extension rule in [CLAUDE.md](CLAUDE.md).

**`Request` additions** (extending the existing entity, which already has `ID/Queue/URI/State/Version`):

| Field | Role | Set by |
|---|---|---|
| `BuildStrategy` | `incremental_since_green` or `full_monorepo`; immutable once set | `process` |
| `BaseURI` | Last-green URI for incremental; empty for full | `process` |

`URI` already exists; `process` sets `BuildStrategy`/`BaseURI` at admit (process.md step 7c) and `build` reads them (step 4). Both are immutable for the Request's life.

## Queue contract additions

Two topic keys in `stovepipe/core/messagequeue/topics.go` — `TopicKeyBuild` (`process`/`analyze` → `build`) and `TopicKeyBuildSignal` (`build` → `buildsignal`) — and one proto message per key, since the contract test binds **exactly one message to each topic key** (see [messagequeue-contract.md](doc/rfc/messagequeue-contract.md)). `ProcessRequest` is bound to `process` and cannot be reused; the new messages mirror its shape (one id field, its own `topic_keys` option):

- `BuildRequest{ id }` (request id) → `topic_keys "build"`, produced by `process`/`analyze`, consumed by `build`. Phase 2's per-project trigger must also identify its project; because each topic key binds exactly one message, that lands as an **additive optional field on this same message** (protojson discards unknown fields, so the evolution is backward-compatible), not a second message type — the field's shape is deferred to the `analyze` design with the rest of the project-scoped trigger.
- `BuildSignal{ id }` (build id) → `topic_keys "buildsignal"`, produced by `build` and re-produced by `buildsignal`, consumed by `buildsignal` (see [buildsignal.md](doc/rfc/stovepipe/steps/buildsignal.md)).

### Partitioning

`process`/`analyze` → `build` is partitioned by **request id**: per-request build work is independent (the per-Queue concurrency gate already ran in `process`), and a single Request's (rare) duplicate deliveries stay ordered — though with no pre-trigger dedup check that ordering is a tidiness property, not a correctness dependency; duplicates are absorbed downstream (see [Idempotency](#idempotency)). `build` → `buildsignal` is partitioned by **build id** (the runner-assigned `Build.ID`), so each build's poll loop is an independent partition. The id is unique per build, so one `Request`'s several Phase-2 project builds land in distinct partitions — the point: a slow poll on one must not block the others. The flip side of request-id partitioning on the `build` topic is that all of one Request's Phase-2 *triggers* serialize through a single partition; that is fine because `build` is trigger-only and cheap, and the concurrency Phase 2 needs comes from the per-build poll partitions, not from parallel triggering. This is a deliberate divergence from SubmitQueue, which partitions its build poll loop by *batch id* (its unit of work); stovepipe goes finer.
