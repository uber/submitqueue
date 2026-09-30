# Stovepipe Workflow

Stovepipe answers one question for the rest of the company: **at which commit is this thing green?** It continuously polls a repository branch for its latest commit, validates that commit, works out which projects (if any) are broken at it, records the result, and notifies downstream systems so they can gate deployments on a known-good commit. It is a post-land service: code lands first, Stovepipe finds out whether it was good.

The pipeline is a queue-driven chain of small, single-purpose controllers, in the same style as SubmitQueue (SQ). Each controller consumes one topic, advances one entity, and publishes to the next topic. Most hops carry only an **ID** and the controller reloads the entity from storage; the entry hop carries the caller's input because there is no row to load yet. What runs today is:

> **poll for a new head → ingest → process the build strategy → build → buildsignal → record whole-repo greenness (and any project results the resolver returns) → hook.**

There is no `analyze` controller and no `analyze` topic. A later project-analysis stage that would map a target graph onto another `build` pass is design only, described under [Designed, not built](#designed-not-built-project-analysis). `record` resolves project results inline on the same delivery that writes the repository fact.

## What Stovepipe is agnostic about

Two deliberate abstractions keep Stovepipe from being a git tool or a Bazel tool:

- **The VCS is behind a `SourceControl` extension.** Stovepipe never shells out to git. Every commit, ref, and branch head is an opaque **URI** that a `SourceControl` implementation produces and interprets. A ref is `git://remote/repo/ref/…`; a specific commit is `git://remote/repo/ref/…/<sha>`. The `git://` scheme is just the reference implementation — a Mercurial or Perforce backend would mint its own scheme behind the same contract. Nothing downstream of `SourceControl` parses a URI; it is a token you hand back to `SourceControl` to ask questions ("is A an ancestor of B?", "what is the head of this ref?").
- **The build system is behind a build-runner extension** (see [build-runner.md](../submitqueue/build-runner.md)). `Trigger` starts a build at a head URI, optionally against a baseline URI, and `Status` returns pass/fail plus metadata. A target graph that a separate analyze stage would map to projects is part of the unbuilt design, not this contract.

Designing to these contracts — not to git and Bazel specifically — is the whole point: the same pipeline should validate any branch in any VCS built by any build system.

## Core concepts

### URI — the unit of identity for "where"

Everything Stovepipe records greenness *about* is a URI: a specific commit on a specific branch. The last-known-good commit is a URI; a build is run against a URI; a project's greenness is recorded against a URI. Because the URI is opaque, Stovepipe can compare, store, and key on it without knowing it is a git SHA.

### Queue — the unit of identity for "what we validate"

Stovepipe reuses SQ's **Queue** concept for the same two reasons SQ does — to **namespace the generated IDs** and to give callers a **stable handle for the repo+ref being validated** — plus a third that is specific to a post-land validator: a Queue **owns the last-known-good URI** and the greenness history for its branch.

A Queue is named by a **stable logical string** (e.g. `monorepo/main`), and that name is what the ingest API takes — *not* a raw URI. SourceControl/config resolves the Queue name to a concrete VCS URI base. This keeps callers (and the external poller) free of VCS detail: they say "the `monorepo/main` Queue has moved", and Stovepipe resolves what that means.

A Queue is *not* tied to trunk specifically — any branch can be a Queue. "Queue" here is the **validation namespace**; it is distinct from the **messaging queue** the pipeline runs on. Where the two could be confused, this doc says "messaging queue" for the transport and "Queue" for the namespace entity.

### Request — one validation of one head

When the poller reports that a Queue has a new head, Stovepipe mints a **Request** (an ID namespaced by the Queue, exactly as the SQ gateway mints a request ID) representing "validate this Queue at this head URI". The Request, not the URI, is the thing that flows through the pipeline and accumulates state (the chosen build strategy, the build outcome, the recorded greenness).

Identity for the *head* is the `(Queue, head URI)` pair, and that pair is the **dedup key**: if the poller reports the same head twice, or a future webhook producer races the poller, both resolve to the same Request and the work happens once. The minted Request ID is the routing handle; the dedup key is what makes ingestion idempotent.

### Greenness — a degree, not a boolean

Greenness is recorded as a **health degree** where **`0` means green** and **higher means more broken**, with **`1` meaning fully broken**. Stovepipe starts with only the two endpoints — `0` (green) and `1` (broken) — at the whole-repo level. The space between exists so that, once project-level analysis lands, "broken to some extent" (a fraction of projects failing) has somewhere to live without changing the contract or the state machine. A commit that has been ingested but not yet validated has *no* recorded greenness for the relevant scope — absence is distinct from `0`, and callers must treat "not yet recorded" as not-green for gating.

### Project — greenness at a finer grain

A **project** is a caller-defined slice of the repository. Whole-repo greenness answers "is the branch green at this URI"; project greenness answers the question deployments actually need — **"is *this project* green at this URI"**, and its dual, "what is the latest URI at which this project is green". The running pipeline does not derive projects from a target graph. On a succeeded or failed request, `record` gives the request's terminal build id to `projectresult.Resolver` and writes one validation fact per result it returns. The example server wires the noop resolver, which returns no results. How a resolver chooses projects is implementer-specific. The separate analyze stage that would map a target graph to project-scoped builds is not built; see [Designed, not built](#designed-not-built-project-analysis).

### Promotion ref — the last green commit, by name

A Queue may have a **promotion ref**: a stable branch name (say `verified-main` for `monorepo/main`) that Stovepipe advances to each commit it establishes green. A deploy gate or cache warmer then fetches that name and needs to know nothing about Stovepipe, URIs, or greenness degrees. It is the pull-shaped counterpart to Hooks' push: the same fact, available to consumers that would rather resolve a ref than subscribe to an event.

The ref is a *cache* of the last-green URI, not a second record of greenness. It only ever moves where the bookmark already points, so it inherits the same forward-only rule, and a commit that a history rewrite has dropped from the branch is skipped rather than retried — the next green commit corrects the ref. Which ref a Queue promotes to, and whether it has one at all, is `SourceControl` configuration resolved from the Queue name alongside the repo and credentials; the pipeline names only the commit, never a branch.

## Extensions

| Extension | Responsibility |
|---|---|
| **SourceControl** | Resolve a Queue name to its current head URI; answer ancestry/comparison questions between two URIs (is the new head a fast-forward descendant of the last green, or was history rewritten?); enumerate commits in a range; advance the Queue's **promotion ref** to a commit. The sole owner of URI semantics, including which refs a Queue name resolves to. |
| **build-runner** | Build a scope at a URI, optionally relative to a baseline URI. `Trigger` returns a build id; `Status` returns pass/fail and the caller-supplied metadata it echoed. It does not return a target graph. See [build-runner.md](../submitqueue/build-runner.md). |
| **Hooks** | Deliver Stovepipe's validation events to downstream systems. What is published today is repository-scoped: validation of this URI has begun, this URI is green or not green, and a cancelled validation ended without a fact. Fire-and-forget notification, decoupled so Stovepipe does not know or care who consumes the event. The shared cross-domain hook seam rather than a Stovepipe-specific extension. See [hook-framework.md](../hook-framework.md). |
| **Storage** | Persist Queues (incl. last-green URI), Requests, build records, and per-URI / per-project greenness. Key/value-shaped per the extension-design rules in [AGENTS.md](../../../AGENTS.md). |

Hooks are the notification boundary. When validation of a commit begins, when a whole-repository validation fact is recorded, and when a cancelled validation ends without a fact, the event reaches deployment systems, dashboards, and developer tooling without any of them polling Stovepipe's store, and each environment can route it to its own downstream (a deploy gate, a Slack notifier, an event bus) without changing the pipeline. The mechanism is the cross-domain hook framework rather than a call out of the pipeline stages: `process` and `record` publish a repository-scoped `HookEvent` to Stovepipe's `hook` topic, and a dispatcher stage consumes it and invokes the wired hooks, so a slow or failing downstream cannot add latency to the pipeline. Both halves exist; what a deployment supplies is the hooks themselves, since the example server resolves every event to `noop`. Per-project hook events are not published. See [process.md](steps/process.md#hooks) for the start event and [record.md](steps/record.md#hooks) for the fact-to-event mapping.

## Workflow

What runs is one pass per Request. It establishes whole-repository greenness, and on a succeeded or failed outcome `record` also gives the Request's terminal build id to `projectresult.Resolver` before writing whatever project facts it returns. That call is inline on the record delivery. It does not publish to another stage, and it does not start another build.

```
 external poller ──(Queue name)──► ┌──────────────────────────────┐
 "Queue moved"  (outside OSS)      │ ingest                       │
                                   │ Resolve head URI via         │
                                   │ SourceControl; mint Request; │
                                   │ persist (greenness: none);   │
                                   │ dedup on (Queue, head URI)   │
                                   └───────────────┬──────────────┘
                                                   │ RequestID
                                                   ▼
                                   ┌──────────────────────────────┐   Hooks
                                   │ process                      │┄┄┄┄┄►  "validation
                                   │ Ask SourceControl: is head a │        started"
                                   │ descendant of last-green?    │
                                   │  → incremental since green   │
                                   │ else (history rewrite)       │
                                   │  → full monorepo             │
                                   └───────────────┬──────────────┘
                                                   │ RequestID (+ strategy, baseline URI)
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │ build                        │
                                   │ Run build-runner for the     │
                                   │ chosen scope; baseline =     │
                                   │ last-green URI iff incremental│
                                   └───────────────┬──────────────┘
                                                   │ BuildID
                                                   ▼
                                   ┌──────────────────────────────┐
                                   │ buildsignal                  │
                                   │ Poll until terminal; record  │
                                   │ status; release the slot;    │
                                   │ project the outcome          │
                                   └───────────────┬──────────────┘
                                                   │ RequestID
                                                   ▼
                                   ┌──────────────────────────────┐   Hooks
                                   │ record                       │┄┄┄┄┄►  "URI green,
                                   │ Write whole-repo greenness;  │      not green, or
                                   │ on green advance last-green  │      cancelled"
                                   │ and the promotion ref;       │
                                   │ resolve project results      │
                                   │ inline                       │
                                   └──────────────────────────────┘
```

1. **ingest** — invoked by the external poller with a **Queue name**. It asks `SourceControl` for that Queue's current head URI, mints a Request namespaced by the Queue, persists it with no recorded greenness yet, and dedups on `(Queue, head URI)` so a re-reported head is processed once. It publishes the RequestID onward.
2. **process** — decides build strategy (incremental since last-green vs full monorepo), gates concurrent work per Queue, coalesces backlog to the latest head, publishes a **hook event** announcing that validation of the commit has begun, and publishes to `build`. See [process.md](steps/process.md).
3. **build** — runs the build-runner for the chosen scope. A flag derived from `process` decides whether to build relative to the last-green **baseline URI** (incremental) or from scratch (full). It records a build and publishes the BuildID.
4. **buildsignal** — polls until the build is terminal, records that status, releases the Queue's `in_flight_count` slot, projects the terminal status and build id onto the Request (`succeeded` / `failed` / `cancelled`), and publishes the RequestID to `record`.
5. **record** — for a succeeded or failed Request, writes the whole-repo greenness for the head URI (`0` green / `1` broken to start), derived from the Request's build outcome. On green it advances the Queue's **last-green URI** so the next `process` can build incrementally from here, and asks `SourceControl` to advance the Queue's **promotion ref** to the same commit (see [Promotion ref](#promotion-ref--the-last-green-commit-by-name)). It then gives the Request's terminal build id to `projectresult.Resolver` and writes one validation fact per returned result. The example server uses the noop resolver, so that list is empty unless a deployment supplies another. It publishes `validation.repository.recorded` for that fact. The Queue's `in_flight_count` was already released by `buildsignal` when the build went terminal. A cancelled Request writes no fact and publishes `validation.repository.cancelled`, so a consumer can stop waiting on the commit.

### Designed, not built: project analysis

An `analyze` stage is not part of the pipeline. `stovepipe/core/messagequeue` has no analyze topic, and `stovepipe/controller` has no analyze package. The design, still only a design, is a stage that would take a build's target graph, map broken or at-risk targets to projects, and publish project-scoped builds back through `build` → `buildsignal` → `record`. That second pass, a per-project hook event, and intermediate greenness degrees are not what the controllers do. Open questions for that design stay at the bottom of this doc.

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **ingest** | Queue name (from poller) | process | Resolve head URI via SourceControl, mint Request, persist (no greenness), dedup on `(Queue, head URI)` |
| **process** | RequestID | build, hook topic | Build strategy, concurrency gate, backlog coalescing; announce validation start on admit → [process.md](steps/process.md) |
| **build** | RequestID | buildsignal | Run the build-runner for the chosen scope; baseline = last-green URI iff incremental |
| **buildsignal** | BuildID | record | Record terminal build status; release `in_flight_count`; project the outcome onto the Request; publish the request id |
| **record** | RequestID | hook topic | Write whole-repo greenness; resolve project results inline; on green advance last-green URI and the promotion ref; publish the recorded or cancelled repository hook event |
| **hook** | HookEvent | — | Invoke the hooks the resolver returns. The example server resolves every event to noop |

## Step RFCs

Per-stage design detail lives under `steps/` so this doc stays a pipeline overview:

- [process.md](steps/process.md) — build-strategy decision, concurrency gate, backlog coalescing, [concurrency lifecycle](steps/process.md#concurrency-lifecycle), entity changes, [waiting for a slot](steps/process.md#waiting-for-a-slot)
- [build.md](steps/build.md) — trigger-only stage: reads the decided scope off the Request, triggers the build-runner, hands off to buildsignal; the stovepipe `BuildRunner` contract and why it differs from SubmitQueue's
- [buildsignal.md](steps/buildsignal.md) — the poll loop: hold-based re-poll cadence, per-build partitioning, and the fail-closed handoff to record
- [record.md](steps/record.md) — turning a terminal build outcome into an immutable validation fact, monotonic last-green advancement and ref promotion, and the hook event announcing the outcome. Its Phase 2 / analyze handoff is the unbuilt design, not a topic `record` publishes

## Dedup, idempotency, and history rewrites

Ingestion is idempotent on `(Queue, head URI)`, so duplicate poller reports — and any future webhook producer racing the poller — converge on one Request. The pipeline persists the Queue's last-green URI and per-URI greenness, so it must tolerate a **history rewrite**: when `SourceControl` reports that the last-green URI is no longer an ancestor of the current head, `process` falls back to a full-monorepo build rather than trusting a baseline that no longer exists on the branch. The system converges to a correct greenness rather than wedging on a stale pointer.

## Fail-closed on unprocessable work

Callers gate deployments on greenness, so the dangerous failure is a Request that can never finish and silently leaves a URI with no recorded greenness — indistinguishable, to a naive caller, from "not yet validated". Following SQ's DLQ-reconciliation posture, a Request whose validation can never complete must be driven to a **conservative terminal `failed` outcome** — which whatever records greenness treats as not-green — rather than left non-terminal: gating stays safe (never falsely green), and the pipeline moves on. That conservative outcome is **final**, not provisional: validation facts are immutable and first-fact-wins, so a late successful result for a fail-closed URI is dropped rather than overwriting recorded history. The cost is bounded — the branch keeps moving, and the next head re-establishes greenness on its own Request. Note that finality cuts deeper than "the outcome is never revised": a forced `failed` can itself be recorded as a broken fact by a delivery still in flight, so a commit whose build actually passed can end up permanently marked broken. Whether that is correct or a category error is open — see [record.md](steps/record.md#what-fail-closed-actually-guarantees). See [submitqueue/orchestrator/controller/dlq/README.md](../../../submitqueue/orchestrator/controller/dlq/README.md) for the shared reconcile-only design.

## Open questions

- **Greenness degree semantics.** The endpoints (`0` green, `1` fully broken) are fixed; the meaning of intermediate values once projects exist (fraction of projects broken? weighted severity?) is deferred until project analysis is concrete.
- **Poller vs. webhook ingestion.** Only the external poller is in scope now. The dedup key is designed so a webhook producer can be added later without changing identity, but that producer is out of scope for this RFC.
- **Project mapping contract.** Not built. There is no analyze controller or topic. `record` already persists facts from `projectresult.Resolver` on the same delivery as the repository fact. The unbuilt analyze design — a target-graph mapping, project-scoped builds, and whether that mapping is an extension or an external service — is still open.
