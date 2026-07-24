# Stovepipe Workflow

Stovepipe answers one question for the rest of the company: **at which commit is this thing green?** It continuously polls a repository branch for its latest commit, validates that commit, works out which projects (if any) are broken at it, records the result, and notifies downstream systems so they can gate deployments on a known-good commit. It is a post-merge service: code lands first, Stovepipe finds out whether it was good.

The pipeline is a queue-driven chain of small, single-purpose controllers, in the same style as SubmitQueue (SQ). Each controller consumes one topic, advances one entity, and publishes to the next topic. Most hops carry only an **ID** and the controller reloads the entity from storage; the entry hop carries the caller's input because there is no row to load yet. The high-level shape is:

> **poll for a new head → ingest → process the build strategy → build → record greenness → analyze projects → record per-project greenness → notify downstream.**

## What Stovepipe is agnostic about

Two deliberate abstractions keep Stovepipe from being a git tool or a Bazel tool:

- **The VCS is behind a `SourceControl` extension.** Stovepipe never shells out to git. Every commit, ref, and branch head is an opaque **URI** that a `SourceControl` implementation produces and interprets. A ref is `git://remote/repo/ref/…`; a specific commit is `git://remote/repo/ref/…/<sha>`. The `git://` scheme is just the reference implementation — a Mercurial or Perforce backend would mint its own scheme behind the same contract. Nothing downstream of `SourceControl` parses a URI; it is a token you hand back to `SourceControl` to ask questions ("is A an ancestor of B?", "what is the head of this ref?").
- **The build system is behind a build-runner extension** (see [build-runner.md](../submitqueue/build-runner.md)), which returns a pass/fail and a **target graph** that the project-analysis stage maps to projects.

Designing to these contracts — not to git and Bazel specifically — is the whole point: the same pipeline should validate any branch in any VCS built by any build system.

## Core concepts

### URI — the unit of identity for "where"

Everything Stovepipe records greenness *about* is a URI: a specific commit on a specific branch. The last-known-good commit is a URI; a build is run against a URI; a project's greenness is recorded against a URI. Because the URI is opaque, Stovepipe can compare, store, and key on it without knowing it is a git SHA.

### Queue — the unit of identity for "what we validate"

Stovepipe reuses SQ's **Queue** concept for the same two reasons SQ does — to **namespace the generated IDs** and to give callers a **stable handle for the repo+ref being validated** — plus a third that is specific to a post-merge validator: a Queue **owns the last-known-good URI** and the greenness history for its branch.

A Queue is named by a **stable logical string** (e.g. `monorepo/main`), and that name is what the ingest API takes — *not* a raw URI. SourceControl/config resolves the Queue name to a concrete VCS URI base. This keeps callers (and the external poller) free of VCS detail: they say "the `monorepo/main` Queue has moved", and Stovepipe resolves what that means.

A Queue is *not* tied to trunk specifically — any branch can be a Queue. "Queue" here is the **validation namespace**; it is distinct from the **messaging queue** the pipeline runs on. Where the two could be confused, this doc says "messaging queue" for the transport and "Queue" for the namespace entity.

### Request — one validation of one head

When the poller reports that a Queue has a new head, Stovepipe mints a **Request** (an ID namespaced by the Queue, exactly as the SQ gateway mints a request ID) representing "validate this Queue at this head URI". The Request, not the URI, is the thing that flows through the pipeline and accumulates state (the chosen build strategy, the build outcome, the recorded greenness).

Identity for the *head* is the `(Queue, head URI)` pair, and that pair is the **dedup key**: if the poller reports the same head twice, or a future webhook producer races the poller, both resolve to the same Request and the work happens once. The minted Request ID is the routing handle; the dedup key is what makes ingestion idempotent.

### Greenness — a degree, not a boolean

Greenness is recorded as a **health degree** where **`0` means green** and **higher means more broken**, with **`1` meaning fully broken**. Stovepipe starts with only the two endpoints — `0` (green) and `1` (broken) — at the whole-repo level. The space between exists so that, once project-level analysis lands, "broken to some extent" (a fraction of projects failing) has somewhere to live without changing the contract or the state machine. A commit that has been ingested but not yet validated has *no* recorded greenness for the relevant scope — absence is distinct from `0`, and callers must treat "not yet recorded" as not-green for gating.

### Project — greenness at a finer grain

A **project** is a caller-defined slice of the repository. Whole-repo greenness answers "is the branch green at this URI"; project greenness answers the question deployments actually need — **"is *this project* green at this URI"**, and its dual, "what is the latest URI at which this project is green". Projects are derived from the build's **target graph**: analysis sees which targets broke and maps them to projects. How targets map to projects is implementer-specific (directory ownership, build metadata, an external service) and lives behind the project-analysis stage, not in the core pipeline.

## Extensions

| Extension | Responsibility |
|---|---|
| **SourceControl** | Resolve a Queue name to its current head URI; answer ancestry/comparison questions between two URIs (is the new head a fast-forward descendant of the last green, or was history rewritten?); enumerate commits in a range. The sole owner of URI semantics. |
| **build-runner** | Build a scope at a URI (optionally relative to a baseline URI), returning pass/fail and the target graph. See [build-runner.md](../submitqueue/build-runner.md). |
| **Hooks** | Publish Stovepipe's greenness events to downstream systems — "this URI / this project is now green (or not green)". Fire-and-forget notification, decoupled so Stovepipe does not know or care who consumes the event. |
| **Storage** | Persist Queues (incl. last-green URI), Requests, build records, and per-URI / per-project greenness. Key/value-shaped per the extension-design rules in [CLAUDE.md](../../../CLAUDE.md). |

The **Hooks** extension is the notification boundary. Whenever a greenness fact is recorded — whole-repo green/not-green, or later a project green/not-green — `record` fires the relevant hook so deployment systems, dashboards, and developer tooling learn about it without polling Stovepipe's store. Hooks are pluggable so each environment can route events to its own downstream (a deploy gate, a Slack notifier, an event bus) without changing the pipeline.

## Workflow

The pipeline runs in two phases against the same Request. **Phase 1** establishes whole-repo greenness. **Phase 2** refines it to per-project greenness. Both phases reuse the same `build` → `buildsignal` → `record` machinery; `record` is re-entrant and fans out, which is why it is not a terminal stage.

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
                                   ┌──────────────────────────────┐
                                   │ process                      │
                                   │ Ask SourceControl: is head a │
                                   │ descendant of last-green?    │
                                   │  → incremental since green   │
                                   │ else (history rewrite)       │
                                   │  → full monorepo             │
                                   └───────────────┬──────────────┘
              ┌────────────────────────────────────┤ RequestID (+ strategy, baseline URI)
              │ PHASE 1: whole-repo greenness       ▼
              │                    ┌──────────────────────────────┐
              │                    │ build                        │
              │                    │ Run build-runner for the     │
              │                    │ chosen scope; baseline =     │
              │                    │ last-green URI iff incremental│
              │                    └───────────────┬──────────────┘
              │                                    │ BuildID
              │                                    ▼
              │                    ┌──────────────────────────────┐
              │                    │ buildsignal                  │
              │                    │ Await/record build status +  │
              │                    │ target graph                 │
              │                    └───────────────┬──────────────┘
              │                                    │ BuildID
              │                                    ▼
              │                    ┌──────────────────────────────┐   Hooks
              │                    │ record                       │┄┄┄┄┄►  "URI green /
              │                    │ Write whole-repo greenness    │      not green"
              │                    │ for URI; if green advance     │
              │                    │ Queue's last-green URI; Hooks │
              │                    └───────────────┬──────────────┘
              │ PHASE 2: project greenness         │ RequestID
              │                                    ▼
              │                    ┌──────────────────────────────┐
              │                    │ analyze                      │
              │                    │ Map broken/at-risk targets   │
              │                    │ → projects (impl-specific);  │
              │                    │ decide project-scoped builds │
              │                    └───────────────┬──────────────┘
              │                                    │ RequestID (+ project)
              │                                    ▼
              │                    ┌──────────────────────────────┐
              │                    │ build → buildsignal          │
              │                    │ CI job runs; artifacts stored │
              │                    │ in blob store; status read    │
              │                    └───────────────┬──────────────┘
              │                                    │ BuildID
              │                                    ▼
              │                    ┌──────────────────────────────┐   Hooks
              └───────────────────►│ record                       │┄┄┄┄┄►  "project P
                                   │ Capture per-project greenness │      green / not
                                   │ for the URI; fire Hooks       │      green at URI"
                                   └──────────────────────────────┘
```

### Phase 1 — whole-repo greenness

1. **ingest** — invoked by the external poller with a **Queue name**. It asks `SourceControl` for that Queue's current head URI, mints a Request namespaced by the Queue, persists it with no recorded greenness yet, and dedups on `(Queue, head URI)` so a re-reported head is processed once. It publishes the RequestID onward.
2. **process** — decides build strategy (incremental since last-green vs full monorepo), gates concurrent work per Queue, coalesces backlog to the latest head, and publishes to `build`. See [process.md](steps/process.md).
3. **build** — runs the build-runner for the chosen scope. A flag derived from `process` decides whether to build relative to the last-green **baseline URI** (incremental) or from scratch (full). It records a build and publishes the BuildID.
4. **buildsignal** — records the build's status and target graph when the build completes, then publishes the BuildID to `record` (the `Build` row carries its RequestID, so `record` reaches the Request with a direct get).
5. **record** — writes the whole-repo greenness for the head URI (`0` green / `1` broken to start). On green it advances the Queue's **last-green URI** so the next `process` can build incrementally from here. It also decrements the Queue's `in_flight_count`, opening the process concurrency gate for the next head. It fires the **Hooks** extension with the green/not-green event, then fans out into Phase 2.

### Phase 2 — project greenness

6. **analyze** (project-analysis) — takes the build's target graph and maps the relevant targets to **projects**, using whatever implementer-specific mapping is configured. It decides which project-scoped builds / CI jobs are needed to attribute breakage to specific projects, and publishes those builds.
7. **build → buildsignal** — the project-scoped CI job runs; its artifacts are stored in a blob store (e.g. TerraBlob), and `buildsignal` reads back the status. This is the same machinery as Phase 1, reused at project granularity.
8. **record** — captures **per-project greenness for the URI** — for each project, green or not at this commit — and fires **Hooks** per project. This is what lets a caller ask "is project P green at URI U?" and "what is the latest URI where project P is green?".

`record` appearing twice is intentional: it is one re-entrant stage that records greenness at whatever granularity the current phase produced and notifies downstream. The Request is *complete* when every planned granularity has been recorded, not at a single terminal hop.

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **ingest** | Queue name (from poller) | process | Resolve head URI via SourceControl, mint Request, persist (no greenness), dedup on `(Queue, head URI)` |
| **process** | RequestID | build | Build strategy, concurrency gate, backlog coalescing → [process.md](steps/process.md) |
| **build** | RequestID | buildsignal | Run the build-runner for the chosen scope; baseline = last-green URI iff incremental |
| **buildsignal** | BuildID | record (P1), record (P2) | Record build status + target graph; signal completion |
| **record** | BuildID | analyze (P1→P2), Hooks | Write greenness; advance last-green URI on whole-repo green; decrement `in_flight_count`; fire Hooks |
| **analyze** | RequestID | build | Map broken/at-risk targets → projects; decide project-scoped builds |

## Step RFCs

Per-stage design detail lives under `steps/` so this doc stays a pipeline overview:

- [process.md](steps/process.md) — build-strategy decision, concurrency gate, backlog coalescing, [concurrency lifecycle](steps/process.md#concurrency-lifecycle), entity changes, [waiting for a slot](steps/process.md#waiting-for-a-slot)
- [build.md](steps/build.md) — trigger-only stage: reads the decided scope off the Request, triggers the build-runner, hands off to buildsignal; the stovepipe `BuildRunner` contract and why it differs from SubmitQueue's
- [buildsignal.md](steps/buildsignal.md) — the poll loop: `PublishAfter` re-poll cadence, target-graph return, per-build partitioning, and the fail-closed handoff to record
- [record.md](steps/record.md) — immutable greenness facts, idempotent Queue-slot release, monotonic last-green advancement, Hooks notification, and the Phase 1 handoff to analyze

## Dedup, idempotency, and history rewrites

Ingestion is idempotent on `(Queue, head URI)`, so duplicate poller reports — and any future webhook producer racing the poller — converge on one Request. The pipeline persists the Queue's last-green URI and per-URI greenness, so it must tolerate a **history rewrite**: when `SourceControl` reports that the last-green URI is no longer an ancestor of the current head, `process` falls back to a full-monorepo build rather than trusting a baseline that no longer exists on the branch. The system converges to a correct greenness rather than wedging on a stale pointer.

## Fail-closed on unprocessable work

Callers gate deployments on greenness, so the dangerous failure is a Request that can never finish and silently leaves a URI with no recorded greenness — indistinguishable, to a naive caller, from "not yet validated". Following SQ's DLQ-reconciliation posture, a Request whose validation can never complete must be driven to a **conservative not-green outcome** rather than left non-terminal: gating stays safe (never falsely green), and the pipeline moves on. State writes use optimistic-locking CAS, so a late successful update wins cleanly over the conservative one. See [submitqueue/orchestrator/controller/dlq/README.md](../../../submitqueue/orchestrator/controller/dlq/README.md) for the shared reconcile-only design.

## Open questions

- **Greenness degree semantics.** The endpoints (`0` green, `1` fully broken) are fixed; the meaning of intermediate values once projects exist (fraction of projects broken? weighted severity?) is deferred until project analysis is concrete.
- **Poller vs. webhook ingestion.** Only the external poller is in scope now. The dedup key is designed so a webhook producer can be added later without changing identity, but that producer is out of scope for this RFC.
- **Project mapping contract.** The exact shape of the target-graph→project mapping behind `analyze` (and whether it is a Stovepipe extension or an external service) is left to the project-analysis design.
</content>
