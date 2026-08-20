# Hooks Framework

Fire-and-forget side effects for pipeline lifecycle events: one shared event contract, a durable hook topic per domain, pluggable hooks.

## Problem

The pipelines emit lifecycle transitions — a request lands or fails, a batch merges, a build finishes — but nothing can react outside pipeline state: no warehouse export, no PR comments or closes on merge events, no notifications or audit trails. The log topic is not this seam: SubmitQueue request statuses only, consumed solely to build gateway read models.

Two requirements: side effects must never stall or fail the pipeline, and "fire and forget" must not mean lossy — a merge-failure comment that silently never posts is a support ticket.

## Proposal

When a controller performs a transition, it also publishes a **hook event** to a durable `hook` topic. A thin per-domain hook stage consumes it, asks the host's **hooks resolver** which integrations that event belongs to, and runs them — none by default, real integrations as they arrive.

```
pipeline controller                              hook stage (per domain)
state write → hook publish → downstream ──▶ [hook topic] ──▶ decode → validate → Hooks.For(event)
                                                 │                                   └─▶ warehouse, code host, …
                                                 │ retries exhausted
                                                 ▼
                                             [hook_dlq] ──▶ log full event + page; manual republish
```

One event shape for every domain — the CloudEvents core plus a version ordinal:

| Field | What it is | Why it is on the envelope |
|---|---|---|
| `id` | Opaque occurrence identity, publisher-minted (`source/type/subject-id/version`) | Queue dedupe key and hook idempotency key |
| `source` | Producing domain (`submitqueue`) | Keeps multi-domain sinks unambiguous |
| `type` | What happened, one dotted open string (`batch.failed`) | The single filter dimension |
| `timestamp_ms` | Occurrence time, ms since epoch | Uniform time axis |
| `version` | Subject's ordinal at the transition; 0 = not a versioned write | Staleness detection under at-least-once — wall clocks can't |
| `payload` | Per-type facts, always including the subject's id | Everything else; open, additive vocabulary |

Delivery promise:

- At least once, deduped by `id` within the queue's retention window; hooks are idempotent by `id`.
- The hook publish rides the delivery that causes the transition — state write, hook publish, downstream publishes, ack — so a crash replays everything. No outbox.
- A failed publish fails the stage (the hook topic shares the pipeline's queue backend); loss is never silent.
- Past the retry budget, the DLQ logs the complete event and pages; manual republish recovers it.
- Ordering per subject only. Hook outcomes never write pipeline state.

## Decisions

### Identity and replay

- `id` = source / type / subject id / post-transition version; the causal message id stands in when unversioned, plus an ordinal for multiple same-typed events per cause. Components are separator-free. Consumers never parse it.
- Partition key = subject id; per-subject order only.
- Replay finding the target state already written → republish (idempotent). Beyond it → superseded; the event may be lost. Named, accepted gap.
- RPC-caused transitions are at-most-once; needing the guarantee means publishing from the first queue-driven stage.
- Opt-in per host via topic-key registration; a registered host never skips, so off and loss are distinguishable.

### Contract

- `api/base/hook/`: no owning domain, so the message-queue location rule extends — platform-owned contracts live under `api/base/`.
- Envelope = only fields every consumer keys on uniformly; subject, queue, and error are occurrence facts → payload. `source`/`type` are strings, not enums, for additive evolution.
- Payload (`Struct`): shaped per type, add-only, documented by its domain; must carry the subject's id and transient facts (merge step outcomes, build failure detail) — the event is their only durable record. Never entity snapshots; hooks resolve entities from stores.

### Hooks and dispatch

- Extension at `platform/extension/hook/`: the `Hook` contract plus a `Hooks` resolver the host builds in wiring. No `Config` and no `Factory` — selection is the resolver's, and only wiring knows the queue topology.
- Hook contract: at-least-once, idempotent by `id`, plain errors, never writes pipeline state; ignore an event by returning nil (no filter API).
- `Hooks.For(event)` keys on the event, not a queue name: the envelope carries no queue, and which scope selects hooks (queue, source, type) differs per domain. Resolving to none is ordinary. Ships `noop` for a host that wants an explicit placeholder. A cross-domain sink is the same impl wired into each domain.
- Controller (`platform/hook`, wired by each service): decode, validate (`id`/`source`/`type` non-empty), resolve, invoke all. Malformed events dead-letter, never silently acked; hook errors retry then dead-letter, with errs classifiers fast-pathing permanent failures.
- Mixed outcomes: every resolved hook runs even after one fails, and the failures are attributed and joined. `errs` weighs each branch of a joined error, so a transient failure alongside a permanent one still retries.
- DLQ reconciler: log the full event with its failure attribution, page (new metric — the log DLQ only warns), then ack. Manual republish recovers; pipeline state is never touched.
- Per-hook retry isolation later: consumer groups on the same `hook` topic key, once the registry supports multiple groups per key and rejection becomes group-local (today it moves the shared row). Until then one shared budget for all of an event's hooks is accepted.

## Example

```proto
syntax = "proto3";

package uber.base.hook;

import "google/protobuf/struct.proto";

import "api/base/messagequeue/proto/messagequeue.proto";

// HookEvent is one fire-and-forget lifecycle event. Every domain publishes
// this same shape to its own hook topic; hook implementations consume it.
message HookEvent {
    option (uber.base.messagequeue.topic_keys) = "hook";

    string id = 1;                       // Opaque occurrence identity; queue message id and hook idempotency key.
    string source = 2;                   // Producing domain: "submitqueue", "stovepipe", ...
    string type = 3;                     // What happened: "request.landed", "batch.failed", ...
    int64 timestamp_ms = 4;              // Occurrence time, ms since the Unix epoch.
    int32 version = 5;                   // Subject's version at the transition; 0 when not tied to a state write.
    google.protobuf.Struct payload = 6;  // Publisher-defined facts, including the subject's id; never a snapshot.
}
```

A failed batch, carrying merge-result facts persisted nowhere else (protojson: int64 as string, empty fields omitted):

```json
{
  "id": "submitqueue/batch.failed/batch-778/4",
  "source": "submitqueue",
  "type": "batch.failed",
  "timestamp_ms": "1722800012345",
  "version": 4,
  "payload": {
    "batch_id": "batch-778",
    "queue": "go-monorepo",
    "error": "merge conflict",
    "failed_step": "sq-12346",
    "conflict_paths": ["foo/bar.go"]
  }
}
```

## Rejected

- **A contract per domain.** N schemas, N hook shapes, N warehouse tables; one envelope absorbs differences additively.
- **Inline hook calls.** Couples pipeline latency to integrations; a crash between write and call silently drops the notification.
- **A second consumer group on the log topic.** Request statuses only; no path to batch, build, merge, or other domains.
- **Enums for source/type.** protojson rejects unknown enum values; every addition would break consumers.
- **Subject, queue, or error on the envelope.** Occurrence facts; they live in the payload. No major event platform carries a top-level error.
- **Entity snapshots as payload.** Stale on redelivery; competes with the store; drags domain schemas into the shared contract.
- **Typed per-event payloads (`oneof`).** Every new type becomes a wire-contract change.
- **A filter/subscription API.** Returning nil costs nothing; routing can be a wiring decorator later.
- **Best-effort publishing.** Silent loss; failing the stage is safe because dedupe makes the retry idempotent.
- **A transactional outbox.** The publish rides the triggering delivery before ack; a crash replays both. RPC-edge transitions are scoped out instead.
