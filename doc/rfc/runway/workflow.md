# Runway Workflow

Runway is the landing service: it owns VCS operations — landability checking and landing — on behalf of SubmitQueue. Runway is a single service (the domain *is* the service): it subscribes to two inbound topics (`land-conflict-check`, `runway-land`) and publishes results to two outbound topics (`land-conflict-check-signal`, `land-signal`). It is a consumer-only service with no gateway; work arrives via topic queues and results leave via topic queues.

## Land-conflict check and land

The two queues are the same shape but different commit semantics:

- **land-conflict-check** is a dry run. A land request carries an ordered sequence of steps (changes + land strategy). Runway performs a read-only trial land and publishes per-step landability results back.

- **land** is the committing version. A land request carries the same payload but Runway commits the result and reports the revisions it produced (per-step output IDs).

- **promote** is a special `PROMOTE` strategy on the `land` queue: it pushes a commit to a ref as-is (`--ff-only`). The primary use case is forwarding a landed SHA from `main` to `verified/main` without creating a new merge commit.

These are independent input-output flows. A land-conflict check can run without a land ever running, and a land does not depend on a prior check.

## Vocabulary

*Land* is SubmitQueue's verb for integrating a change into the target branch: the public API (`Land`, `LandRequest`), the request lifecycle (`landing`/`landed`), the batch state (`landing`), the topics (`runway-land`, `land-conflict-check`, `land-signal`), and the Runway extension (`lander.Lander`) all use it. *Merge* appears only where it names something outside SubmitQueue's vocabulary: the `MERGE` land strategy (which creates a git merge commit), git primitives (`git merge`, `merge-base`, `MERGE_HEAD`), and external systems' own terms (GitHub marks a PR "merged", GitLab has "merge requests"). New code should not introduce merge-named identifiers for domain concepts.

## Branch serialization

The partition key `repo/target` on both inbound topics serializes all VCS operations for a given branch. The message queue delivers messages with the same partition key to the same consumer in order, so at most one land-conflict check or land operation is in flight for any given branch at any time.

The outbound topics partition by SubmitQueue queue name, matching SubmitQueue's fan-out model where state updates for the same queue are serialized.

## Workflow

```
                  ┌─────────────────────────────────────────────────────┐
                  │              submitqueue orchestrator                │
                  └──────────┬───────────────────────────┬──────────────┘
                             │                           │
                    LandRequest (dry run)       LandRequest (commit)
                              │                           │
                              ▼                           ▼
                [land-conflict-check]              [runway-land]
                              │                           │
                 land-conflict-check ctrl           land ctrl
                         (read-only)            (apply + commit)
                              │                           │
                         LandResult                  LandResult
                              │                           │
                              ▼                           ▼
             [land-conflict-check-signal]       [land-signal]
                              │                           │
                              ▼                           ▼
                   ┌──────────┬───────────────────────────┬──────────────┐
                   │  land-conflict-check-        land-signal ctrl        │
                   │  signal ctrl              (update batch state,       │
                   │  (update request            fan out to conclude)     │
                   │   landability)                                       │
                  │              submitqueue orchestrator                │
                  └─────────────────────────────────────────────────────┘
```

## Per-controller summary

| Controller | In | Out | One-line role |
|---|---|---|---|
| **land-conflict-check** | LandRequest | LandResult -> land-conflict-check-signal | Dry-run land: check landability of ordered steps against the target branch (read-only) |
| **land** | LandRequest | LandResult -> land-signal | Apply, commit, and report per-step output IDs |

The land-conflict-check controller always publishes a result — even when all steps are landable — so SubmitQueue receives a definitive answer. On infrastructure error it nacks for retry.

The land controller publishes a conflict result (and acks) when the land detects a conflict; SubmitQueue handles rebatching. On infrastructure error it nacks for retry. On success it publishes per-step outcomes (output IDs of the revisions produced) so SubmitQueue can update its request state.

## Terminal failures and dead-lettering

Runway is stateless and the sole responder on the client's correlation id: SubmitQueue records the in-flight work before publishing and waits for exactly one `LandResult` echoing that id. Every request must therefore resolve to a result — success or failure — or the client waits forever.

Failures split into two classes:

- **Named terminal outcomes** — a land conflict or an invalid request (an unknown/unsupported strategy, a malformed change URI, or an invalid `PROMOTE` composition). These can never succeed on retry, so the controller publishes a `FAILED` `LandResult` (with a reason) and acks, rather than nacking. The lander surfaces them as the `ErrConflict` / `ErrInvalidRequest` sentinels; `IsTerminal` is the single classification point.

- **Infrastructure faults** — fetch/network/auth failures, a push rejected for a reason other than a moved tip, and so on. These are nacked for retry.

An infrastructure fault that never recovers would exhaust retries and dead-letter. Because nothing consumed those dead-letter topics, such a request produced no signal and left the client's correlation id unresolved. Runway closes that gap with a **DLQ reconciler**: a dedicated consumer subscribes to the inbound topics' `_dlq` queues and, for each dead-lettered request, republishes a `FAILED` `LandResult` (echoing the correlation id) to the corresponding signal topic. Unlike the SubmitQueue/Stovepipe DLQ reconcilers it writes no entity state — the signal is the resolution. It runs under an always-retryable error policy so a transient publish failure retries indefinitely rather than dead-lettering again. A payload that cannot even be decoded carries no correlation id and is dropped.

Together these guarantee the client's correlation id always resolves: the primary controllers handle what they can name, and the reconciler is the backstop for everything else.

## Idempotency

Runway has no persistent state — no request store, no job store, no database. Idempotency is achieved through the VCS contract: land detects already-pushed changes (revisions reachable from HEAD) and treats them as already-landed. Land-conflict check is read-only and naturally idempotent.

## Ownership by service

### Runway

Runway is a single service. It subscribes to two inbound topics (`land-conflict-check`, `runway-land`), performs VCS operations through a pluggable extension, and publishes results to two outbound topics (`land-conflict-check-signal`, `land-signal`). It owns no persistent data.

### Shared: the messaging queue

Runway communicates with SubmitQueue only through the messaging queue. The contract is owned by Runway and published under `api/runway/messagequeue/`; both inbound and outbound topic keys live there. SubmitQueue publishes `LandRequest` messages and consumes the `LandResult` signals.
