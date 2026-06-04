# Pusher

Pluggable abstraction for landing a list of `entity.Change` values onto a
target branch and pushing the result to a source-control remote.

## Interface

`Pusher` exposes a single `Push` method that accepts a list of changes.
Implementations are bound to a specific `(checkout, remote, target)` tuple
at construction time, so the interface itself stays vendor- and
configuration-agnostic.

The interface enforces an **all-or-nothing atomicity contract**: when `Push`
returns an error, no change has reached the remote — neither partially nor
fully. Callers can treat a non-nil error as "the remote is exactly as it was
before the call". The `ErrConflict` sentinel marks user-caused failures so
callers can route them to a non-retry path.

A successful `Push` returns one `ChangeOutcome` per input change in input
order. Each outcome reports either:

- `OutcomeStatusCommitted` with the list of `CommitSHAs` produced on the
  target branch (one change can land as multiple commits, e.g. a stack of
  PRs); or
- `OutcomeStatusAlreadyExisted` with no commits, when the change is already
  present on the target branch (previously landed via another path, or
  subsumed by an earlier change in the same push). Git surfaces this as
  "rebased out" during a cherry-pick.

## Implementations

- [`git/`](git/) — applies changes against a local checkout via `git
  cherry-pick`, then `git push`. Construction takes the path to the
  checkout, the remote name, and the target branch; the implementation
  owns that working tree and serializes concurrent invocations.
- [`fake/`](fake/) — test/example stub. Reports every change as committed
  unless a change URI carries a failure marker (`sq-fake=conflict` →
  `ErrConflict`, `sq-fake=push-error` → error), letting a single running
  stack exercise negative paths from request payloads. Not for production.

## Adding a new backend

1. Create `extension/pusher/{backend}/` with a `Pusher` implementation.
2. Bind the implementation to its checkout/remote/target at construction.
3. Map each `entity.Change` to the backend's commit/push primitives.
4. Honour the atomicity contract: never publish partial state. Return
   `ErrConflict` (wrapped) for user-caused apply failures and a plain error
   for transient infra failures.
