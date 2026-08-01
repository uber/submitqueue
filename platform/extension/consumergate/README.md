# Consumer Gate Extension

Runtime stop/start of individual queue controllers without stopping the service that hosts them. The file implementation is explicitly scoped to deterministic e2e scenarios and single-host development; fleet-wide production pause requires a shared backend that is not part of this change. Design: [doc/rfc/consumer-gate.md](../../../doc/rfc/consumer-gate.md).

## Contract

A gate is identified by a consumer group (every controller subscribes with a unique one, so it is the controller's stable runtime name), optionally narrowed to a single partition. The gate owns the admission check and the parked-delivery observation records; it never waits. `Enter` checks a delivery's gate key synchronously and returns an `Entry`. When the entry is blocked, the caller records the observation with `Park` (the implementation stamps the entered identity and `ParkedAtMs`) and defers the delivery itself — the consumer postpones it, so the same message redelivers after a re-check delay and passes through `Enter` again. When the gate has opened, the caller removes the record with `Unpark` (a no-op when nothing was parked, so the admit path may call it unconditionally) and proceeds. Stopping is a barrier, not preemption — a delivery already past its gate is not recalled.

The gate does not own the source queue delivery and cannot acknowledge, nack, reject, postpone, remove, or move it. Deferring a blocked delivery is the caller's action; the gate only records what is waiting.

The package defines three interfaces:

- `Gate` exposes `Enter`, a synchronous check keyed on consumer group and partition that returns an `Entry`.
- `Entry` exposes `Blocked`, plus `Park`/`Unpark` over a `DeliveryDescriptor` containing only caller-owned message data; the implementation combines that descriptor with the gate identity captured by `Enter` to create (or remove) the observable `Parked` record. Re-parking the same delivery on a re-check overwrites its record, so records stay bounded by currently blocked deliveries. Callers that never need gating wire the `noop/` implementation.
- `Admin` is the write surface tests and tooling use: close a gate, open it, list what a stopped controller is holding.

Parked records are the "observe" half of stop/observe/start: awaiting one is the only way to *know* a stop caught a specific message (as opposed to the message not having arrived yet). The record is removed on the admit path once the gate opens, so the parked tree remains a view of current state rather than an unbounded delivery history.

## Failure posture

An `Enter`, `Park`, or `Unpark` that cannot read or write gate state surfaces the error to its caller without further interpretation. What to do with a failed check — for example, letting the delivery through — is the caller's policy, not the gate's.

## Implementations

- [file/](file/) — an explicit E2E and single-host development implementation using a shared directory. In the E2E stack the directory is bind-mounted into every service container, so the test process manipulates gates and reads parked records as local files. It coordinates only processes that see that directory and is not a fleet-wide production control plane.
- [noop/](noop/) — a no-op gate whose Enter always returns an unblocked Entry, for callers that do not need runtime gating.
