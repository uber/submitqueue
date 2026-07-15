# Consumer Gate Extension

Runtime stop/start of individual queue controllers — for deterministic e2e scenario control and for operational pause of a consuming stage — without stopping the service that hosts them. Design: [doc/rfc/consumer-gate.md](../../../doc/rfc/consumer-gate.md).

## Contract

A gate is identified by a consumer group (every controller subscribes with a unique one, so it is the controller's stable runtime name), optionally narrowed to a single partition. The gate owns both the admission mechanism and the parked-delivery observation records: `Enter` checks a delivery's gate key synchronously, and a blocked `Entry`'s `Watch` records the parked delivery (stamping the entered identity and `ParkedAtMs`) and returns a channel that yields once — `nil` when the gate opens, or an error if gate state cannot be read or written. The record is removed before `Watch` yields on every terminal path, so parked records describe only deliveries currently blocked behind a gate. Handing back a channel rather than blocking lets the caller multiplex the wait against its own events (context cancellation, visibility extension) in a single select; the package-level `Wait` helper wraps `Watch` for callers that only need the simple blocking behaviour. Stopping is a barrier, not preemption — a delivery already past its gate is not recalled.

The package defines three interfaces, the package-level `Wait` helper, plus the `Config`:

- `Gate` exposes `Enter`, a synchronous check keyed on consumer group and partition that returns an `Entry` — a future the caller inspects with `Blocked` and, only when blocked, watches with `Watch`, supplying a `DeliveryDescriptor` containing only caller-owned message data. The implementation combines that descriptor with the gate identity captured by `Enter` and its own parked timestamp to create the observable `Parked` record. `Watch` returns a channel; the free function `Wait` blocks on it for callers that do not multiplex. Polling implementations (see `file/`) re-check gate state on a timer; notification-capable implementations can release the instant the gate opens. Callers that never need gating wire the `noop/` implementation.
- `Admin` is the write surface tests and tooling use: close a gate, open it, list what a stopped controller is holding.

Parked records are the "observe" half of stop/observe/start: awaiting one is the only way to *know* a stop caught a specific message (as opposed to the message not having arrived yet). Once the wait ends, the record is removed so the parked tree remains a view of current state rather than an unbounded delivery history.

## Failure posture

An `Enter` or `Watch` that cannot read or record gate state surfaces the error to its caller without further interpretation. What to do with a failed check — for example, letting the delivery through — is the caller's policy, not the gate's.

## Implementations

- [file/](file/) — gate state as plain files in a shared directory. Pausing a controller is writing a small file, resuming is `rm`, inspecting a paused stage is `ls` and `cat`. In the e2e stack the directory is bind-mounted into every service container, so the test process manipulates gates and reads parked records as local files. Enter reads the applicable gate files for every delivery; a blocked entry's watch goroutine polls them on a configurable interval.
- [noop/](noop/) — a no-op gate whose Enter always returns an unblocked Entry, for callers that do not need runtime gating.
