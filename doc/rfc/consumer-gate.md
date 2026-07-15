# Consumer Gate

Stopping and starting individual queue controllers at runtime for deterministic e2e scenario control and single-host development, without stopping the service that hosts them.

## Problem

The pipeline is a set of queue controllers spread across three services, and deterministic tests need the same primitive repeatedly: *stop one controller from taking new messages, observe some condition while it is stopped, then start it again.*

- **E2e scenario control.** A test that must interleave two in-flight messages ("the batch controller must not consume its message before the cancel controller has finished") needs to halt exactly one controller while its siblings keep running. Stopping the whole service is too coarse: it kills every controller in the process, re-assigns ephemeral ports, and turns a scenario step into a container lifecycle event.
- **Single-host development.** A developer reproducing an ordering-sensitive scenario locally needs the same stop/observe/start control without restarting the whole stack.

Nothing in the system expresses this today. The queue can be manipulated from outside (e.g. starving a consumer by occupying its partition leases), but data-plane tricks of that kind are structurally limited: they only work if arranged before the controller ever touches the partition, they couple the caller to one backend's internals, and they are invisible to the service. This RFC makes stop/observe/start a small, coherent mechanism for tests and local development instead. Fleet-wide production pause is explicitly out of scope for the file implementation.

## Decisions

### The gate is consumer middleware, acting on deliveries before the controller

The consumer framework already owns the two facts that make an in-process gate clean. Dispatch is **serial per partition** — `consumeLoop` routes each delivery to a per-partition goroutine, and the next delivery of a partition is not started until the current one completes — so holding one delivery blocks exactly that partition and nothing else. And the framework **owns ack/nack** — controllers signal outcome only through `Process`'s return value — so a delivery can be held simply by not yet invoking the controller.

The gate is a decorator installed by the consumer around every registered controller. Before invoking `Process`, it consults gate state for the controller's consumer group. If the gate is closed, the delivery is **parked**: the decorator blocks in place, keeping that delivery in flight and periodically calling `ExtendVisibilityTimeout` — already part of the `Delivery` contract, and specified to *not* increment the retry count — until the gate opens or the consumer shuts down. Gating does not acknowledge, nack, reject, remove, or move the source delivery; it remains owned by the queue for the same consumer group and partition. When the gate opens, the same delivery proceeds into the controller in partition order. If the process dies or shuts down while parked, extension stops, visibility lapses, and the queue makes the delivery eligible for normal redelivery.

Stopping is a barrier, not preemption: a message already inside `Process` when the gate closes runs to completion; the gate guarantees no *new* message enters the controller.

One bounded side effect is accepted and documented: the routing loop feeds each partition through a channel buffered at the subscription's batch size, so if messages keep arriving for a parked partition, the topic's routing loop eventually stalls once that buffer fills. For a fully closed gate this is moot (every partition parks anyway), and at test volumes it never triggers.

### Gate identity: consumer group, optionally narrowed to a partition

Every controller subscribes with a unique consumer group (`orchestrator-batch`, `runway-merge`, …), so the consumer group *is* the controller's stable runtime name — the natural key for "stop this controller". A gate may optionally carry a partition key; absent one, it gates every partition. Partition-scoped gates keep unrelated traffic flowing through the same controller (e.g. parking one test queue's partition while other queues proceed), which matters if e2e scenarios ever run concurrently.

### Gate state is a separate extension

The consumer gate is a shared extension in its own right, not a feature of any queue backend. The contract lives at `platform/extension/consumergate/`: the behavioral interface the consumer reads, the write surface tests and tooling use, and the `Config`. `Watch` accepts a caller-owned `DeliveryDescriptor` containing only message data; the implementation combines it with the gate identity captured by `Enter` and its own timestamp to create the observable `Parked` record, so callers cannot supply or overwrite gate-owned fields. Implementations live in subdirectories, per the standard extension layout. The consumer package takes the read-side interface as a dependency; wiring passes the file implementation only when `CONSUMER_GATE_DIR` is explicitly configured and otherwise passes the no-op implementation.

Keeping the contract separate from any backend is what lets the storage medium be chosen per deployment: a filesystem directory first (below), a database- or config-service-backed implementation later if fleet-wide coordination demands it — with the middleware, the wiring shape, and every test written against the contract unchanged.

### E2E and local implementation: files in a shared directory

The first implementation stores gate state as plain files under a configured directory. Presence of a gate file means the gate is closed; deleting the file opens it. Parked deliveries are recorded as JSON files. The layout:

```
{dir}/gates/{consumer_group}/all                       # gates every partition of the controller
{dir}/gates/{consumer_group}/p-{urlenc(partition)}     # gates one partition
{dir}/parked/{consumer_group}/{topic}/{urlenc(id)}.json  # one parked delivery record
```

Consumer groups and topics are already filesystem-safe by the repo's naming rules; partition keys and message IDs may contain `/` (request IDs like `queue/1`), so they are URL-encoded in file names. Gate files contain human-readable JSON metadata — `reason`, `created_by`, `created_at_ms` — so an operator finding a paused controller can tell why. Parked records carry the payload, attempt, and `parked_at_ms` while a delivery is blocked; the record is deleted before the wait ends, so payloads are not retained after release, cancellation, or monitoring failure. All writes go through temp-file-plus-rename so readers never see partial JSON.

Files are the simplest medium for the E2E and single-host scope:

- **Developer interface for free.** Pausing a controller is writing a small file; resuming is `rm`. Inspecting a paused stage is `ls` and `cat`. No client, no schema, no query.
- **Trivially reachable out of process.** In the e2e stack, the compose file bind-mounts a host directory into every service container at a fixed path (passed via one environment variable); the test process manipulates gates and reads parked records as local files. In single-host dev the same directory works as-is.
- **Independent of the queue database.** Gate state remains available while the configured directory remains available.

The middleware **polls** the directory rather than using filesystem notifications: inotify is platform-specific, watches can overflow or require re-registration, and event behavior varies across bind mounts, overlay or network filesystems, rootless Docker, and Docker Desktop's host/container filesystem bridge. Polling is the portable convergence mechanism; filesystem events may be added later as an optional wakeup optimization alongside it.

The file implementation gates only processes that see the same directory. Its state survives a process or container restart only when that directory is backed by storage that survives the restart, and it does not survive node replacement unless the storage is shared and persistent. It is not a fleet-wide production control plane. Services therefore enable it only through the explicit `CONSUMER_GATE_DIR` opt-in; otherwise they wire the no-op gate.

### Read path: direct reads and bounded release latency

The middleware checks the applicable gate files for every delivery. A parked delivery re-checks them on a short interval (configurable, ~1s). Closing a gate therefore affects the next delivery check without waiting for a cache refresh; opening one releases already parked deliveries within one poll interval.

Tests do not depend on that latency. The deterministic patterns are two: **arrange first** (close the gate before publishing the message that must be caught — exact by construction), or **await the observed effect** (the parked record, below) instead of assuming timing.

### Observation: parked deliveries are recorded

Parking writes the parked record before blocking. This record is the "observe" half of stop/observe/start: a test awaits the record to *know* the stop caught its message (there is otherwise no signal distinguishing "gated and parked" from "not arrived yet"), can assert on the recorded payload, and can decide what to do next while the controller is provably stopped. The record is removed before the wait reports release, cancellation, or failure, so records are bounded by currently parked messages and the directory is empty whenever no delivery is held behind a gate.

### Failure posture: fail open

If gate state cannot be read (directory missing, I/O error), the middleware logs, increments an error counter, and lets deliveries through. Gating is auxiliary; a broken gate medium must not become a pipeline stall. The consequence — a closed gate is best-effort under infra failure — is acceptable because tests assert observed effects (parked records, downstream state), not the mechanism.

## Test walk-through

The cancellation scenario, expressed as stop → observe → start:

1. The test closes the gate for `runway-mergeconflictcheck` (all partitions, or scoped to the test queue's partition key), before landing.
2. It lands a request. The orchestrator runs it to the merge-conflict-check hand-off; runway's subscriber delivers the check message, and the gate parks it.
3. The test awaits the parked record — proof the controller is stopped *and* holding exactly this message. Runway itself is still running; its RPC surface and merge controller are untouched.
4. While stopped, the test observes and acts: it cancels the request, awaits the terminal `cancelled` status through the existing event plane, and asserts no batch ever enrolled the request.
5. The test opens the gate. Within a refresh tick the parked delivery proceeds into the controller as the same attempt; runway answers the now-stale check, and the test asserts the signal is dropped for the halted request.

The two-controller interleaving scenario is the same shape: close controller X's gate, drive both messages, await X's parked record and Y's downstream effect in the required order, open X's gate.

## Rejected

- **Starving a controller through the queue's data plane** (e.g. occupying its partition leases from outside the service). Needs zero service changes, which is why it was the harness's first candidate, but it is pre-hold-only (an actively consuming controller cannot be stopped), coupled to one backend's scheduling internals, and invisible to the service — no observation, no metrics, nothing reusable by an operator. Once service modifications are on the table, the middleware dominates it.
- **A database-backed store in this E2E-focused change.** A shared store is the appropriate direction if fleet-wide production pause is required, but it needs its own availability, caching, and administration design. It should be added as another `consumergate` implementation rather than coupled to a queue backend.
- **Admin RPC on each service for pause/resume.** Reaches the same middleware, but costs a new proto surface, port, and auth story on three services. The shared test directory already supplies the out-of-process control required here.
- **Config/env-driven controller enablement plus restart.** Restart granularity is the whole process — it bounces every sibling controller and disturbs in-flight leases, destroying exactly the "others keep running" property mid-scenario. A static topology tool, not a stop/start lever.
- **Fail closed on gate-state errors.** Turns an auxiliary medium's failure into a full consumption stall across every gated consumer. The gate exists to *add* control, not to add a new way for the pipeline to stop on its own.
- **Message-level breakpoint rules in v1** (match on message ID or payload, single-step release). The parked records and the gate key structure leave room for this evolution, but stop/observe/start at controller and partition granularity covers every scenario currently in hand; rule matching, rule versioning, and partial release are complexity deferred until a test needs them.
