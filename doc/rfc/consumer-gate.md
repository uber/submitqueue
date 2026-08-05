# Consumer Gate

Stopping and starting individual queue controllers at runtime for deterministic e2e scenario control and single-host development, without stopping the service that hosts them.

## Problem

The pipeline is a set of queue controllers spread across three services, and deterministic tests need the same primitive repeatedly: *stop one controller from taking new messages, observe some condition while it is stopped, then start it again.*

- **E2e scenario control.** A test that must interleave two in-flight messages ("the batch controller must not consume its message before the cancel controller has finished") needs to halt exactly one controller while its siblings keep running. Stopping the whole service is too coarse: it kills every controller in the process, re-assigns ephemeral ports, and turns a scenario step into a container lifecycle event.
- **Single-host development.** A developer reproducing an ordering-sensitive scenario locally needs the same stop/observe/start control without restarting the whole stack.

Nothing in the system expresses this today. The queue can be manipulated from outside (e.g. starving a consumer by occupying its partition leases), but data-plane tricks of that kind are structurally limited: they only work if arranged before the controller ever touches the partition, they couple the caller to one backend's internals, and they are invisible to the service. This RFC makes stop/observe/start a small, coherent mechanism for tests and local development instead. Fleet-wide production pause is explicitly out of scope for the file implementation.

## Decisions

### The gate is consumer middleware, acting on deliveries before the controller

The consumer framework already owns the two facts that make an in-process gate clean. Dispatch is **serial per partition** — `consumeLoop` routes each delivery to a per-partition goroutine, and the next delivery of a partition is not started until the current one completes — so stopping one delivery stops exactly that partition and nothing else. And the framework **owns the delivery outcome** — controllers signal it only through `Process`'s return value — so a delivery can be stopped simply by never invoking the controller.

The gate is a check installed by the consumer ahead of every registered controller. Before invoking `Process`, it consults gate state for the controller's consumer group. If the gate is closed, the delivery is **parked**: the check writes the observable parked record and then **postpones** the delivery — the same hold/postpone primitive controllers use to wait (see [Consumer Hold](consumer-hold.md)) — so the message goes back to the queue invisible for a short re-check delay, acts as a barrier its partition waits behind, and redelivers without consuming retry budget. Each redelivery re-checks the gate: still closed re-parks and re-postpones; open removes the parked record and proceeds into the controller in partition order. Nothing waits in memory — no goroutine blocks, no visibility lease is renewed, and a process death while a delivery is parked loses nothing, because the parked state *is* queue state.

Stopping is a barrier, not preemption: a message already inside `Process` when the gate closes runs to completion; the gate guarantees no *new* message enters the controller.

The gate is an external stop lever — closed and opened from outside the controller. A controller that itself needs to wait (backing off for a budget slot, polling a slow status) should not reach for the gate; it holds its own delivery directly — see [Consumer Hold](consumer-hold.md). The gate is the same postpone mechanism applied *before* the controller by an *external* decision.

The re-check loop has a bounded cost: while a gate is closed, each blocked partition redelivers its head message once per re-check delay (~1s), re-reading gate state and rewriting one parked record per cycle. At test volumes this is noise, and it buys the property that matters: deliveries already fetched into the consumer's in-memory buffer behind a blocked one are each postponed in turn as the partition drains, so nothing sits in memory accumulating visibility lapses.

### Gate identity: consumer group, optionally narrowed to a partition

Every controller subscribes with a unique consumer group (`orchestrator-batch`, `runway-merge`, …), so the consumer group *is* the controller's stable runtime name — the natural key for "stop this controller". A gate may optionally carry a partition key; absent one, it gates every partition. Partition-scoped gates keep unrelated traffic flowing through the same controller (e.g. parking one test queue's partition while other queues proceed), which matters if e2e scenarios ever run concurrently.

### Gate state is a separate extension

The consumer gate is a shared extension in its own right, not a feature of any queue backend. The contract lives at `platform/extension/consumergate/`: the behavioral interface the consumer reads and the write surface tests and tooling use. `Park` accepts a caller-owned `DeliveryDescriptor` containing only message data; the implementation combines it with the gate identity captured by `Enter` and its own timestamp to create the observable `Parked` record, so callers cannot supply or overwrite gate-owned fields, and `Unpark` removes the record on the admit path. The consumer package takes the read-side interface as a dependency; wiring passes the file implementation only when `CONSUMER_GATE_DIR` is explicitly configured and otherwise passes the no-op implementation.

Keeping the contract separate from any backend is what lets the storage medium be chosen per deployment: a filesystem directory first (below), a database- or config-service-backed implementation later if fleet-wide coordination demands it — with the middleware, the wiring shape, and every test written against the contract unchanged.

### E2E and local implementation: files in a shared directory

The first implementation stores gate state as plain files under a configured directory. Presence of a gate file means the gate is closed; deleting the file opens it. Parked deliveries are recorded as JSON files. The layout:

```
{dir}/gates/{consumer_group}/all                       # gates every partition of the controller
{dir}/gates/{consumer_group}/p-{urlenc(partition)}     # gates one partition
{dir}/parked/{consumer_group}/{topic}/{urlenc(id)}.json  # one parked delivery record
```

Consumer groups and topics are already filesystem-safe by the repo's naming rules; partition keys and message IDs may contain `/` (request IDs like `queue/1`), so they are URL-encoded in file names. Gate files contain human-readable JSON metadata — `reason`, `created_by`, `created_at_ms` — so an operator finding a paused controller can tell why. Parked records carry the payload, attempt, and `parked_at_ms` while a delivery is blocked; each re-check of a still-closed gate refreshes the record, and the admit path removes it once the gate opens, so payloads are not retained after release. All writes go through temp-file-plus-rename so readers never see partial JSON.

Files are the simplest medium for the E2E and single-host scope:

- **Developer interface for free.** Pausing a controller is writing a small file; resuming is `rm`. Inspecting a paused stage is `ls` and `cat`. No client, no schema, no query.
- **Trivially reachable out of process.** In the e2e stack, the compose file bind-mounts a host directory into every service container at a fixed path (passed via one environment variable); the test process manipulates gates and reads parked records as local files. In single-host dev the same directory works as-is.
- **Independent of the queue database.** Gate state remains available while the configured directory remains available.

The store never watches the directory: filesystem notifications such as inotify are platform-specific, can overflow or require re-registration, and behave differently across bind mounts, overlay or network filesystems, rootless Docker, and Docker Desktop's host/container filesystem bridge. Instead, gate state is re-read on every delivery attempt — and a blocked delivery's re-check cadence is the consumer's postpone delay, which is the portable convergence mechanism regardless of medium.

The file implementation gates only processes that see the same directory. Its state survives a process or container restart only when that directory is backed by storage that survives the restart, and it does not survive node replacement unless the storage is shared and persistent. It is not a fleet-wide production control plane. Services therefore enable it only through the explicit `CONSUMER_GATE_DIR` opt-in; otherwise they wire the no-op gate.

### Read path: direct reads and bounded release latency

The check reads the applicable gate state for every delivery. A blocked delivery is postponed for a short re-check delay (~1s) and re-checks on redelivery. Closing a gate therefore affects the next delivery check without waiting for a cache refresh; opening one releases blocked deliveries within one re-check delay plus the queue's own poll interval.

Tests do not depend on that latency. The deterministic patterns are two: **arrange first** (close the gate before publishing the message that must be caught — exact by construction), or **await the observed effect** (the parked record, below) instead of assuming timing.

### Observation: parked deliveries are recorded

Parking writes the parked record before the delivery is postponed. This record is the "observe" half of stop/observe/start: a test awaits the record to *know* the stop caught its message (there is otherwise no signal distinguishing "gated and parked" from "not arrived yet"), can assert on the recorded payload, and can decide what to do next while the controller is provably stopped. While the gate stays closed the record is continuously refreshed by the re-check loop; the admit path removes it once the gate opens, so records are bounded by currently parked messages and the directory is empty whenever no delivery is blocked behind a gate.

### Failure posture: fail open

If gate state cannot be read (directory missing, I/O error), the check logs, increments an error counter, and lets deliveries through. A failed parked-record write is logged and the delivery is still postponed — the record is observability, never the mechanism. A failed postpone is abandoned like a failed acknowledge: the visibility timeout lapses into a normal redelivery, which re-checks the gate (that redelivery consumes one retry attempt, bounded per lapse). Gating is auxiliary; a broken gate medium must not become a pipeline stall. The consequence — a closed gate is best-effort under infra failure — is acceptable because tests assert observed effects (parked records, downstream state), not the mechanism.

## Test walk-through

The cancellation scenario, expressed as stop → observe → start:

1. The test closes the gate for `runway-mergeconflictcheck` (all partitions, or scoped to the test queue's partition key), before landing.
2. It lands a request. The orchestrator runs it to the merge-conflict-check hand-off; runway's subscriber delivers the check message, and the gate parks it.
3. The test awaits the parked record — proof the controller is stopped *and* holding exactly this message. Runway itself is still running; its RPC surface and merge controller are untouched.
4. While stopped, the test observes and acts: it cancels the request, awaits the terminal `cancelled` status through the existing event plane, and asserts no batch ever enrolled the request.
5. The test opens the gate. Within a re-check tick the postponed delivery redelivers, clears the open gate, and proceeds into the controller as a fresh attempt (postponing resets retry accounting); runway answers the now-stale check, and the test asserts the signal is dropped for the halted request.

The two-controller interleaving scenario is the same shape: close controller X's gate, drive both messages, await X's parked record and Y's downstream effect in the required order, open X's gate.

## Rejected

- **Starving a controller through the queue's data plane** (e.g. occupying its partition leases from outside the service). Needs zero service changes, which is why it was the harness's first candidate, but it is pre-hold-only (an actively consuming controller cannot be stopped), coupled to one backend's scheduling internals, and invisible to the service — no observation, no metrics, nothing reusable by an operator. Once service modifications are on the table, the middleware dominates it.
- **Parking deliveries in memory** (block the partition goroutine on a blocked delivery, extending its visibility until the gate opens — this design's original mechanism). It holds a goroutine and a lease per blocked partition, and it only babysits the delivery it parks: messages already fetched into the per-partition buffer behind it (up to the batch size) have their visibility lapse, redeliver as duplicates that stuff the buffer until the topic's routing loop stalls, and burn a retry attempt per lapse until they are spuriously dead-lettered without ever failing. Postponing each blocked delivery back to the queue removes the held state entirely and inherits hold's retry exemption; the costs are a ~1s release quantization and the released message being a fresh attempt rather than the same one.
- **A database-backed store in this E2E-focused change.** A shared store is the appropriate direction if fleet-wide production pause is required, but it needs its own availability, caching, and administration design. It should be added as another `consumergate` implementation rather than coupled to a queue backend.
- **Admin RPC on each service for pause/resume.** Reaches the same middleware, but costs a new proto surface, port, and auth story on three services. The shared test directory already supplies the out-of-process control required here.
- **Config/env-driven controller enablement plus restart.** Restart granularity is the whole process — it bounces every sibling controller and disturbs in-flight leases, destroying exactly the "others keep running" property mid-scenario. A static topology tool, not a stop/start lever.
- **Fail closed on gate-state errors.** Turns an auxiliary medium's failure into a full consumption stall across every gated consumer. The gate exists to *add* control, not to add a new way for the pipeline to stop on its own.
- **Message-level breakpoint rules in v1** (match on message ID or payload, single-step release). The parked records and the gate key structure leave room for this evolution, but stop/observe/start at controller and partition granularity covers every scenario currently in hand; rule matching, rule versioning, and partial release are complexity deferred until a test needs them.
