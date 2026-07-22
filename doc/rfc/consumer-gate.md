# Consumer Gate

Stopping and starting individual queue controllers at runtime — for deterministic e2e scenario control and for operational pause of a consuming stage — without stopping the service that hosts them.

## Problem

The pipeline is a set of queue controllers spread across three services, and several needs reduce to the same primitive: *stop one controller from taking new messages, observe some condition while it is stopped, then start it again.*

- **E2e scenario control.** A test that must interleave two in-flight messages ("the batch controller must not consume its message before the cancel controller has finished") needs to halt exactly one controller while its siblings keep running. Stopping the whole service is too coarse: it kills every controller in the process, re-assigns ephemeral ports, and turns a scenario step into a container lifecycle event.
- **Operational pause.** During an incident, the safest first move is often "stop consuming" — park a stage that is processing poison input or hammering a struggling dependency — without redeploying or scaling to zero, and while keeping the rest of the service (RPC surface, other controllers) alive.

Nothing in the system expresses this today. The queue can be manipulated from outside (e.g. starving a consumer by occupying its partition leases), but data-plane tricks of that kind are structurally limited: they only work if arranged before the controller ever touches the partition, they couple the caller to one backend's internals, and they are invisible to the service — no first-class semantics, no metrics, nothing an operator can reuse. This RFC makes stop/observe/start a small, coherent, first-class mechanism instead.

## Decisions

### The gate is consumer middleware, acting on deliveries before the controller

The consumer framework already owns the two facts that make an in-process gate clean. Dispatch is **serial per partition** — `consumeLoop` routes each delivery to a per-partition goroutine, and the next delivery of a partition is not started until the current one completes — so holding one delivery blocks exactly that partition and nothing else. And the framework **owns ack/nack** — controllers signal outcome only through `Process`'s return value — so a delivery can be held simply by not yet invoking the controller.

The gate is a decorator installed by the consumer around every registered controller. Before invoking `Process`, it consults gate state for the controller's consumer group. If the gate is closed, the delivery is **parked**: the decorator blocks in place, keeping the delivery in-flight and periodically calling `ExtendVisibilityTimeout` — already part of the `Delivery` contract, and specified to *not* increment the retry count — until the gate opens or the consumer shuts down. When the gate opens, the parked delivery proceeds into the controller as the same attempt, in partition order. Parked messages therefore never burn retry budget, never touch the DLQ, and are never lost: if the process dies while parked, extension stops, visibility lapses, and the queue redelivers normally.

Stopping is a barrier, not preemption: a message already inside `Process` when the gate closes runs to completion; the gate guarantees no *new* message enters the controller.

One bounded side effect is accepted and documented: the routing loop feeds each partition through a channel buffered at the subscription's batch size, so if messages keep arriving for a parked partition, the topic's routing loop eventually stalls once that buffer fills. For a fully closed gate this is moot (every partition parks anyway), and at test volumes it never triggers.

### Gate identity: consumer group, optionally narrowed to a partition

Every controller subscribes with a unique consumer group (`orchestrator-batch`, `runway-merge`, …), so the consumer group *is* the controller's stable runtime name — the natural key for "stop this controller". A gate may optionally carry a partition key; absent one, it gates every partition. Partition-scoped gates keep unrelated traffic flowing through the same controller (e.g. parking one test queue's partition while other queues proceed), which matters if e2e scenarios ever run concurrently.

### Gate state is a separate extension

The consumer gate is a shared extension in its own right, not a feature of any queue backend. The contract lives at `platform/extension/consumergate/`: the behavioral interface the middleware reads (is this group/partition gated? record a parked delivery, record its release), the write surface tests and tooling use (close a gate, open it), and the `Config`. `Watch` accepts a caller-owned `DeliveryDescriptor` containing only message data; the implementation combines it with the gate identity captured by `Enter` and its own timestamp to create the observable `Parked` record, so callers cannot supply or overwrite gate-owned fields. Implementations live in subdirectories, per the standard extension layout. The consumer package takes the read-side interface as a dependency — wiring constructs an implementation and passes it to `consumer.New` via a new option; when no gate is configured, the middleware is absent and the consumer behaves exactly as today. The wiring delta is one option argument at each consumer construction site (gateway, orchestrator primary, orchestrator DLQ, runway); no per-controller wiring, and DLQ consumers are gated uniformly with the rest.

Keeping the contract separate from any backend is what lets the storage medium be chosen per deployment: a filesystem directory first (below), a database- or config-service-backed implementation later if fleet-wide coordination demands it — with the middleware, the wiring shape, and every test written against the contract unchanged.

### First implementation: files in a shared directory

The first implementation stores gate state as plain files under a configured directory. Presence of a gate file means the gate is closed; deleting the file opens it. Parked deliveries are recorded as JSON files. The layout:

```
{dir}/gates/{consumer_group}/all                       # gates every partition of the controller
{dir}/gates/{consumer_group}/p-{urlenc(partition)}     # gates one partition
{dir}/parked/{consumer_group}/{topic}/{urlenc(id)}.json  # one parked delivery record
```

Consumer groups and topics are already filesystem-safe by the repo's naming rules; partition keys and message IDs may contain `/` (request IDs like `queue/1`), so they are URL-encoded in file names. Gate files contain human-readable JSON metadata — `reason`, `created_by`, `created_at_ms` — so an operator finding a paused controller can tell why. Parked records carry the payload, attempt, and `parked_at_ms` while a delivery is blocked; the record is deleted before the wait ends, so payloads are not retained after release, cancellation, or monitoring failure. All writes go through temp-file-plus-rename so readers never see partial JSON.

Files are the simplest medium that satisfies every requirement in this RFC, and simplicity is the point of the first implementation:

- **Operator interface for free.** Pausing a controller is writing a small file; resuming is `rm`. Inspecting a paused stage is `ls` and `cat`. No client, no schema, no query.
- **Trivially reachable out of process.** In the e2e stack, the compose file bind-mounts a host directory into every service container at a fixed path (passed via one environment variable); the test process manipulates gates and reads parked records as local files. In single-host dev the same directory works as-is.
- **Durable and independent.** State survives service restarts — a paused stage stays paused until explicitly opened — and the gate has no dependency on the queue backend or any database being healthy.

The middleware **polls** the directory rather than using filesystem notifications: inotify is platform-specific, watches can overflow or require re-registration, and event behavior varies across bind mounts, overlay or network filesystems, rootless Docker, and Docker Desktop's host/container filesystem bridge. Polling is the portable convergence mechanism; filesystem events may be added later as an optional wakeup optimization alongside it. The known limit of the file medium is multi-replica fleets: a file gates the replicas that see the directory, so a fleet-wide pause needs the deployment platform to distribute the file — or a future store-backed implementation of the same contract. That trade is accepted; the deployments this RFC serves (e2e, single-host dev, per-instance operational pause) are exactly where files excel.

### Read path: direct reads and bounded release latency

The middleware checks the applicable gate files for every delivery. A parked delivery re-checks them on a short interval (configurable, ~1s). Closing a gate therefore affects the next delivery check without waiting for a cache refresh; opening one releases already parked deliveries within one poll interval.

Tests do not depend on that latency. The deterministic patterns are two: **arrange first** (close the gate before publishing the message that must be caught — exact by construction), or **await the observed effect** (the parked record, below) instead of assuming timing.

### Observation: parked deliveries are recorded

Parking writes the parked record before blocking. This record is the "observe" half of stop/observe/start: a test awaits the record to *know* the stop caught its message (there is otherwise no signal distinguishing "gated and parked" from "not arrived yet"), can assert on the recorded payload, and can decide what to do next while the controller is provably stopped. For an operator, the same records answer "what is this paused controller holding?". The record is removed before the wait reports release, cancellation, or failure, so records are bounded by currently parked messages and the directory is empty whenever no delivery is held behind a gate.

### Failure posture: fail open

If gate state cannot be read (directory missing, I/O error), the middleware logs, increments an error counter, and lets deliveries through. Gating is auxiliary; a broken gate medium must not become a pipeline stall. The consequence — a closed gate is best-effort under infra failure — is acceptable because tests assert observed effects (parked records, downstream state), not the mechanism, and an operator gets the failure loudly in metrics.

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
- **A database-backed store as the first implementation** (a table in the queue database). Reaches every replica and centralizes fleet-wide state, but costs a schema, a store, and migrations for what a directory of files does in the deployments at hand — and it ties gate availability to a database. It remains the natural second implementation of the same extension contract if fleet-wide coordination is ever needed.
- **Admin RPC on each service for pause/resume.** Reaches the same middleware, but costs a new proto surface, port, and auth story on three services, and state vanishes on restart. A shared medium gives out-of-process reach, durability, and one lever shared by tests and operators.
- **Config/env-driven controller enablement plus restart.** Restart granularity is the whole process — it bounces every sibling controller and disturbs in-flight leases, destroying exactly the "others keep running" property mid-scenario. A static topology tool, not a stop/start lever.
- **Fail closed on gate-state errors.** Turns an auxiliary medium's failure into a full consumption stall across every gated consumer. The gate exists to *add* control, not to add a new way for the pipeline to stop on its own.
- **Message-level breakpoint rules in v1** (match on message ID or payload, single-step release). The parked records and the gate key structure leave room for this evolution, but stop/observe/start at controller and partition granularity covers every scenario currently in hand; rule matching, rule versioning, and partial release are complexity deferred until a test needs them.
