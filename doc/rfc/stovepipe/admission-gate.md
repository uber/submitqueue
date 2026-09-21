# Stovepipe Admission Gates

Status: proposed. This RFC defines the framework and extension contract; storage and controller integration land separately.

## Problem

Stovepipe currently implements one build-admission policy directly in `process`: admit only while the queue's in-flight count is below its concurrency limit. New requirements, such as general admission throttling and a cooldown after a failed build, will continue to surface, indicating a need for a standard admission gating framework that individual controllers can use to gate work based on customized policy decisions.

The immediate requirements are:

- Limit concurrent logical validations per queue.
- Throttle admissions generally, for example to at most one start per hour.
- After a build runner reports a failed result, defer the next admission for a configured cooldown.
- Keep coalescing active while a request is deferred, so a newer head can supersede it without waiting for the gate to open.

The framework must also leave room for policies such as maintenance windows, resource budgets, provider health, or an operator hold, and for logical admission boundaries other than build admission.

## Scope

An **admission gate** decides whether one domain entity may cross a logical pipeline boundary. The first use is build admission: whether the latest accepted Stovepipe request may reserve validation capacity and advance toward the build stage.

This is separate from the shared [Consumer Gate](../consumer-gate.md). A consumer gate is an external operational control that stops deliveries before a controller. A Stovepipe admission gate is domain policy evaluated by a controller for a specific request. Both defer with queue redelivery, but they answer different questions and own different state.

## Contract

The vendor-neutral contract lives at `stovepipe/extension/admissiongate`:

```go
type Blocker string

type Result struct {
    Decision  Decision
    BlockedBy []Blocker
}

type Gate interface {
    TryAdmit(context.Context, entity.Request) (Result, error)
}

type Config struct {
    QueueName string
}

type Gates interface {
    For(Config) (Gate, error)
}
```

`Gates` is the host-owned resolver across queues for one logical boundary. The controller receives the resolver for the admission it performs. Like the `buildrunner`, `sourcecontrol`, and `storage` resolver contracts, `For` takes a typed `Config` containing only `QueueName`; wiring uses that identity to select and bind the implementation. It returns exactly one composite gate: returning a slice of independently stateful gates would make atomic admission impossible when one gate records a reservation before a later gate defers. Concrete routing belongs in service wiring, not an extension implementation package.

`TryAdmit` takes the thin `entity.Request`, following the repository's identity-in extension rule for request-stage decisions. Passing only its string ID would discard the queue and immutable request identity already available to the controller, force implementations to parse an ID or add another lookup merely to recover that identity, and diverge from other decision extensions that receive the stage entity. The request is a reference, not a bundle of pre-resolved policy facts: a gate reloads mutable state and resolves queue-scoped storage, configuration, request history, clocks, or remote services through dependencies injected when its implementation is constructed.

`Result` represents expected control flow:

- `DecisionAdmitted` means this request's admission was already recorded or has been durably recorded before the call returns.
- `DecisionDeferred` means no admission was recorded because one or more policies currently block it. `BlockedBy` contains typed `Blocker` values: stable, low-cardinality policy identifiers for logs and metrics, not human-readable errors. Each implementation defines constants for the policies it can report.
- `DecisionUnknown` is not a runtime outcome. It is the invalid zero value, consistent with entity enums elsewhere in the repository, so an implementation that accidentally returns an empty `Result` fails closed. The controller converts a nil-error result with this decision into an error.

Errors are reserved for failures to evaluate or durably record the decision. A closed gate is not an error and does not consume retry budget.

There is intentionally no `Complete`, `Release`, or `RecordOutcome` method. `TryAdmit` must be able to derive the current answer from durable state. This keeps build outcomes owned by the request lifecycle and prevents `buildsignal`, DLQ controllers, or future terminal paths from each needing policy-specific callbacks.

## Current Behavior

On `main`, `process` loads the queue and its `QueueConfig`, coalesces the request against `Queue.LatestRequestID`, and compares `Queue.InFlightCount` with `QueueConfig.MaxConcurrent`. When the queue is full, it calls `delivery.Hold(QueueConfig.GateWaitDelayMs)` and returns successfully. `GateWaitDelayMs` is only the delay before redelivery and another concurrency check; it does not impose a minimum interval between admitted builds. Redelivery starts from request loading and coalescing, so a newer head can supersede the deferred request.

When capacity is available, `process` claims it by incrementing `Queue.InFlightCount` with a queue-version CAS. A queue version conflict reloads the queue and repeats coalescing before another claim. After a terminal runner result, `buildsignal` decrements the counter before marking the request terminal. Relevant DLQ paths also decrement it so abandoned processing work does not permanently consume capacity. There is no minimum-admission timestamp, failure-cooldown state, or opaque admission payload on the queue today.

## Process Integration

`process` retains responsibility for request choreography; the gate owns only admission policy and its reservation. The integration preserves the current re-check behavior:

1. Load the request and queue, then coalesce it against the latest request ID.
2. Resolve the gate with `gates.For(admissiongate.Config{QueueName: request.Queue})`.
3. Call `TryAdmit(ctx, request)`.
4. On `DecisionDeferred`, hold the delivery for the queue's normal gate re-check delay and return successfully.
5. On redelivery, start again at coalescing before evaluating the gate.
6. On `DecisionAdmitted`, derive the build strategy, transition the request to `processing`, and publish to `build` using the existing persist-before-publish ordering.

The hold delay belongs to controller scheduling configuration, not `Result`. A policy may know an exact deadline, but sleeping until that deadline would suppress coalescing for its full duration. Frequent bounded re-checks preserve superseding and make all policies converge through the same path.

If the process dies after the gate records admission but before the request reaches `processing`, redelivery calls `TryAdmit` with the same request ID. The result is admitted without reserving twice, and the controller retries the transition. If the process message ultimately reaches its DLQ, the DLQ's terminal request transition becomes visible to later reconciliation.

## State And Storage

The extension contract does not prescribe storage. A stateless gate stores nothing; another implementation may use an implementation-owned table, a key-value store, or a remote quota service. Those dependencies are injected when the implementation is constructed, and neither `Gates` nor `Gate` exposes a generic state API.

The proposed standard composite build gate does need a small amount of durable state for idempotent reservations, concurrency reconciliation, minimum-interval history, and failure cooldown. Its first implementation adds `AdmissionState []byte` to `entity.Queue` and appends an `admission_state BLOB` column to the end of the MySQL queue schema. `QueueStore` only round-trips those bytes as part of the existing versioned queue snapshot; it does not parse, validate, merge, or version the payload.

Keeping this implementation's state on the queue is deliberate rather than a framework requirement. The standard build gate must order its reservation with `Queue.LatestRequestID`, matching the current queue CAS that prevents a newly superseded head from claiming capacity. A separate table would give admission state its own CAS but could not atomically observe the latest-head update without a cross-entity transaction. A different gate whose facts do not need that ordering should own its own table or backend instead of adding data to this payload.

The standard gate exclusively owns the payload's encoding and compatibility. Its initial implementation uses versioned JSON because the state is small and operationally inspectable, but the storage contract is opaque bytes rather than a JSON contract. Another gate does not read or write this envelope. Changing the standard implementation for a live queue requires that the replacement understand or explicitly migrate the prior payload.

A representative initial payload is:

```json
{
  "version": 1,
  "active_request_ids": [
    "request/monorepo/main/42"
  ],
  "policies": {
    "minimum_interval": {
      "last_admitted_at_ms": 1789506000000
    },
    "failure_cooldown": {
      "not_before_ms": 1789509600000
    }
  }
}
```

This shape illustrates the minimum facts the initial policies need, not a shared wire contract. Active request IDs support idempotency and concurrency reconciliation; the last admission time survives completion for general throttling; and the cooldown deadline survives removal of the failed request. Policy keys and values are namespaced inside the implementation's versioned envelope. Unrelated controllers never mutate individual keys, and independently selected extensions never share a metadata map.

Storing the envelope on the queue gives admission one optimistic-lock boundary with the queue's latest-head pointer and other coordination fields. The gate loads the complete queue snapshot, changes only its owned field, computes `newVersion = oldVersion + 1`, performs the conditional write, and assigns the new version only after success. On `ErrVersionMismatch`, it reloads and restarts evaluation. It preserves concurrent changes to fields it does not own by always rebuilding from the reloaded snapshot.

No cross-entity transaction is introduced. Recording an admission and transitioning its request to `processing` remain two convergent versioned writes. The request ID is the idempotency key joining them.

## Independent Reconciliation

The state retains the IDs of active requests. Before evaluating a new admission, the gate reconciles that bounded set against authoritative request storage:

- `accepted` or `processing` remains active.
- A terminal request is removed from the active set.
- Its terminal request-log record supplies the outcome reason and occurrence time needed by outcome-sensitive policies.
- If the request is terminal but its corresponding log is not visible yet, evaluation defers. The lifecycle writer will retry the log write, and a later gate evaluation converges.

The lookup cost is bounded by the configured concurrency limit rather than queue history size. It uses primary-key request reads and request-owned log reads; it requires no query by status or new secondary index.

Using request history is what lets failure cooldown mean "after the runner-reported failure" rather than "after some later admission attempt noticed a failure." The initial cooldown policy reacts only to `RequestOutcomeReasonBuildFailed`. Success, cancellation, superseding, and failures synthesized by a DLQ or timeout do not activate it unless a later policy explicitly chooses those reasons.

This reconciliation replaces the current shared-counter ownership in which `process` increments `Queue.InFlightCount` and `buildsignal` or a DLQ path decrements it. Under the proposal, `buildsignal` remains policy-neutral: it persists the build result, request terminal state, and request log, but neither understands the gate envelope nor invokes an admission callback. The next `TryAdmit` observes those durable facts and removes completed reservations.

## Policy Composition

One resolved `Gate` is the atomic composition boundary for one queue and logical admission. The implementation may contain several policies, but they are not independently stateful extensions called in sequence.

For each `TryAdmit`, the implementation:

1. Loads and decodes one state snapshot.
2. Reconciles completed admissions into policy facts.
3. Evaluates every enabled policy against the same snapshot.
4. If any policy blocks, persists reconciliation changes if needed but records no new admission.
5. If all policies allow, applies every policy's admission mutation and the active-request reservation to one new snapshot and commits it with one queue CAS.

This prevents partial admission: an interval policy cannot consume its next slot only for a later budget policy to reject the request. Policy evaluation and proposed mutations may be separate internal primitives in the standard implementation, but they are deliberately absent from the public extension contract. That leaves alternative gate implementations free to use a remote quota service, a rules engine, or a single purpose-built algorithm without emulating an in-process policy interface.

The initial standard gate composes:

- **Concurrency:** preserve the current rule by deferring while the number of reconciled active admissions is at the per-queue limit.
- **Minimum interval:** add general throttling by requiring at least the configured number of milliseconds between admission timestamps. Non-positive values disable it.
- **Failure cooldown:** add outcome-sensitive throttling by deferring until a runner-reported failure's occurrence time plus the configured cooldown. Non-positive values disable it.

Future policies for build admission, such as a calendar window, cost budget, provider-health circuit, or manual hold, fit inside the same atomic composition. A policy that needs its own durable facts receives a namespaced section in the gate envelope. A fundamentally different backend or evaluation model is another `Gate` implementation selected by wiring.

## Configuration And Routing

Today `queueconfig` supplies `MaxConcurrent` and `GateWaitDelayMs`, and the service uses its built-in default implementation. The proposed minimum-interval and failure-cooldown settings belong in the same typed queue configuration contract; a deployment-backed configuration implementation is separate implementation work. Mutable observations such as admission times and failure facts belong in `AdmissionState`. Configuration is read during evaluation so a changed interval or cooldown affects the next attempt without rewriting stored state.

The common admission-gate contract does not define a universal policy configuration language. The standard gate understands Stovepipe's typed queue settings. Another gate may receive configuration through dependencies injected by its constructor. Per-queue selection belongs in service wiring through `Gates.For`, consistent with other plural resolver contracts; no implementation package contains a routing map.

This separation permits gradual evolution. Build admission can use the standard composite gate, a high-cost queue can use a remote budget gate, and a future promotion controller can receive a separately wired `Gates` resolver without changing the gate or result contracts.

## Observability

The controller records admitted and deferred counters; its operation name identifies the guarded boundary. Deferred counters may additionally use each `BlockedBy` identifier, whose low-cardinality contract makes it safe as a metric tag. Logs include request ID, queue, decision, and blockers.

The gate implementation records evaluation, state decode, reconciliation, CAS-conflict, and dependency errors. It must not place opaque state contents or arbitrary configuration values in metric tags.

An operator can inspect the initial JSON state in MySQL, but that is diagnostic only. No controller, API, or operational tool may depend on its internal keys without going through an implementation-owned decoder.

## Failure Posture

Admission gates fail closed. An error loading configuration, resolving storage, decoding state, reconciling outcomes, or committing an admission returns an error from `TryAdmit`; the controller does not advance the request. Normal consumer retry and DLQ behavior handles persistent infrastructure or configuration failures.

Unknown state versions also fail closed. Silently resetting an unreadable payload could over-admit work or discard a live cooldown. Rollouts that change the codec must support mixed-version readers and writers for the deployment window or migrate queues before switching implementations.

## Rollout

The implementation PR can migrate incrementally:

1. Add the opaque queue field and append the MySQL column, treating empty bytes as version-1 empty state.
2. Implement the standard composite gate with a concurrency policy matching the current `InFlightCount < MaxConcurrent` behavior.
3. Wire `process` through `Gates`, replace its direct counter claim, and make reconciliation authoritative instead of direct releases from `buildsignal` and DLQ paths.
4. Add minimum interval as a new standard policy for general admission throttling.
5. Add failure cooldown as another new standard policy.

During a rolling deployment, old and new processes must not concurrently own different representations of admission capacity. The wiring cutover therefore occurs only after every binary understands the new queue field, or behind a deployment-wide switch that keeps one ownership model active at a time.

## Rejected

- **A callback from every terminal path.** A `Complete` or `RecordOutcome` method makes correctness depend on buildsignal, cancellation, timeout, and DLQ paths all notifying the gate exactly once. Reconciliation from durable request facts is simpler and converges after missed work.
- **One extension call per policy.** Stateful gates called sequentially cannot atomically roll back earlier reservations when a later policy blocks. One gate owns composition and one CAS boundary.
- **Policy fields as queue columns.** Typed columns are easy for the first interval and cooldown but require schema and storage-contract changes for every new policy. An opaque, ownership-specific field keeps storage backend-neutral.
- **A generic queue metadata map.** It obscures ownership and invites unrelated controllers to mutate extension-defined keys. `admission_state` names one writer and one compatibility contract.
- **Sleeping until a policy deadline.** A long hold delays the next coalescing check. The normal short deferred wait keeps superseding responsive.
- **In-memory reservations.** They diverge across replicas and disappear on restart. Durable queue-scoped state plus optimistic locking is required for distributed admission.
- **Failing open on gate errors.** Admission exists to protect finite or costly resources; an unavailable policy dependency must not silently remove that protection.
