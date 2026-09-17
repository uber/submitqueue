# Stovepipe Admission Gates

Status: proposed. This RFC defines the framework and extension contract; storage and controller integration land separately.

## Problem

Stovepipe currently makes its build-admission decisions directly in `process`: admit only below the queue's concurrency limit, and optionally delay starts by a minimum interval. A failure cooldown adds a third decision with the same shape, but implementing each rule in a controller spreads policy across lifecycle stages and makes every new rule another special case.

The immediate requirements are:

- Limit concurrent logical validations per queue.
- Throttle admissions generally, for example to at most one start per hour.
- After a build runner reports a failed result, defer the next admission for a configured cooldown.
- Keep coalescing active while a request is deferred, so a newer head can supersede it without waiting for the gate to open.

The framework must also leave room for policies such as maintenance windows, resource budgets, provider health, or an operator hold, and for logical admission points other than build admission.

## Scope

An **admission gate** decides whether one domain entity may cross a logical pipeline boundary. The first point is `build`: whether the latest accepted Stovepipe request may reserve validation capacity and advance toward the build stage.

This is separate from the shared [Consumer Gate](../consumer-gate.md). A consumer gate is an external operational control that stops deliveries before a controller. A Stovepipe admission gate is domain policy evaluated by a controller for a specific request. Both defer with queue redelivery, but they answer different questions and own different state.

## Contract

The vendor-neutral contract lives at `stovepipe/extension/admissiongate`:

```go
type Point string

const PointBuild Point = "build"

type Result struct {
    Decision  Decision
    BlockedBy []string
}

type Gate interface {
    TryAdmit(context.Context, entity.Request) (Result, error)
}

type Gates interface {
    For(Point, entity.Request) (Gate, error)
}
```

`Point` is an open string identifier. The shared package names only points understood by Stovepipe; adding a point is additive and does not change `Gate`.

`Gates` is the host-owned resolver across queues and points. `For` takes the request rather than a separate queue configuration because the request already carries its authoritative queue identity. It returns exactly one composite gate: returning a slice of independently stateful gates would make atomic admission impossible when one gate records a reservation before a later gate defers. Concrete routing belongs in service wiring, not an extension implementation package.

`TryAdmit` takes the thin `entity.Request`, following the repository's identity-in extension rule. The request already identifies its queue. A gate resolves the queue-scoped storage, configuration, request history, clocks, or remote services it needs through dependencies injected when its implementation is constructed. Controllers do not pre-resolve policy facts and hand them across the contract.

`Result` represents expected control flow:

- `DecisionAdmitted` means this request's admission was already recorded or has been durably recorded before the call returns.
- `DecisionDeferred` means no admission was recorded because one or more policies currently block it. `BlockedBy` contains stable, low-cardinality policy identifiers for logs and metrics, not human-readable errors.
- `DecisionUnknown` is the invalid zero value and must be treated as an implementation failure.

Errors are reserved for failures to evaluate or durably record the decision. A closed gate is not an error and does not consume retry budget.

There is intentionally no `Complete`, `Release`, or `RecordOutcome` method. `TryAdmit` must be able to derive the current answer from durable state. This keeps build outcomes owned by the request lifecycle and prevents `buildsignal`, DLQ controllers, or future terminal paths from each needing policy-specific callbacks.

## Process Integration

`process` retains responsibility for request choreography; the gate owns only admission policy and its reservation:

1. Load the request and queue, then coalesce it against the latest request ID.
2. Resolve the gate with `gates.For(PointBuild, request)`.
3. Call `TryAdmit(ctx, request)`.
4. On `DecisionDeferred`, hold the delivery for the queue's normal gate re-check delay and return successfully.
5. On redelivery, start again at coalescing before evaluating the gate.
6. On `DecisionAdmitted`, derive the build strategy, transition the request to `processing`, and publish to `build` using the existing persist-before-publish ordering.

The hold delay belongs to controller scheduling configuration, not `Result`. A policy may know an exact deadline, but sleeping until that deadline would suppress coalescing for its full duration. Frequent bounded re-checks preserve superseding and make all policies converge through the same path.

If the process dies after the gate records admission but before the request reaches `processing`, redelivery calls `TryAdmit` with the same request ID. The result is admitted without reserving twice, and the controller retries the transition. If the process message ultimately reaches its DLQ, the DLQ's terminal request transition becomes visible to later reconciliation.

## Durable State

The first implementation adds `AdmissionState []byte` to `entity.Queue` and appends an `admission_state BLOB` column to the end of the MySQL queue schema. `QueueStore` only round-trips those bytes as part of the existing versioned queue snapshot; it does not parse, validate, merge, or version the payload.

The concrete gate exclusively owns the payload's encoding and compatibility. The initial implementation uses versioned JSON because the state is small and operationally inspectable, but the storage contract is opaque bytes rather than a JSON contract. A different implementation may use protobuf or another encoding. Changing the implementation for a live queue requires that the replacement understand or explicitly migrate the prior payload.

A representative initial payload is:

```json
{
  "version": 1,
  "last_admitted_request_id": "request/monorepo/main/42",
  "active": {
    "request/monorepo/main/42": {
      "admitted_at_ms": 1789506000000
    }
  },
  "policies": {
    "minimum_interval": {
      "last_admitted_at_ms": 1789506000000
    },
    "failure_cooldown": {
      "not_before_ms": 1789509600000,
      "source_request_id": "request/monorepo/main/41"
    }
  }
}
```

This shape illustrates ownership, not a shared wire contract. Policy keys and values are namespaced inside the implementation's versioned envelope. Unrelated controllers never mutate individual keys, and independently selected extensions never share a metadata map.

Storing the envelope on the queue gives admission one optimistic-lock boundary with the queue's latest-head pointer and other coordination fields. The gate loads the complete queue snapshot, changes only its owned field, computes `newVersion = oldVersion + 1`, performs the conditional write, and assigns the new version only after success. On `ErrVersionMismatch`, it reloads and restarts evaluation. It preserves concurrent changes to fields it does not own by always rebuilding from the reloaded snapshot.

No cross-entity transaction is introduced. Recording an admission and transitioning its request to `processing` remain two convergent versioned writes. The request ID is the idempotency key joining them.

## Independent Reconciliation

The state retains the IDs and admission times of active requests. Before evaluating a new admission, the gate reconciles that bounded set against authoritative request storage:

- `accepted` or `processing` remains active.
- A terminal request is removed from the active set.
- Its terminal request-log record supplies the outcome reason and occurrence time needed by outcome-sensitive policies.
- If the request is terminal but its corresponding log is not visible yet, evaluation defers. The lifecycle writer will retry the log write, and a later gate evaluation converges.

The lookup cost is bounded by the configured concurrency limit rather than queue history size. It uses primary-key request reads and request-owned log reads; it requires no query by status or new secondary index.

Using request history is what lets failure cooldown mean "after the runner-reported failure" rather than "after some later admission attempt noticed a failure." The initial cooldown policy reacts only to `RequestOutcomeReasonBuildFailed`. Success, cancellation, superseding, and failures synthesized by a DLQ or timeout do not activate it unless a later policy explicitly chooses those reasons.

`buildsignal` therefore remains policy-neutral. It persists the build result, request terminal state, and request log. It neither understands the gate envelope nor invokes an admission callback.

## Policy Composition

One resolved `Gate` is the atomic composition boundary for one queue and admission point. The implementation may contain several policies, but they are not independently stateful extensions called in sequence.

For each `TryAdmit`, the implementation:

1. Loads and decodes one state snapshot.
2. Reconciles completed admissions into policy facts.
3. Evaluates every enabled policy against the same snapshot.
4. If any policy blocks, persists reconciliation changes if needed but records no new admission.
5. If all policies allow, applies every policy's admission mutation and the active-request reservation to one new snapshot and commits it with one queue CAS.

This prevents partial admission: an interval policy cannot consume its next slot only for a later budget policy to reject the request. Policy evaluation and proposed mutations may be separate internal primitives in the standard implementation, but they are deliberately absent from the public extension contract. That leaves alternative gate implementations free to use a remote quota service, a rules engine, or a single purpose-built algorithm without emulating an in-process policy interface.

The initial standard gate composes:

- **Concurrency:** defer while the number of reconciled active admissions is at the per-queue limit.
- **Minimum interval:** when configured above zero, require at least that many milliseconds between admission timestamps. Non-positive values disable it.
- **Failure cooldown:** after a configured runner-reported failure, defer until the failure occurrence time plus the cooldown. Non-positive values disable it.

Future policies at the same point, such as a calendar window, cost budget, provider-health circuit, or manual hold, fit inside the same atomic composition. A policy that needs its own durable facts receives a namespaced section in the gate envelope. A fundamentally different backend or evaluation model is another `Gate` implementation selected by wiring.

## Configuration And Routing

Queue policy settings remain deployment configuration supplied through `queueconfig`; mutable observations remain in `AdmissionState`. Configuration is read during evaluation so a changed interval or cooldown affects the next attempt without rewriting stored state.

The common admission-gate contract does not define a universal policy configuration language. The standard gate understands Stovepipe's typed queue settings. Another gate may receive configuration through dependencies injected by its constructor. Per-queue and per-point selection belongs in service wiring through `Gates.For`, consistent with other plural resolver contracts; no implementation package contains a routing map.

This separation permits gradual evolution. The current build point can use the standard composite gate, a high-cost queue can use a remote budget gate, and a future promotion point can use an independently selected gate without changing controllers' result handling.

## Observability

The controller records admitted and deferred counters tagged by admission point. Deferred counters may additionally use each `BlockedBy` identifier, whose low-cardinality contract makes it safe as a metric tag. Logs include request ID, queue, point, decision, and blockers.

The gate implementation records evaluation, state decode, reconciliation, CAS-conflict, and dependency errors. It must not place opaque state contents or arbitrary configuration values in metric tags.

An operator can inspect the initial JSON state in MySQL, but that is diagnostic only. No controller, API, or operational tool may depend on its internal keys without going through an implementation-owned decoder.

## Failure Posture

Admission gates fail closed. An error loading configuration, resolving storage, decoding state, reconciling outcomes, or committing an admission returns an error from `TryAdmit`; the controller does not advance the request. Normal consumer retry and DLQ behavior handles persistent infrastructure or configuration failures.

Unknown state versions also fail closed. Silently resetting an unreadable payload could over-admit work or discard a live cooldown. Rollouts that change the codec must support mixed-version readers and writers for the deployment window or migrate queues before switching implementations.

## Rollout

The implementation PR can migrate incrementally:

1. Add the opaque queue field and append the MySQL column, treating empty bytes as version-1 empty state.
2. Implement the standard composite gate with concurrency and minimum-interval policies matching current behavior.
3. Wire `process` through `Gates` and remove its direct admission-counter/deadline decisions.
4. Stop `buildsignal` from directly releasing admission capacity; reconciliation becomes authoritative.
5. Add failure cooldown as another standard policy.

During a rolling deployment, old and new processes must not concurrently own different representations of admission capacity. The wiring cutover therefore occurs only after every binary understands the new queue field, or behind a deployment-wide switch that keeps one ownership model active at a time.

## Rejected

- **A callback from every terminal path.** A `Complete` or `RecordOutcome` method makes correctness depend on buildsignal, cancellation, timeout, and DLQ paths all notifying the gate exactly once. Reconciliation from durable request facts is simpler and converges after missed work.
- **One extension call per policy.** Stateful gates called sequentially cannot atomically roll back earlier reservations when a later policy blocks. One gate owns composition and one CAS boundary.
- **Policy fields as queue columns.** Typed columns are easy for the first interval and cooldown but require schema and storage-contract changes for every new policy. An opaque, ownership-specific field keeps storage backend-neutral.
- **A generic queue metadata map.** It obscures ownership and invites unrelated controllers to mutate extension-defined keys. `admission_state` names one writer and one compatibility contract.
- **Sleeping until a policy deadline.** A long hold delays the next coalescing check. The normal short deferred wait keeps superseding responsive.
- **In-memory reservations.** They diverge across replicas and disappear on restart. Durable queue-scoped state plus optimistic locking is required for distributed admission.
- **Failing open on gate errors.** Admission exists to protect finite or costly resources; an unavailable policy dependency must not silently remove that protection.
