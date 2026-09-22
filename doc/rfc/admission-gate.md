# Controller Admission Gates

## Scope

An **admission gate** decides whether a domain entity may cross a queue-scoped logical pipeline boundary. The shared contract is typed to that entity; each use supplies its own policies, dependencies, and any storage it needs. Gates may enforce concurrency, rate limits, cooldowns, maintenance windows, resource budgets, provider health, operator holds, or other admission policies.

This differs from the shared [Consumer Gate](consumer-gate.md). A consumer gate is an external operational control checked before a controller receives a delivery. An admission gate is domain policy evaluated inside a controller. Both may defer work through redelivery, but they answer different questions and own different state.

## Contract

The vendor-neutral contract lives at `platform/extension/admissiongate`:

```go
type Blocker string

type Result struct {
    Decision  Decision
    BlockedBy []Blocker
}

type Gate[T any] interface {
    TryAdmit(context.Context, T) (Result, error)
}

type Config struct {
    QueueName string
}

type Gates[T any] interface {
    For(Config) (Gate[T], error)
}
```

`Gate[T]` preserves domain identity without making `platform` depend on a domain. Each use binds `T` to its native stage entity, such as a request or batch. The platform package contains only this contract and its generated mocks. Implementations remain with their owning domains.

`Gates[T]` represents one logical boundary and resolves its composite gate for a queue. `For` follows existing resolver patterns: `Config` carries only `QueueName`, and service wiring selects and binds the implementation. It returns one gate rather than exposing the implementation's policy composition to the controller.

`TryAdmit` receives the owning domain's thin stage entity, following the repository's identity-in rule for decision extensions. Passing only an ID would discard identity already available to the controller and force the implementation to parse or reload it. The entity remains a reference: the implementation reloads mutable facts and resolves other dependencies internally.

`Result` expresses expected control flow:

- `DecisionAdmitted` means current policy permits the controller to continue admitting the candidate. The controller may still need to claim domain capacity before advancing it.
- `DecisionDeferred` means policy currently blocks the candidate. `BlockedBy` contains typed, low-cardinality policy identifiers for logs and metrics. Implementations define constants for the policies they report.
- `DecisionUnknown` follows the repository's enum convention as the invalid zero value, not a runtime outcome. The controller converts a nil-error result with this decision into an error, so an empty `Result` fails closed.

Errors are reserved for policy-evaluation failures. Deferral is not an error and does not consume retry budget.

There is no `Complete`, `Release`, or `RecordOutcome` method. `TryAdmit` evaluates authoritative facts through dependencies supplied to the implementation. Domain capacity and lifecycle transitions remain owned by their existing controllers.

## State And Storage

The shared contract does not prescribe or require storage. A gate may evaluate configuration and existing domain facts directly or delegate to a policy service. Dependencies are injected at construction; `Gate[T]` exposes no generic state API.

Policy state belongs to the dependency that makes the policy authoritative, not to the admission framework. A static policy may need no state. A policy that consumes a rate or budget must provide an atomic, candidate-idempotent decision so concurrent evaluations cannot spend the same capacity twice. Its concrete backend owns any schema or remote storage it needs.

The framework adds neither policy-specific columns nor an opaque metadata field to domain tables. A concrete policy may reuse existing domain state when that state already expresses the required fact, but the gate does not take ownership of that state.

## Reconciliation

Admission does not replace domain reconciliation. Controllers continue to own capacity claims, releases, and lifecycle facts. A gate reads those facts or delegates to a policy provider that maintains its own authoritative view.

Outcome-sensitive policies should derive decisions from durable request state and history when practical. The initial Stovepipe cooldown reacts only to `RequestOutcomeReasonBuildFailed`; success, cancellation, superseding, and DLQ- or timeout-synthesized failures do not activate it. If efficient evaluation requires derived state, the concrete policy owns that projection and its reconciliation.

## Policy Composition

One `Gate[T]` represents the composed policy decision for a queue and logical admission. Policy primitives remain implementation details, so an implementation may evaluate local rules, call a remote policy service, or use a purpose-built algorithm.

Pure policies may be evaluated together without coordination. Policies that consume capacity must expose an atomic composite operation or define compensation; the shared gate contract does not invent a transaction across independent policy backends. Any capacity-consuming operation must be idempotent for the candidate because queue delivery is at least once.

The initial Stovepipe admission flow combines:

- **Concurrency:** `process` retains the existing queue-counter check and CAS claim.
- **Minimum interval:** the gate requires the configured number of milliseconds between admissions; non-positive disables it.
- **Failure cooldown:** the gate defers until a runner-reported failure's occurrence time plus the configured cooldown; non-positive disables it.

Future Stovepipe policies remain behind the same gate. A fundamentally different backend or evaluation model is another `Gate[entity.Request]` selected by wiring.

## Configuration And Routing

Today `queueconfig` supplies `MaxConcurrent` and `GateWaitDelayMs` through its built-in default implementation. The proposed minimum interval and failure cooldown belong in the same typed queue configuration. Configuration changes affect the next evaluation; mutable observations remain in the domain or policy provider that owns them.

The shared contract defines no universal policy configuration language. Implementations receive configuration and other dependencies at construction. Per-queue routing belongs in service wiring through `Gates[T].For`, not in implementation packages.

## Observability

The controller records admitted and deferred counters; its operation name identifies the guarded boundary. Deferred counters may use `BlockedBy` as a low-cardinality dimension. Logs include candidate ID, queue, decision, and blockers.

The implementation records evaluation and dependency errors. Arbitrary configuration values and policy state must not appear in metric tags.

## Failure Posture

Admission fails closed. If configuration or policy evaluation fails, `TryAdmit` returns an error and the controller does not advance the candidate. Normal consumer retry and DLQ behavior handles persistent failures.

## Rejected Alternatives

- **Callbacks from terminal paths:** `Complete` or `RecordOutcome` would make domain lifecycle controllers depend on the gate. Policies that need outcomes consume authoritative domain facts or maintain their own projection.
- **One extension call per policy:** exposing policy composition to the controller couples choreography to policy configuration. One resolved gate returns the composed decision.
- **Gate-owned domain columns:** policy-specific fields or an opaque gate payload would couple replaceable policy implementations to shared domain storage.
- **Sleeping until a policy deadline:** long holds suppress coalescing. Short re-checks preserve responsive superseding.
- **Process-local policy state:** replicas diverge and restarts lose rate, budget, or cooldown facts. Stateful policies require an authoritative backend.
- **Failing open:** policy infrastructure failure must not silently remove protection for finite or costly resources.

## Example Use Case: Stovepipe Build Gating

### Use Case

Stovepipe build admission is the first use of the shared contract. Its `process` stage must decide whether the latest accepted request may reserve validation capacity and advance to `build`.

Stovepipe currently enforces only the queue's concurrency limit, directly in `process`. General throttling, failure cooldowns, and future policies need a common framework instead of more controller-specific branches. The immediate requirements are:

- Limit concurrent logical validations per queue.
- Throttle admissions per queue, for example to one start per hour.
- After the build runner reports a failure, defer the next admission for a configured cooldown.
- Continue coalescing while a request is deferred so a newer head can supersede it promptly.

On `main`, `process` loads the queue and `QueueConfig`, coalesces the request against `Queue.LatestRequestID`, and compares `Queue.InFlightCount` with `QueueConfig.MaxConcurrent`. A full queue causes `delivery.Hold(QueueConfig.GateWaitDelayMs)` followed by a successful return. `GateWaitDelayMs` is only the redelivery cadence; it is not a minimum interval between builds. Redelivery starts with loading and coalescing again, so a newer head can supersede the deferred request.

When capacity is available, `process` increments `Queue.InFlightCount` with a queue-version CAS. A version conflict reloads the queue and repeats coalescing before another claim. After a terminal runner result, `buildsignal` decrements the counter before making the request terminal. Relevant DLQ paths also decrement it. There is no admission timestamp, failure-cooldown state, or opaque admission payload today.

### Integration

`process` retains request choreography and build-slot ownership; the gate owns only the policy decision:

1. Load the request and queue, then coalesce against the latest request ID.
2. Resolve `Gate[entity.Request]` with `gates.For(admissiongate.Config{QueueName: request.Queue})`.
3. Call `TryAdmit(ctx, request)`.
4. On `DecisionDeferred`, hold for the normal gate re-check delay and return successfully.
5. On `DecisionAdmitted`, run the existing concurrency check, derive the build strategy, and CAS-increment `Queue.InFlightCount` to claim a build slot.
6. If the queue CAS conflicts, reload, coalesce, and reevaluate policy and capacity before retrying.
7. After claiming a slot, transition the request to `processing` and publish to `build` using the existing persist-before-publish ordering.

On redelivery, `process` restarts at loading and coalescing. The hold delay remains controller scheduling configuration rather than part of `Result`. Waiting until a policy's exact deadline would suppress coalescing for that entire period; short bounded re-checks keep superseding responsive.

The existing slot lifecycle remains unchanged. `process` compensates if its slot claim is not followed by the transition to `processing`; `buildsignal` releases the slot before recording a terminal build outcome; and request, build, and buildsignal DLQ reconciliation releases a slot when failing a `processing` request. The gate receives no lifecycle callbacks.

### Rollout

The Stovepipe integration can migrate incrementally:

1. Land the shared contract and a Stovepipe gate implementation.
2. Wire `process` through `Gates[entity.Request]` while preserving its existing concurrency claim and release paths.
3. Add minimum interval as a general-throttling policy.
4. Add failure cooldown as an outcome-sensitive policy.

The framework requires no queue-schema migration. A stateful policy introduces and owns its backend only when that policy is implemented; the admission-gate contract does not prescribe one.
