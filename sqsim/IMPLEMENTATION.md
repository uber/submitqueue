# SQSim implementation design

This document defines the core SQSim entities, their invariants, the fluent authoring API, and representative scenarios. The product-level decisions live in [the SQSim RFC](../doc/rfc/submitqueue/sqsim.md).

## Package shape

```text
sqsim/
├── IMPLEMENTATION.md
├── scenario.go
├── scenario_test.go
├── behavior.go
├── behavior_test.go
├── land.go
├── land_test.go
├── validation.go
├── validation_test.go
├── entity/
│   ├── scenario.go
│   ├── scenario_test.go
│   ├── behavior.go
│   └── behavior_test.go
├── scenarios/
│   ├── registry.go
│   ├── registry_test.go
│   ├── happy_path.go
│   ├── happy_path_test.go
│   └── ...
├── model/
├── adapter/
│   ├── buildrunner/
│   └── merger/
├── runner/
├── observer/
├── verifier/
└── tui/
```

`sqsim/entity` owns immutable data. The root `sqsim` package owns builders, validation, convenience constructors, and aliases that let scenario authors use `sqsim.Scenario` rather than import both packages.

`sqsim/scenarios` owns the concrete scenario catalog. It imports the root `sqsim` package and contains an explicit registry from public scenario name to builder function. It does not use package initialization for registration.

Every Go source file has a sibling `_test.go` where it contains behavior worth testing. Tests cover one logical path rather than mirroring every field or constructor.

## Scenario

`Scenario` is the complete immutable workload submitted to one fresh SubmitQueue stack.

```go
type Scenario struct {
	// TimeoutMs is the maximum wall-clock duration of the run.
	TimeoutMs int64 `json:"timeout_ms"`
	// Lands are the requests submitted by the run in declaration order.
	Lands []Land `json:"lands"`
}
```

The scenario does not contain a schema version. The concrete Go scenario and the SQSim binary are built from the same repository revision, and the generated profile exists only for the duration of that run.

The scenario does not contain its public name. The registry entry supplies the name when a scenario is selected. This avoids repeating the same name in a filename, registry key, and builder call.

### Invariants

- `TimeoutMs` is positive.
- `Lands` is non-empty.
- Land names are unique within the scenario.
- Land declaration order is stable and is used as the tie-breaker when two requests have the same submission delay.
- Building a scenario returns a deep immutable value. Builders must copy slices and maps before returning.

## Land

`Land` describes one Gateway `Land` call and the behavior expected when its synthetic change reaches external-system stages.

```go
type Land struct {
	// Name identifies the Land within its scenario.
	Name string `json:"name"`
	// Queue is the SubmitQueue queue receiving the request.
	Queue string `json:"queue"`
	// SubmitAfterMs is the delay from run start before submission.
	SubmitAfterMs int64 `json:"submit_after_ms"`
	// Behavior describes the external systems encountered by this request.
	Behavior Behavior `json:"behavior"`
	// Expectation describes the public terminal outcome required by the run.
	Expectation Expectation `json:"expectation"`
}

type Expectation struct {
	// Status is the expected public terminal request status.
	Status ExpectedRequestStatus `json:"status"`
}

type ExpectedRequestStatus string

const (
	RequestLanded    ExpectedRequestStatus = "landed"
	RequestError     ExpectedRequestStatus = "error"
	RequestCancelled ExpectedRequestStatus = "cancelled"
)
```

The runner generates the change URI. Scenario authors do not supply it:

```text
sqsim://local/<scenario-name>/<land-name>
```

### Invariants

- `Name` is non-empty and safe as one URI path segment.
- `Queue` is non-empty.
- `SubmitAfterMs` is non-negative and less than `Scenario.TimeoutMs`.
- All three external-system behaviors are present.
- The expected request status is terminal.

## Behavior

`Behavior` groups the three external operations that a request can encounter.

```go
type Behavior struct {
	// BuildRunner describes build triggering and status polling.
	BuildRunner BuildRunnerBehavior `json:"build_runner"`
	// MergeConflictCheck describes Runway dry-run mergeability checks.
	MergeConflictCheck MergeConflictCheckBehavior `json:"merge_conflict_check"`
	// Merge describes Runway committing merge calls.
	Merge MergeBehavior `json:"merge"`
}
```

Behavior values are embedded directly in each Land. They are not registered or referenced by a string. A local Go variable can be reused when several Lands need the same immutable behavior.

Behavior values do not implement production extension interfaces. They are serializable model input. Adapter packages implement the extension interfaces and interpret these values.

## Invocation and fault model

An invocation describes one call made by SubmitQueue to an external system.

```go
type Invocation[T any] struct {
	// DelayMs is synchronous provider latency before the call returns.
	DelayMs int64 `json:"delay_ms"`
	// Outcome is the result applied by the external system.
	Outcome T `json:"outcome"`
	// Fault optionally changes how the result is returned to the caller.
	Fault Fault `json:"fault"`
}

type Fault struct {
	// Kind identifies the error classification returned to the caller.
	Kind FaultKind `json:"kind"`
	// Phase states whether the outcome was applied before the error.
	Phase FaultPhase `json:"phase"`
}

type FaultKind string

const (
	FaultNone         FaultKind = ""
	FaultRetryable    FaultKind = "retryable"
	FaultNonRetryable FaultKind = "non_retryable"
)

type FaultPhase string

const (
	FaultBeforeSideEffect FaultPhase = "before_side_effect"
	FaultAfterSideEffect  FaultPhase = "after_side_effect"
)
```

`Fault{}` means no fault.

### Invocation semantics

- No fault: wait for `DelayMs`, apply the outcome, and return it.
- Before-side-effect fault: wait for `DelayMs`, do not apply the outcome, and return the modeled error.
- After-side-effect fault: wait for `DelayMs`, apply and retain the outcome, then return the modeled error.
- A retry of an operation whose after-side-effect outcome was retained returns the retained result without applying the side effect again.
- A before-side-effect fault consumes its invocation. A later call advances to the next invocation.
- SQSim only returns results. The real consumer and controller decide whether and when another call occurs.

The model uses real wall time. Tests use an injected clock and wait mechanism rather than sleeping.

## Build Runner behavior

```go
type BuildRunnerBehavior struct {
	// Triggers are consumed by logical Trigger operations in declaration order.
	Triggers []Invocation[BuildTriggerOutcome] `json:"triggers"`
}

type BuildTriggerOutcome struct {
	// Build describes the external build created by a successful side effect.
	Build BuildExecution `json:"build"`
}

type BuildExecution struct {
	// Timeline describes status as elapsed wall time from build creation.
	Timeline []BuildStatusPoint `json:"timeline"`
	// StatusFaults inject errors into selected Status calls.
	StatusFaults []FaultOnCall `json:"status_faults"`
}

type BuildStatusPoint struct {
	// AfterMs is elapsed wall time since build creation.
	AfterMs int64 `json:"after_ms"`
	// Status is the status visible at and after AfterMs.
	Status BuildStatus `json:"status"`
}

type FaultOnCall struct {
	// Call is the one-based invocation number.
	Call int `json:"call"`
	// Fault is the error returned for this invocation.
	Fault Fault `json:"fault"`
}
```

Build status uses repository nomenclature:

```go
type BuildStatus string

const (
	BuildAccepted  BuildStatus = "accepted"
	BuildRunning   BuildStatus = "running"
	BuildSucceeded BuildStatus = "succeeded"
	BuildFailed    BuildStatus = "failed"
	BuildCancelled BuildStatus = "cancelled"
)
```

### Build invariants

- At least one Trigger invocation is present.
- A successful Trigger outcome has a non-empty timeline.
- Timeline offsets begin at zero and increase strictly.
- The last timeline status is terminal.
- Status fault call numbers are positive and unique.
- Trigger models provider API behavior. Build duration belongs in the timeline rather than a long Trigger delay.
- `BuildFailed` is terminal. SQSim does not turn the same external build into a later success.

The BuildRunner adapter keys an uncertain Trigger side effect by the ordered base batch IDs plus head batch ID. A retry after response loss returns the same Build ID.

## Merge-conflict-check behavior

```go
type MergeConflictCheckBehavior struct {
	// Invocations are consumed by CheckMergeability calls in order.
	Invocations []Invocation[MergeConflictCheckOutcome] `json:"invocations"`
}

type MergeConflictCheckOutcome string

const (
	Mergeable     MergeConflictCheckOutcome = "mergeable"
	MergeConflict MergeConflictCheckOutcome = "conflict"
)
```

A conflict is an expected terminal business outcome. It is returned through `merger.ErrConflict` and is not a retryable infrastructure failure.

A retryable before-side-effect fault can recover on a later invocation. An after-side-effect fault is not useful for a dry-run check because the check has no durable external side effect, so validation rejects it.

## Merge behavior

```go
type MergeBehavior struct {
	// Invocations are consumed by committing Merge calls in order.
	Invocations []Invocation[MergeOutcome] `json:"invocations"`
}

type MergeOutcome struct {
	// Result selects the business result of the merge.
	Result MergeResult `json:"result"`
}

type MergeResult string

const (
	MergeSucceeded MergeResult = "succeeded"
)
```

The adapter synthesizes stable revision outputs for a successful merge. An after-side-effect fault records those outputs before returning an error. A retry with the same `MergeRequest.ID` returns the recorded outputs and never merges again.

Merge conflict belongs to `MergeConflictCheckBehavior`. A committing merge may return infrastructure faults but does not introduce a second conflict outcome in the initial model.

## Batch-level compatibility

Behavior is attached to a Land, while BuildRunner and Merger operate on batches. The current batch controller creates one batch per request, so one behavior resolves naturally.

Adapters must still validate their inputs:

- Every request in a Build Runner head batch must resolve to the same Build Runner behavior.
- Every step in a Runway request must resolve to the same merge-conflict-check and merge behavior.
- Incompatible behaviors return a clear non-retryable configuration error.

When multi-request batching becomes a supported SQSim use case, the model should gain an explicit batch-level behavior or deterministic behavior-composition rules. The adapter must not silently choose the first request's behavior.

## Registry

Concrete scenarios expose builder functions:

```go
type Builder func() (sqsim.Scenario, error)

var Registry = map[string]Builder{
	"happy-path":             HappyPath,
	"build-status-recovers":  BuildStatusRecovers,
	"merge-response-is-lost": MergeResponseIsLost,
}
```

Registry validation requires:

- A non-empty public name.
- A non-nil builder.
- A builder result that passes scenario validation.
- No duplicate registry key, enforced structurally by the map literal and covered by the registry test.

## Fluent API

Builders own temporary mutable construction state. `Build()` validates and returns immutable entity values.

```go
scenario, err := sqsim.NewScenario().
	Timeout(30 * time.Second).
	Land(
		sqsim.NewLand("l1").
			Queue("sqsim").
			Behavior(behavior).
			Expect(sqsim.RequestLanded),
	).
	Build()
```

`MustBuild()` is not part of the primary API. Concrete scenario functions return errors so `sqsim validate` can report mistakes without panicking.

Convenience constructors can express common outcomes without exposing raw entity literals:

```go
sqsim.SucceedImmediately()
sqsim.SucceedAfter(5 * time.Second)
sqsim.FailAfter(5 * time.Second)
sqsim.RetryableErrorBeforeSideEffect()
sqsim.RetryableErrorAfterSideEffect()
sqsim.SuccessfulMergeConflictCheck()
sqsim.ConflictingMergeConflictCheck()
sqsim.SuccessfulMerge()
```

## Example: delayed happy path

```go
func HappyPath() (sqsim.Scenario, error) {
	happy := sqsim.NewBehavior().
		BuildRunner(
			sqsim.NewBuildRunnerBehavior().
				Trigger(
					sqsim.BuildCreated(
						sqsim.StatusAt(0, sqsim.BuildAccepted),
						sqsim.StatusAt(500*time.Millisecond, sqsim.BuildRunning),
						sqsim.StatusAt(5*time.Second, sqsim.BuildSucceeded),
					),
				),
		).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.NewMergeBehavior().
			Invoke(sqsim.MergeSucceededAfter(500 * time.Millisecond)))

	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(happy).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
```

The `happy` variable is the behavior value. It has no behavior-name string and can be passed to multiple Lands.

## Example: Status error followed by recovery

```go
func BuildStatusRecovers() (sqsim.Scenario, error) {
	buildRecovers := sqsim.NewBehavior().
		BuildRunner(
			sqsim.NewBuildRunnerBehavior().
				Trigger(
					sqsim.BuildCreated(
						sqsim.StatusAt(0, sqsim.BuildAccepted),
						sqsim.StatusAt(time.Second, sqsim.BuildRunning),
						sqsim.StatusAt(5*time.Second, sqsim.BuildSucceeded),
					),
				).
				StatusFaultOnCall(
					2,
					sqsim.RetryableErrorBeforeSideEffect(),
				),
		).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(buildRecovers).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
```

The second `Status` call returns a retryable error. SQ decides whether to redeliver the build-signal message. A later successful call derives status from the original build timeline.

## Example: Trigger fails before side effect, then succeeds

```go
func BuildTriggerRecovers() (sqsim.Scenario, error) {
	buildRecovers := sqsim.NewBehavior().
		BuildRunner(
			sqsim.NewBuildRunnerBehavior().
				Trigger(
					sqsim.BuildTriggerFault(
						sqsim.RetryableErrorBeforeSideEffect(),
					),
					sqsim.BuildCreated(
						sqsim.StatusAt(0, sqsim.BuildAccepted),
						sqsim.StatusAt(3*time.Second, sqsim.BuildSucceeded),
					),
				),
		).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(buildRecovers).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
```

The first Trigger invocation has no side effect. If SQ retries, the next invocation creates the build.

## Example: merge response is lost

```go
func MergeResponseIsLost() (sqsim.Scenario, error) {
	mergeResponseLost := sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(
			sqsim.NewMergeBehavior().
				Invoke(
					sqsim.MergeSucceededAfter(500*time.Millisecond).
						Fault(sqsim.RetryableErrorAfterSideEffect()),
				),
		)

	return sqsim.NewScenario().
		Timeout(30 * time.Second).
		Land(
			sqsim.NewLand("l1").
				Queue("sqsim").
				Behavior(mergeResponseLost).
				Expect(sqsim.RequestLanded),
		).
		Build()
}
```

The first Merge call applies the merge and records its outputs, then returns a retryable error. A retry for the same `MergeRequest.ID` receives the recorded outputs.

## Example: concurrent mixed outcomes

```go
func MixedConcurrentRequests() (sqsim.Scenario, error) {
	happy := sqsim.NewBehavior().
		BuildRunner(sqsim.BuildSucceededAfter(3 * time.Second)).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	buildFails := sqsim.NewBehavior().
		BuildRunner(sqsim.BuildFailedAfter(2 * time.Second)).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	conflicts := sqsim.NewBehavior().
		BuildRunner(sqsim.SuccessfulBuildRunner()).
		MergeConflictCheck(sqsim.ConflictingMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

	return sqsim.NewScenario().
		Timeout(time.Minute).
		Land(
			sqsim.NewLand("fast-success").
				Queue("sqsim").
				Behavior(happy).
				Expect(sqsim.RequestLanded),
			sqsim.NewLand("build-failure").
				Queue("sqsim").
				Behavior(buildFails).
				Expect(sqsim.RequestError),
			sqsim.NewLand("merge-conflict").
				Queue("sqsim").
				SubmitAfter(250*time.Millisecond).
				Behavior(conflicts).
				Expect(sqsim.RequestError),
		).
		Build()
}
```

Land submission delays are relative to one run start, not relative to the preceding Land. Requests with the same delay may be submitted concurrently, subject to the runner's bounded submission concurrency.

## Explicit initial limits

- One synthetic change URI per Land.
- One fresh stack per scenario run.
- No virtual time.
- No service restart during a run.
- No persisted runtime model state or model event stream.
- Verification targets public request outcomes. Adapter tests verify exact invocation count and side-effect idempotency.
- Large scenarios are authored with ordinary Go loops. The DSL does not add a separate load-generation language.
