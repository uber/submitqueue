# SQSim

SQSim is a local scenario runner for exploring and verifying SubmitQueue behavior. It submits ordinary Land requests to a fresh real SubmitQueue stack, models Build Runner and merge-system behavior, and shows each request moving through the pipeline.

SQSim uses real Docker containers, databases, queues, controllers, retries, and wall-clock time. It does not replace SubmitQueue with a second implementation or use virtual time.

## Quick start

Run these commands from the repository root. Docker must be running.

```bash
docker info >/dev/null

./tool/bazel run //tool/sqsim:sqsim -- list
./tool/bazel run //tool/sqsim:sqsim -- validate happy-path
./tool/bazel run //tool/sqsim:sqsim -- run happy-path
```

The last command opens the interactive terminal UI. After the scenario displays `PASS`, press `q` to exit.

Each run:

1. Builds the local Gateway, Orchestrator, and Runway binaries.
2. Creates a uniquely named Docker Compose project.
3. Starts fresh queue and application databases.
4. Applies the database schemas.
5. Submits the scenario's Land requests.
6. Observes progress through the public List, request-summary, and History APIs.
7. Verifies the expected terminal result.
8. Removes the containers, images, volumes, and temporary scenario profile.

The first run is slower because it must build binaries and Docker images.

## Interactive terminal UI

The TUI shows one row per Land request:

```text
REQUEST          VAL  BAT  SCO  SPE  BLD  MRG  STATUS             BUILD
l1                ok   ok   ok   ok    /    .  building             3.2s
```

The columns represent validation, batching, scoring, speculation, building, and merging. An active Build Runner invocation displays a spinner and elapsed time.

Controls:

| Key | Action |
| --- | --- |
| `j` / `k` or arrow keys | Select a request |
| `PgUp` / `PgDn` | Scroll request history |
| `Enter` | Show or hide details |
| `q` | Stop a running scenario or exit a completed scenario |

Try scenarios that make retries and concurrent outcomes visible:

```bash
./tool/bazel run //tool/sqsim:sqsim -- run build-status-recovery
./tool/bazel run //tool/sqsim:sqsim -- run merge-response-lost
./tool/bazel run //tool/sqsim:sqsim -- run mixed-concurrent
```

## Headless mode

Headless mode prints timestamped transitions and a final result for every Land:

```bash
./tool/bazel run //tool/sqsim:sqsim -- run mixed-concurrent --headless
```

Example:

```text
[1.6s] lands            building           sqsim/1
[9.2s] lands            landed             sqsim/1
PASS lands: got landed, expected landed
```

The command uses these exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Every request matched its expectation |
| `1` | The scenario ran, but at least one expectation failed |
| `2` | The command, scenario, or local infrastructure failed |

Use `--poll-interval` to change how often SQSim reads the public lifecycle APIs:

```bash
./tool/bazel run //tool/sqsim:sqsim -- run happy-path --headless --poll-interval=500ms
```

## Included scenarios

Use `sqsim list` to get the authoritative catalog.

| Scenario | Behavior |
| --- | --- |
| `happy-path` | A delayed build and merge both succeed |
| `build-failure` | The Build Runner reports terminal failure |
| `build-status-recovery` | The first build-status call fails transiently, then polling recovers |
| `build-trigger-recovery` | The first build-trigger call fails before its side effect, then a retry succeeds |
| `merge-conflict` | The merge-conflict check reports a conflict |
| `merge-conflict-check-recovery` | The first merge-conflict check fails transiently, then a retry succeeds |
| `merge-response-lost` | Merge succeeds, its response is lost, and a retry returns the retained result without merging twice |
| `mixed-concurrent` | Three staggered requests land, conflict, or fail their build |
| `load-1000` | One thousand successful requests exercise the local stack under load |

`load-1000` is intentionally resource intensive. It provides local functional and scale confidence, not a production-capacity benchmark.

## Authoring a scenario

Scenarios are Go functions under `sqsim/scenarios/`. A scenario combines:

- One or more Land requests
- A Build Runner behavior
- A merge-conflict-check behavior
- A committing merge behavior
- An expected terminal request status

Behavior values are passed directly to Lands. They are not looked up through behavior-name strings.

```go
package scenarios

import (
	"time"

	"github.com/uber/submitqueue/sqsim"
)

func SlowHappyPath() (sqsim.Scenario, error) {
	happy := sqsim.NewBehavior().
		BuildRunner(sqsim.NewBuildRunnerBehavior().
			Trigger(sqsim.BuildCreated(
				sqsim.StatusAt(0, sqsim.BuildAccepted),
				sqsim.StatusAt(time.Second, sqsim.BuildRunning),
				sqsim.StatusAt(5*time.Second, sqsim.BuildSucceeded),
			))).
		MergeConflictCheck(sqsim.SuccessfulMergeConflictCheck()).
		Merge(sqsim.SuccessfulMerge())

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

To add the scenario to the CLI:

1. Add its builder to `sqsim/scenarios/`.
2. Add a sibling `_test.go` file that verifies its intended outcome.
3. Register its public name in `sqsim/scenarios/registry.go`.
4. Run the scenario-package tests.
5. Validate and run it through the CLI.

```bash
./tool/bazel test //sqsim/scenarios:go_default_test --test_output=errors
./tool/bazel run //tool/sqsim:sqsim -- validate example
./tool/bazel run //tool/sqsim:sqsim -- run example
```

## Modeling failures and recovery

The DSL can model elapsed provider time, terminal outcomes, and faults across repeated invocations. Common building blocks include:

```go
sqsim.BuildSucceededAfter(5 * time.Second)
sqsim.BuildFailedAfter(5 * time.Second)
sqsim.RetryableErrorBeforeSideEffect()
sqsim.RetryableErrorAfterSideEffect()
sqsim.ConflictingMergeConflictCheck()
```

A before-side-effect fault consumes the invocation without applying its outcome. A later call advances to the next invocation.

An after-side-effect fault retains the outcome before returning an error. A retry of the same production operation returns the retained result and does not repeat the side effect.

SQSim returns the modeled result or error. The real SubmitQueue controllers and consumer error classification decide whether the operation is retried.

See [the implementation design](IMPLEMENTATION.md) for the complete entity model, invariants, and fluent API examples.

## Troubleshooting

### Docker is unavailable

Confirm Docker Desktop or the Docker daemon is running:

```bash
docker info
```

### A TUI run fails without enough detail

Rerun the same scenario in headless mode. Headless mode prints build progress and emits captured service logs if the run fails:

```bash
./tool/bazel run //tool/sqsim:sqsim -- run happy-path --headless
```

### Check for resources left after an interrupted process

Normal completion and cancellation remove the stack automatically. To inspect for an interrupted SQSim project:

```bash
docker ps -a --format '{{.Names}}' | grep '^sqsim-' || true
docker volume ls --format '{{.Name}}' | grep '^sqsim-' || true
```

## Design documentation

- [SQSim RFC](../doc/rfc/submitqueue/sqsim.md)
- [SQSim implementation design](IMPLEMENTATION.md)
