# SQSim

SQSim is a local scenario runner for building confidence in SubmitQueue behavior. It drives a real, isolated SubmitQueue stack with scripted external-system behavior, observes requests through the public Gateway APIs, and presents their progress in headless or interactive form.

This document captures the product shape and high-level design decisions.

## Problem

SubmitQueue is an eventually consistent, queue-driven distributed system. Unit tests establish controller behavior, and end-to-end tests establish selected workflows, but neither gives an operator or developer an easy way to submit several requests, watch their progress, and explore how the system responds to delayed, failed, or flaky external systems.

The first goal is interactive understanding. The second is correctness testing. The third is capacity and policy experimentation.

## Decisions

### SQSim composes the real local stack

Every run starts a fresh Docker Compose project containing the real Gateway, Orchestrator, Runway, message queues, and databases. SQSim submits ordinary `Land` requests and observes them through the public request-summary, `List`, and History APIs.

SQSim does not implement a second orchestrator, replace the queue, or introduce virtual time. Controller concurrency, retries, message visibility, eventual consistency, and wall-clock delays remain real.

```text
 Go scenario
      |
      v
  sqsim CLI -------- writes one immutable runtime profile
      |                              |
      | Land                         +------ mounted into Orchestrator
      v                              |
   Gateway                           +------ mounted into Runway
      |
      v
 real SubmitQueue pipeline
      |
      +------ List / request summary / History ------> sqsim CLI
```

The SQSim code is a composition layer under `sqsim/` plus a command under `tool/sqsim/`. Production services select SQSim adapters only when an explicit local configuration path is present.

### Scenarios are Go programs

A scenario is immutable typed data constructed through a fluent Go API. It declares one or more Land requests, when each request is submitted, the external-system behavior associated with it, and the expected terminal request status.

Concrete scenarios live under `sqsim/scenarios/` and are registered under human-chosen names. The CLI does not generate names or load checked-in JSON scenario files.

The CLI compiles the selected scenario into one temporary JSON profile before starting the stack. The profile is an implementation artifact shared read-only with Orchestrator and Runway and is removed during teardown.

### Behaviors describe external systems

Each Land carries typed behavior for three operations:

- Build Runner
- merge-conflict check
- merge

Behavior values are data, not implementations of production extension interfaces. SQSim adapters implement `buildrunner.BuildRunner` and `merger.Merger`, load the compiled profile, and execute the declared behavior.

Behaviors can describe real elapsed time, terminal outcomes, and transient faults. The initial model supports retryable failures before a side effect, response loss after a side effect, and subsequent recovery. SQSim never decides whether an operation should be retried. It returns the modeled result and lets the real consumer and controller retry policies decide what happens next.

### Change URIs correlate requests with behavior

The runner generates one URI per Land:

```text
sqsim://local/<scenario-name>/<land-name>
```

The URI flows through the normal request, batch, build, and Runway payloads. The SQSim adapters use it to resolve the corresponding behavior from the loaded profile.

Behavior is authored per Land. Until SubmitQueue creates multi-request batches, this aligns directly with Build Runner and Merger calls. If multi-request batching is introduced, SQSim must either define batch-level composition or require all members of a batch to declare compatible external behavior. It must not choose one member's behavior silently.

### Runtime state is ephemeral

Mutable model state is held in memory by the adapter process and keyed by the production operation identity, such as Build ID or `MergeRequest.ID`. A run starts with fresh containers and does not support service-restart recovery in the initial version.

The initial version does not persist run manifests, reports, model state, or model event directories.

### Observation uses public lifecycle APIs

SQSim projects Gateway request summaries and History events into request-stage progress. The observer must tolerate eventual consistency, repeated history entries, and requests moving faster than the polling interval.

The interactive view presents one row per Land and shows progress through validation, batching, scoring, speculation, building, and merging. The Build Runner stage displays an active spinner and elapsed time while a build is non-terminal.

Headless mode prints timestamped transitions, verifies expectations, and exits with distinct codes for success, expectation failure, and scenario or infrastructure failure.

### Every run is isolated

The runner uses a unique Docker Compose project name, fresh database volumes, and ephemeral host ports. It writes the runtime profile before service startup, waits for readiness, submits the scenario workload, captures logs on failure, and removes containers and volumes during teardown.

Large scenarios may submit hundreds or thousands of requests. They provide local scale confidence and policy experimentation, not a production-capacity benchmark.

## Consumer gates

Consumer gates can later add pause, resume, and deterministic interleaving at controller boundaries. They are complementary to SQSim but are not required for the first useful version. A gate is a barrier before controller execution, not virtual time or exact single-step execution.

## Non-goals

- Virtual time
- Exact one-controller-state ticking
- Replacing the real message queue or databases
- Persisted runtime model state
- Service-restart recovery during a scenario
- A production load-testing claim
- Automatic scenario-name generation
- A browser UI

## Success criteria

SQSim is successful when a developer can select a Go scenario, start one command, watch multiple requests traverse a fresh real stack, model delayed and flaky Build Runner and merge behavior, and receive a deterministic pass or failure result based on the public request lifecycle.
