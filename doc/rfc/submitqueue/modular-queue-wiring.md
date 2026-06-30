# Modular Queue Wiring

Design notes for making the orchestrator's per-queue extension wiring and topic-registry setup modular, data-driven, and reusable across deployers. Decisions and rationale only; the code changes land after this RFC is reviewed.

## Problem

The orchestrator's example `main.go` (`example/submitqueue/orchestrator/server/main.go`) is ~950 lines that mixes three distinct concerns:

1. **Infrastructure bootstrap** — DB connections, logger, metrics, gRPC server, signal handling (~200 lines of generic boilerplate, much of it duplicated between gateway and orchestrator).
2. **Queue topology / topic registry** — `newTopicRegistry` is a static list of 12+ pipeline stages, each with a primary subscription and a mirrored DLQ subscription, plus publish-only topics. Adding or removing a pipeline stage requires editing this function in lockstep with controller registration.
3. **Per-queue extension wiring** — `queueRegistry`, `newQueueRegistry`, and four thin `*Factory` adapter types. The only way to configure which scorer / analyzer / change-provider / build-runner a queue uses is to edit Go code in this file, recompile, and redeploy.

Adding a new queue today requires changes in **three places**: YAML config (`queues.yaml`), Go code (`newQueueRegistry`), and a recompile. Adding a new pipeline stage requires **two coordinated edits** (topic list + controller registration). This makes adoption harder for new integrators who want to deploy SubmitQueue with their own queues and extension profiles.

The [TODO on line 477](../../example/submitqueue/orchestrator/server/main.go) already flags the queue-registry pattern as a candidate for promotion into the domain layer, contingent on a trigger: a second consumer needing the same wiring, data-driven config, or lifecycle requirements.

## Principle

- **The wiring layer assembles; the domain layer provides reusable building blocks.** Today the domain layer owns controllers, entities, and extension interfaces — but the *composition* of extensions into per-queue profiles and the *registration* of controllers into consumers is copy-pasted into each deployer's main.go. These compositions are mechanical and identical across deployers; they belong in the domain.
- **Data-driven where practical, code-driven where necessary.** Queue *names* are already data-driven (YAML via `queueconfig.Store`). Queue *extension profiles* — which scorer, which conflict analyzer — should also be declarable as data. Custom extension *implementations* remain code (a deployer writes a new `scorer.Scorer` impl), but selecting among known implementations should not require a recompile.
- **No DI framework.** The wiring stays explicit Go code. This refactor reduces its volume, not its nature.

## Proposal

### 1. Promote queue-profile registry into `submitqueue/core/queueprofile`

Extract the `queueExtensions` struct (renamed `Profile`) and `queueRegistry` (renamed `Registry`) from the example into `submitqueue/core/queueprofile/`. This is the domain-internal analogue of `submitqueue/core/topickey` — infrastructure shared between the orchestrator and (potentially) future services, but private to the SubmitQueue domain.

```go
// submitqueue/core/queueprofile/profile.go
package queueprofile

// Profile is the full set of extension implementations for a single queue.
// Grouping per queue (rather than per extension) lets the wiring read as
// "for this queue, here are its scorer, analyzer, change provider, …"
// and lets a profile start from a baseline and override only what differs.
type Profile struct {
    // ChangeProvider resolves change metadata for land requests in this queue.
    ChangeProvider changeprovider.ChangeProvider

    // BuildRunner triggers and polls CI builds for batches in this queue.
    BuildRunner buildrunner.BuildRunner

    // Scorer computes success probability for batches in this queue.
    Scorer scorer.Scorer

    // Analyzer detects conflicts between batches in this queue.
    Analyzer conflict.Analyzer
}

// Registry maps a queue name to its Profile, falling back to a default
// for queues without an explicit entry. It is the single place that knows
// the queue topology; extension packages remain queue-agnostic.
type Registry struct { … }

func NewRegistry(def Profile, perQueue map[string]Profile) Registry
func (r Registry) Get(queue string) Profile
```

The four thin factory adapters (`changeProviderFactory`, `buildRunnerFactory`, `scorerFactory`, `analyzerFactory`) also move into this package as exported types. They are mechanical — `For(cfg) → registry.Get(cfg.QueueName).X` — and every deployer needs them identically.

**Why a new package instead of expanding `queueconfig`:** `queueconfig` is a resolution target (key/value store of queue names). The profile registry is a *consumer* of queue names that additionally bundles behavioral extension instances. Mixing them would give `queueconfig` a dependency on every extension interface, violating the "stores are resolution targets, not aggregators" principle from CLAUDE.md.

### 2. Extract topic-registry builder into `submitqueue/core/topicregistry`

Replace `newTopicRegistry` with a reusable builder that declaratively constructs primary + DLQ topic pairs from a slice of stage specs, plus publish-only topics.

```go
// submitqueue/core/topicregistry/builder.go
package topicregistry

// StageSpec declares one pipeline stage that needs a primary subscription
// and an auto-generated DLQ subscription.
type StageSpec struct {
    // Key is the consumer.TopicKey for this stage.
    Key consumer.TopicKey

    // Name is the wire topic name (e.g. "start", "batch").
    Name string

    // GroupSuffix is the consumer-group suffix (e.g. "orchestrator-start").
    GroupSuffix string
}

// BuildParams configures the topic registry builder.
type BuildParams struct {
    // Queue is the message queue backend.
    Queue extqueue.Queue

    // SubscriberName identifies this subscriber for partition leases.
    SubscriberName string

    // Stages are the primary pipeline stages. Each gets a paired DLQ
    // subscription with DLQ disabled (no _dlq_dlq cascade) and a high
    // MaxAttempts for convergent reconciliation.
    Stages []StageSpec

    // PublishOnly are topics the service publishes to but never consumes.
    PublishOnly []consumer.TopicConfig
}

func Build(p BuildParams) (consumer.TopicRegistry, error)
```

This eliminates the manual duplication of the primary/DLQ pairing pattern across 12 stages. Each stage is one `StageSpec` entry; the builder guarantees every primary stage gets a correctly-configured DLQ subscription (disabled DLQ-of-DLQ, high MaxAttempts) without copy-paste. Adding or removing a pipeline stage becomes adding or removing one line.

### 3. Extract controller-registration helpers into `submitqueue/orchestrator/controller/wire`

Create a `wire` subpackage under `submitqueue/orchestrator/controller/` with two functions:

```go
// submitqueue/orchestrator/controller/wire/wire.go
package wire

// PrimaryParams holds the dependencies needed to construct and register
// all primary pipeline controllers.
type PrimaryParams struct {
    Consumer          consumer.Consumer
    Logger            *zap.SugaredLogger
    Scope             tally.Scope
    Registry          consumer.TopicRegistry
    ChangeProviderF   changeprovider.Factory
    BuildRunnerF      buildrunner.Factory
    ScorerF           scorer.Factory
    ConflictF         conflict.Factory
    Counter           counter.Counter
    Store             storage.Storage
}

// RegisterPrimary creates and registers all primary pipeline controllers.
// Returns the count of registered controllers.
func RegisterPrimary(p PrimaryParams) (int, error)

// DLQParams holds the dependencies needed to construct and register
// all DLQ reconciliation controllers.
type DLQParams struct {
    Consumer consumer.Consumer
    Logger   *zap.SugaredLogger
    Scope    tally.Scope
    Store    storage.Storage
}

// RegisterDLQ creates and registers all DLQ reconciliation controllers.
// Returns the count of registered controllers.
func RegisterDLQ(p DLQParams) (int, error)
```

This keeps the controller list in the domain layer (testable, importable) and reduces the wiring main.go to: build dependencies → call `wire.RegisterPrimary` → call `wire.RegisterDLQ`. Adding a new pipeline stage becomes a single-file edit in this package.

### 4. Extend `QueueConfig` with optional profile hints

Add an optional `Profile` field to `entity.QueueConfig` so the YAML file can declare which scorer / conflict / build-runner strategy each queue uses:

```yaml
queues:
  - name: test-queue
    profile:
      scorer: heuristic
      conflict: file-overlap
  - name: e2e-test-queue
    profile:
      scorer: composite
      conflict: none
  - name: e2e-cancel-queue
    # No profile — inherits the baseline.
```

```go
// submitqueue/entity/queue_config.go
type QueueConfig struct {
    // Name uniquely identifies this queue within the system.
    Name string `json:"name" yaml:"name"`

    // Profile carries optional hints for which extension implementations
    // this queue uses. The wiring layer maps hint strings to concrete
    // extension instances; the entity does not import extension packages.
    // Zero value means "use the deployer's baseline profile."
    Profile QueueProfile `json:"profile,omitempty" yaml:"profile,omitempty"`
}

// QueueProfile carries string-typed hints for extension selection.
// Each field names a known implementation (e.g. "heuristic", "composite",
// "file-overlap", "none", "all"). Deployers register the mapping from
// hint → implementation in the wiring layer. An empty string means
// "inherit from the baseline."
type QueueProfile struct {
    // Scorer names the scoring strategy (e.g. "heuristic", "composite").
    Scorer string `json:"scorer,omitempty" yaml:"scorer,omitempty"`

    // Conflict names the conflict-analysis strategy (e.g. "all", "none", "file-overlap").
    Conflict string `json:"conflict,omitempty" yaml:"conflict,omitempty"`

    // BuildRunner names the build-runner backend (e.g. "fake", "jenkins").
    BuildRunner string `json:"build_runner,omitempty" yaml:"build_runner,omitempty"`

    // ChangeProvider names the change-provider backend (e.g. "github", "phabricator", "routing").
    ChangeProvider string `json:"change_provider,omitempty" yaml:"change_provider,omitempty"`
}
```

**Constraints:** `QueueConfig` stays a simple data carrier — it does NOT import extension packages. The mapping from hint string → extension instance remains in the wiring layer (`example/.../main.go` or a deployer's equivalent). This preserves the clean architecture boundary: entities are pure, factories are injected.

### 5. Refactor the example orchestrator main.go

Rewrite the example to compose the new packages:

1. Build infrastructure (DB, logger, metrics) — stays in main.go (deployment-specific).
2. Load queue configs from YAML — stays in main.go.
3. Build queue profiles using `queueprofile.NewRegistry(...)` — stays in main.go but is now ~30 lines instead of ~100, and can optionally be driven by profile hints from the YAML.
4. Build topic registry using `topicregistry.Build(...)` — one call, ~10 lines instead of ~90.
5. Register controllers using `wire.RegisterPrimary(...)` + `wire.RegisterDLQ(...)` — two calls instead of ~170 lines.
6. Start consumers and gRPC server — stays in main.go.

**Result:** main.go drops from ~950 to ~300 lines. The domain-layer packages are independently testable. A new integrator copies the example, edits `queues.yaml` (including optional profile hints), and optionally customizes the `queueprofile.Registry` population — no need to understand the full pipeline topology.

## What each extraction produces

| Extraction | New package | Key types | Lines removed from main.go |
|---|---|---|---|
| Queue profiles | `submitqueue/core/queueprofile` | `Profile`, `Registry`, `ChangeProviderFactory`, `BuildRunnerFactory`, `ScorerFactory`, `AnalyzerFactory` | ~100 (queueExtensions, queueRegistry, 4 factory types, newQueueRegistry) |
| Topic registry | `submitqueue/core/topicregistry` | `StageSpec`, `BuildParams`, `Build()` | ~90 (newTopicRegistry) |
| Controller wire | `submitqueue/orchestrator/controller/wire` | `PrimaryParams`, `DLQParams`, `RegisterPrimary()`, `RegisterDLQ()` | ~170 (registerPrimaryControllers, registerDLQControllers) |
| Profile hints | `submitqueue/entity` (extended) | `QueueProfile` (added to `QueueConfig`) | 0 (additive) |

## Rejected

- **DI framework (wire/dig/fx).** Adds indirection and a build-time dependency for a problem that explicit code solves. The refactor reduces the volume of explicit wiring, not its nature.
- **Hot-reload of queue configs.** Out of scope. The YAML is loaded at startup. Hot-reload can build on this foundation later — `queueconfig.Store` already abstracts the read path, so swapping the YAML impl for a watching impl is a future, independent change.
- **Changing the Factory interface contract.** The existing `Factory.For(Config)` pattern is sound and is the way controllers resolve per-queue extension instances. We add a first-class registry that factories resolve *against*, not a new factory contract.
- **Promoting `newChangeProvider` / `newGitHubChangeProvider` / `newPhabChangeProvider` out of the example.** These are deployment-specific (token sources, HTTP clients, timeouts). They stay in the wiring layer.
- **Merging `queueprofile` into `queueconfig`.** The config store is a resolution target (key/value); the profile registry aggregates behavioral instances. Mixing them gives `queueconfig` a dependency on every extension interface, violating the "stores are resolution targets" principle.
- **A generic "service bootstrap" package.** The duplicated boilerplate between gateway and orchestrator (logger, metrics, DB, gRPC server, signal handling) is real but is a separate, orthogonal concern. Folding it into this RFC would conflate infrastructure and domain — extract it separately if/when a third service lands.

## Triggers

Per the existing TODO, the extractions should land when any of these occur:

1. A second consumer needs the same wiring (a real production server, or an e2e harness building real per-queue profiles).
2. Per-queue config becomes data-driven (build profiles from `queueconfig.Store` / `queues.yaml` instead of Go literals) — step 4 of this proposal.
3. The bundle grows lifecycle (Close / health / hot-reload).

Steps 1–3 can land independently as mechanical extractions with zero behavioral change. Step 4 (profile hints) is additive and can follow once the structural extractions stabilize.
