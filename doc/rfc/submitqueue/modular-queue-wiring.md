# Modular Queue Wiring

Design notes for making the orchestrator's per-queue extension wiring and topic-registry setup modular, reusable, and importable by external deployers. Decisions and rationale only; the code changes land after this RFC is reviewed.

## Problem

The orchestrator's example `main.go` (`example/submitqueue/orchestrator/server/main.go`) is ~950 lines that mixes three distinct concerns:

1. **Infrastructure bootstrap** — DB connections, logger, metrics, gRPC server, signal handling (~200 lines of generic boilerplate, much of it duplicated between gateway and orchestrator).
2. **Queue topology / topic registry** — `newTopicRegistry` is a static list of 12+ pipeline stages, each with a primary subscription and a mirrored DLQ subscription, plus publish-only topics. Adding or removing a pipeline stage requires editing this function in lockstep with controller registration.
3. **Per-queue extension wiring** — `queueRegistry`, `newQueueRegistry`, and four thin `*Factory` adapter types. The only way to configure which scorer / analyzer / change-provider / build-runner a queue uses is to edit Go code in this file, recompile, and redeploy.

Adding a new queue today requires changes in **three places**: YAML config (`queues.yaml`), Go code (`newQueueRegistry`), and a recompile. Adding a new pipeline stage requires **two coordinated edits** (topic list + controller registration). The topic → subscription → DLQ subscription → DLQ controller linkage is maintained by copy-paste across 12 stages, where forgetting any half creates a silent failure.

The [TODO on line 477](../../example/submitqueue/orchestrator/server/main.go) already flags the queue-registry pattern as a candidate for promotion into the domain layer, contingent on a trigger: a second consumer needing the same wiring, data-driven config, or lifecycle requirements.

## Vocabulary

```
seam     an extension interface the library defines and the deployer fills
         (storage, buildrunner, sourcecontrol, …) — always an interface, never an impl
stage    one pipeline step: a topic being consumed + the controller consuming it
         (+ optionally its dead-letter reconciler)
engine   pipeline.Construct — the ONE shared assembly routine
profile  the host's per-queue choice of seam impls
host     the deployer binary: our own example/ mains, or an external repo's fx/plain-main app
```

## Principle

- **Declare, don't assemble.** A service is defined by three declarations — a seam contract (struct), a topology table (slice of stages), and a controller set (struct). The engine (`pipeline.Construct`) consumes these declarations and produces one lifecycle handle. The host fills seams and starts the handle. Assembly logic lives in exactly one place (the engine), not in every deployer's main.go.
- **Library vs. host.** The library (controllers, engine, extension interfaces) owns no server, reads no environment variables, catches no signals, and calls no `os.Exit`. The host owns transport (gRPC/HTTP), process model (signals, exit codes, health checks), and policy (credentials, impl selection, queue routing). This boundary makes the library importable by any host — fx, plain main, test harness — without unwanted side effects.
- **Profiles stay host-private.** The per-queue routing decision ("monorepo/main gets buildkite, monorepo/exp gets local runner") is host policy. It lives in the host's `profiles.go`, never crosses the library boundary, and maps to the library's `Factory` interfaces through thin adapters at the seam boundary.

```
┌───────────────────────── HOST ──────────────────────────┐
│  transport            process model         policy      │
│  gRPC/HTTP — mount    fx or plain main,    config, creds,│
│  controllers behind   signals, exit codes  impl selection,│
│  YOUR proto+server    health checks        queue routing │
└──────────┬──────────────────┬──────────────────┬─────────┘
           │ glue: pb svc →   │ Start(ctx)       │ Deps
           │  controllers     ▼ Stop(ctx)        ▼
┌──────────────────────── LIBRARY ────────────────────────┐
│  controllers        ONE lifecycle handle    extension   │
│  (pure logic)       from pipeline.Construct seams       │
│        NO server · NO env reads · NO signals · NO exit  │
└─────────────────────────────────────────────────────────┘
```

## Proposal

### Step 1 · `platform/lifecycle` — the one interface a host ever sees

```go
// Component is anything with a lifecycle. Construct returns one; hosts drive it.
type Component interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
}

// Group runs an ordered list of Components as one Component.
//
//   Start: members in order; if member i fails to start, members i-1…0 are
//          stopped in reverse and the error is returned — no half-started state.
//   Stop:  members in REVERSE order (work-acceptors drain before the
//          connections under them close); errors joined, none swallowed.
func NewGroup(ordered ...Component) *Group
```

The engine uses Group internally; hosts can also nest Groups (e.g. two services in one process). This replaces the ad-hoc `sync.WaitGroup` + `chan` + manual error-joining in today's main.go.

### Step 2 · `platform/pipeline` — the engine

```go
// Stage is one row of a service's topology table. D is that service's Deps type.
type Stage[D any] struct {
    // Key is the stage's LOGICAL topic key (e.g. topickey.Start). The engine maps
    // it to a physical topic name via the TopicNames option — the mapping is host
    // data, so two deployments can run the same pipeline on different topic names.
    Key consumer.TopicKey

    // New builds the stage's controller from the service's Deps. The engine calls
    // it ONCE, eagerly, inside Construct — so a nil/missing dependency fails at
    // boot with the stage's name on it, never mid-delivery.
    New func(D) (consumer.Controller, error)

    // DLQ, when non-nil, declares "this stage dead-letters". The engine then
    // derives the paired DLQ topic (<topic>_dlq, retry budget, DLQ-of-DLQ
    // disabled) AND registers this reconciler on the DLQ consumer. Declaring
    // one without getting the other is impossible — that's the invariant.
    DLQ func(D) (consumer.Controller, error)
}
```

```go
// Construct is the ONLY assembly code in the repo. Schematic:
func Construct[D any](deps D, stages []Stage[D], opts ...Option) (lifecycle.Component, error) {
    o := applyOptions(opts)                       // TopicNames map, Classifiers, extra Components

    registry := consumer.NewTopicRegistry()
    primary  := consumer.New(o.queues, registry, o.classifiers, subscriberName())
    dlq      := consumer.New(o.queues, registry, errs.AlwaysRetryableProcessor, subscriberName())

    for _, s := range stages {
        topic := o.physicalName(s.Key)            // logical key → deployment's topic name
        registry.Add(s.Key, subscription(topic))

        ctl, err := s.New(deps)                   // eager ⇒ boot-time, named failure
        if err != nil { return nil, fmt.Errorf("stage %s: %w", s.Key, err) }
        primary.Register(ctl)

        if s.DLQ != nil {                         // declared ⇒ pair + reconciler, derived together
            registry.Add(dlqKey(s.Key), dlqSubscription(topic))
            rec, err := s.DLQ(deps)
            if err != nil { return nil, fmt.Errorf("stage %s dlq: %w", s.Key, err) }
            dlq.Register(rec)
        }
    }

    // order is decided HERE, once: infra → publishers → consumers; Stop reverses it
    return lifecycle.NewGroup(o.infra, o.publishers, primary, dlq), nil
}
```

This unifies topic-registry construction, subscription configuration, controller creation, DLQ pairing, and consumer lifecycle into one call. Each stage is a single row in a typed table; the engine enforces the invariant that every DLQ-declaring stage gets a paired DLQ subscription and reconciler, derived together. The host never sees consumer internals.

### Step 3 · Service self-declaration — `submitqueue/orchestrator/pipeline.go`

A service's **entire** definition is three declarations — a struct (seams), a slice (topology), a constructor (controllers). No assembly code:

```go
// ① Deps: one field per dependency the pipeline needs.
// This struct IS the service's public API toward deployers.
type Deps struct {
    Logger  *zap.SugaredLogger
    Scope   tally.Scope
    Storage storage.Storage                // singleton seams
    Queues  messagequeue.Stores
    BuildRunner    buildrunner.Factory     // per-queue seams: Factory is a RESOLVER —
    ChangeProvider changeprovider.Factory  //   For(Config{QueueName}) → impl for THAT queue
    Scorer         scorer.Factory
    Analyzer       conflict.Factory
    Counter        counter.Counter
}

// ② Stages: the pipeline topology as a typed table.
// Adding a stage = adding one row. Nothing else, anywhere.
var Stages = []pipeline.Stage[Deps]{
    {
        Key: topickey.Start,
        New: func(d Deps) (consumer.Controller, error) {
            return start.NewController(d.Logger, d.Scope, d.Storage, d.Queues), nil
        },
        DLQ: func(d Deps) (consumer.Controller, error) {
            return dlq.NewRequestController(d.Logger, d.Scope, d.Storage,
                dlq.DecodeLandRequestID, dlq.TopicKey(topickey.Start),
                "orchestrator-start-dlq"), nil
        },
    },
    { Key: topickey.Validate, /* same shape */ },
    { Key: topickey.Batch,    /* same shape */ },
    // … all 12 stages
}

// ③ Controllers: RPC-facing controllers, constructed but NOT bound to any
// wire contract. Binding to a proto service + transport is host glue,
// because consumers may use different protos or transports.
type Controllers struct {
    Ping *controller.PingController
}

func NewControllers(d Deps) Controllers {
    return Controllers{Ping: controller.NewPingController(d.Logger, d.Scope)}
}
```

### Step 4 · Host-private profiles — `service/.../profiles.go`

Profiles stay entirely in the host. Nothing profile-shaped crosses the library boundary:

```go
type Profile struct {
    BuildRunner    buildrunner.BuildRunner
    ChangeProvider changeprovider.Provider
    Scorer         scorer.Scorer
    Analyzer       conflict.Analyzer
}

type Profiles struct {
    byQueue        map[string]Profile
    defaultProfile Profile
}

func (p Profiles) For(queue string) Profile {
    if prof, ok := p.byQueue[queue]; ok { return prof }
    return p.defaultProfile
}

func newProfiles(cfg Config) Profiles {
    return Profiles{
        byQueue: map[string]Profile{
            "monorepo/main": {BuildRunner: buildkite.New(cfg.CI), Scorer: heuristic.New()},
            "monorepo/exp":  {BuildRunner: local.New(),           Scorer: heuristic.New()},
        },
        defaultProfile: Profile{BuildRunner: noop.New(), Scorer: constant.New()},
    }
}

// Thin adapters crossing the boundary — the http.HandlerFunc trick:
type buildRunnerFunc func(buildrunner.Config) (buildrunner.BuildRunner, error)
func (f buildRunnerFunc) For(c buildrunner.Config) (buildrunner.BuildRunner, error) { return f(c) }

func (p Profiles) BuildRunnerFactory() buildrunner.Factory {
    return buildRunnerFunc(func(c buildrunner.Config) (buildrunner.BuildRunner, error) {
        return p.For(c.QueueName).BuildRunner, nil
    })
}
// … same pattern for Scorer, Analyzer, ChangeProvider
```

### Step 5 · Host main.go — `service/.../main.go`

```go
func run(ctx context.Context) error {
    cfg := loadConfig()                                    // env/flags: host-owned
    logger, scope := newLogger(cfg), newScope(cfg)

    store  := storagemysql.New(cfg.DB, logger, scope)
    queues := mqmysql.New(cfg.QueueDB, logger, scope)
    profiles := newProfiles(cfg)                           // Step 4

    deps := orchestrator.Deps{
        Logger: logger, Scope: scope,
        Storage: store, Queues: queues,
        BuildRunner:    profiles.BuildRunnerFactory(),
        ChangeProvider: profiles.ChangeProviderFactory(),
        Scorer:         profiles.ScorerFactory(),
        Analyzer:       profiles.AnalyzerFactory(),
    }

    pl, err := pipeline.Construct(deps, orchestrator.Stages,
        pipeline.TopicNames(cfg.TopicNames),               // logical → physical topic names
        pipeline.Classifiers(backendClassifiers()),
    )
    if err != nil { return err }

    srv := grpc.NewServer()
    ctls := orchestrator.NewControllers(deps)
    pb.RegisterSubmitQueueOrchestratorServer(srv, rpcServer{c: ctls})

    if err := pl.Start(ctx); err != nil { return err }
    defer pl.Stop(context.Background())
    return serveUntilDone(ctx, srv)
}
```

## At delivery time — all the pieces meeting

```
row appears on topic "start", partition key "monorepo/exp"
 └─▶ primary consumer (built in Step 2) holds the partition lease, fetches delivery
      └─▶ start controller (built by Step 3's row) .Process(ctx, delivery)
           └─▶ needs a runner for THIS queue:
               deps.BuildRunner.For(Config{QueueName: "monorepo/exp"})
                └─▶ Step 4's adapter → profiles.For("monorepo/exp").BuildRunner → local runner
                     (the SAME Deps field answers "monorepo/main" with buildkite
                      on the next delivery — that's the Factory-as-resolver contract,
                      identical to today's buildRunnerFactory{queues} in main.go)
      controller returns nil ⇒ ack
      controller returns err ⇒ Step 5's classifier decides: retry (nack) or not;
      after retry budget ⇒ row moves to "start_dlq" ⇒ Step 2's derived pairing
      guarantees the reconciler from Step 3's DLQ field is listening there
```

## Generalizes across all four services

```
                 gateway              orchestrator          stovepipe            runway
────────────────────────────────────────────────────────────────────────────────────────────
 Deps seams   counter · storage ·  changeprovider ·      storage · counter ·  storage ·
              queueconfig.Store ·  buildrunner · scorer  sourcecontrol.       merger Factory
              requestlog store     analyzer · validator  Factory ·
                                   (+7 speculation)      queueconfig.Store

 Stages       log                  start · cancel ·      process              mergeconflictcheck ·
 (rows)                            validate · batch ·                         merge
                                   … (+ DLQ column)

 Controllers  Gateway              Orchestrator          Stovepipe            Runway
              Ping·Land·Cancel     Ping                  Ping·Ingest          Ping

 host keeps   impl selection · creds · TopicKey→name map · classifiers · transport · signals
              — identical across all four columns: a new service copies any column and
                fills in two rows of DATA
────────────────────────────────────────────────────────────────────────────────────────────
```

Two integration surfaces fall out — the Go library surface above (Deps · Stages · Controllers · `pipeline.Construct`), and the proto contracts in `api/` for cross-language consumers. The Go library never dictates the wire contract; binding controllers to a proto service + transport is host glue.

## What the engine enforces

| Concern | Enforced by |
|---|---|
| Start/Stop ordering, rollback on partial failure | `lifecycle.Group` — one implementation, tested once |
| DLQ pair + reconciler always present together | Engine property: any row declaring `DLQ:` gets both the DLQ subscription and the reconciler, derived together |
| Missing seam at boot | Eager ctor run in `Construct` ⇒ named boot error with the failing stage |
| Naming drift | Nothing to name — services export data (Deps struct + Stages slice), not assembly functions |
| Wrong data (bad row) | Typechecking + a trivial data test (`Stages` keys unique) |

## Fluent builder API — convenience layer on top of the engine

The `pipeline.Construct[D]` engine is the foundational API: typed, composable, and testable. For deployers who wire a single orchestrator with a handful of queues, a **fluent builder** provides a more readable entry point without hiding or replacing the engine.

### Usage

```go
app, err := submitqueue.New().
    WithStorage(store).
    WithMessageQueue(mq).
    WithQueue(
        submitqueue.Queue("go-code").
            WithChangeProvider(ghProvider).
            WithBuildRunner(buildkiteBuildRunner).
            WithScorer(defaultScorer).
            WithConflictAnalyzer(tango),
    ).
    WithQueue(
        submitqueue.Queue("monorepo/exp").
            WithChangeProvider(ghProvider).
            WithBuildRunner(localRunner).
            WithScorer(defaultScorer).
            WithConflictAnalyzer(fileOverlap),
    ).
    Build()

if err != nil { return err }
if err := app.Start(ctx); err != nil { return err }
defer app.Stop(context.Background())
```

### Implementation sketch

```go
// submitqueue/builder.go
package submitqueue

// Builder accumulates configuration for a SubmitQueue orchestrator app.
// It is a convenience layer — Build() populates a Deps struct, constructs
// profiles, and calls pipeline.Construct under the hood.
type Builder struct {
    storage  storage.Storage
    queues   messagequeue.Stores
    perQueue map[string]Profile
    opts     []pipeline.Option
    errs     []error
}

func New() *Builder { return &Builder{perQueue: map[string]Profile{}} }

func (b *Builder) WithStorage(s storage.Storage) *Builder {
    b.storage = s; return b
}

func (b *Builder) WithMessageQueue(q messagequeue.Stores) *Builder {
    b.queues = q; return b
}

func (b *Builder) WithQueue(qb QueueBuilder) *Builder {
    b.perQueue[qb.name] = qb.profile; return b
}

func (b *Builder) WithOption(o pipeline.Option) *Builder {
    b.opts = append(b.opts, o); return b
}

func (b *Builder) Build() (*App, error) {
    // Validate required fields (storage, queues, at least one queue).
    // Populate Deps from the accumulated state.
    // Build profiles registry from perQueue map.
    // Call pipeline.Construct(deps, orchestrator.Stages, b.opts...).
    // Return App wrapping the lifecycle.Component.
}

// QueueBuilder accumulates per-queue extension selections.
type QueueBuilder struct {
    name    string
    profile Profile
}

func Queue(name string) QueueBuilder { return QueueBuilder{name: name} }

func (q QueueBuilder) WithChangeProvider(cp changeprovider.ChangeProvider) QueueBuilder {
    q.profile.ChangeProvider = cp; return q
}

func (q QueueBuilder) WithBuildRunner(br buildrunner.BuildRunner) QueueBuilder {
    q.profile.BuildRunner = br; return q
}

func (q QueueBuilder) WithScorer(s scorer.Scorer) QueueBuilder {
    q.profile.Scorer = s; return q
}

func (q QueueBuilder) WithConflictAnalyzer(a conflict.Analyzer) QueueBuilder {
    q.profile.Analyzer = a; return q
}
```

### Design constraints

- **Convenience, not replacement.** The builder calls `pipeline.Construct` — it does not bypass or duplicate the engine. Deployers who need full control (custom `Option`s, multi-service composition, fx integration) use the engine directly.
- **Compile-time type safety.** Each `With*` method takes the concrete extension interface, not a string hint. A missing or mistyped extension is a compile error.
- **`QueueBuilder` is a value type.** The fluent chain returns copies, not pointers, so partial builders are safe to reuse as templates (e.g. a `baseQueue` with defaults that each real queue overrides).
- **`Build()` validates eagerly.** Missing required fields (no storage, no queues, zero queue profiles) produce a clear error at build time, not a nil-pointer panic at runtime.
- **No global state.** `New()` returns an isolated builder. Multiple orchestrator apps can coexist in the same process (useful for integration tests).

## Trade-off: profile hints vs. removing QueueConfig entirely

Profile selection (which scorer/conflict/build-runner a queue uses) deserves separate scrutiny because there is an open question about whether `QueueConfig` should exist at all.

### Current role of QueueConfig

`QueueConfig` today is a single-field entity (`Name string`). Its sole consumer is the gateway's `LandController`, which calls `queueconfig.Store.Get(ctx, queue)` to reject requests targeting unknown queues — a pure name-validation gate. The orchestrator does not import `queueconfig` at all; it maintains its own hardcoded `queueRegistry` with no programmatic link to the YAML config. The TODO on line 477 of the orchestrator example envisions bridging the two ("see also queueconfig.Store, which holds the per-queue data half"), but that bridge does not exist today.

### Three options for per-queue extension selection

| Option | Description | Pros | Cons |
|---|---|---|---|
| **A: Profile hints in QueueConfig** | Add `QueueProfile` fields to the entity; deployers declare scorer/conflict/etc. in `queues.yaml`; the wiring layer maps hint strings → instances. | Single source of truth for queue identity + behavior. YAML-only queue addition for known extension types. | Expands `QueueConfig` from a pure name registry into a config carrier — if QueueConfig is later removed, these fields need a new home. The entity gains fields the gateway doesn't use (profile hints are orchestrator-only). |
| **B: Separate profile config file** | Leave `QueueConfig` as-is (name-only). Create a separate config consumed only by the host's profiles.go. | Clean separation: gateway validates names, host resolves profiles. `QueueConfig` stays minimal and removable. No entity-level coupling. | Two config files to keep in sync (queue names must match). More moving parts in the wiring layer. |
| **C: Profiles stay in Go code (recommended)** | Per-queue profiles stay in the host's `profiles.go` as Go code. Full type safety — a misspelled scorer name is a compile error, not a runtime lookup miss. Consistent with the existing philosophy ("all behavioral and VCS configuration lives in the extension factory implementations"). | Simplest change. No new config surface. Type-safe. Matches the "host owns policy" principle. `core/queueprofile` stays trigger-gated per the in-code TODO: if a trigger fires, `profiles.go` moves wholesale — nothing profile-shaped ever crossed the boundary. | Adding a new queue requires a recompile. |

### If QueueConfig is removed

If queue name validation moves to a different mechanism (e.g. the host's profile registry becomes the implicit registry of valid queues, and the gateway queries it), then `QueueConfig` + `queueconfig.Store` can be removed without losing the validation gate. This would be a clean removal: the `queueconfig` extension package, its YAML impl, its mock, and the `QueueConfig` entity all go away. Option C is resilient to this removal because profiles never depended on `QueueConfig` in the first place.

## Rejected

- **DI framework (wire/dig/fx).** `pipeline.Construct` is not DI: no runtime graph, no reflection, no topo-sort — a typed engine over declarative data. One-offs enter via `Option`s. Hosts that use fx can wrap the engine in `fx.Provide` / `fx.Hook` without the engine knowing.
- **Hot-reload of queue configs.** Out of scope. The YAML is loaded at startup. Hot-reload can build on this foundation later.
- **Changing the Factory interface contract.** The existing `Factory.For(Config)` pattern is sound. The engine consumes factories exactly as today's main.go does; profiles produce them through the same thin adapters currently in main.go.
- **Promoting `newChangeProvider` / `newGitHubChangeProvider` / `newPhabChangeProvider` out of the example.** These are deployment-specific (token sources, HTTP clients, timeouts). They stay in the host.
- **Merging profiles into `queueconfig`.** The config store is a resolution target (key/value); profiles aggregate behavioral instances. Mixing them gives `queueconfig` a dependency on every extension interface, violating the "stores are resolution targets" principle.

## Migration path

The refactor can land incrementally:

1. **`platform/lifecycle`** — pure addition, no existing code changes. Tested independently.
2. **`platform/pipeline`** — pure addition, no existing code changes. Tested with mock stages.
3. **Service self-declarations** (`orchestrator/pipeline.go`, etc.) — pure addition alongside the existing controllers.
4. **Example main.go rewrite** — the one breaking change: replace ~950 lines with ~100 lines composing the new packages. The existing behavior is identical; the diff is large but mechanical.

Steps 1–3 can land independently as pure additions with zero behavioral change. Step 4 is the switch-over.
