# SubmitQueue Repository Guide

## Key Concepts

SubmitQueue is a distributed system for managing code submission workflows. It follows clean architecture with interface-driven extensibility.

**Immutability and Eventual Consistency:**

1. **Immutable entities** — once created, don't modify in place. Create new versions with updated fields.
2. **Eventual consistency** — handle stale reads, idempotent operations, and convergence over time.
3. **Event sourcing** — store events (what happened) rather than just current state for critical changes.
4. **Optimistic locking** — use version numbers instead of pessimistic locks. Avoid transactions; prefer optimistic concurrency and retries. **Version arithmetic lives in the controller, not the storage layer.** Update methods take both `oldVersion` (the where-clause guard) and `newVersion` (the value to write); the store performs a pure conditional write. Controllers compute `newVersion = oldVersion + 1`, call the store, and only assign `entity.Version = newVersion` after the call succeeds. Pre-incrementing in memory before the call is a bug pattern — on error the in-memory version drifts ahead of the database. See [submitqueue/extension/storage/README.md](submitqueue/extension/storage/README.md).
5. **Idempotency keys** — include unique request IDs, check for duplicates before executing.

```go
// Immutable entity pattern
type Request struct {
    ID        string
    Version   int       // For optimistic locking
    Status    Status
    CreatedAt int64
    UpdatedAt int64
}

// Controller pattern — version arithmetic outside storage, assigned only on success
newVersion := request.Version + 1
if err := store.UpdateStatus(ctx, request.ID, request.Version, newVersion, newStatus); err != nil {
    return err
}
request.Version = newVersion
```

## Architecture

### Project Layout

```
submitqueue/                        # repo root (Go module github.com/uber/submitqueue)
├── api/                            # Published wire contracts (cross-domain/external)
│   ├── submitqueue/{gateway,orchestrator}/{proto,protopb}/   # RPC (proto)
│   ├── stovepipe/{proto,protopb}/  # single-service RPC (proto) — no service segment yet
│   ├── runway/{proto,protopb}/     # RPC (proto) — single-service domain, no service segment
│   └── runway/messagequeue/        # external queue contracts (proto + protojson)
├── platform/                       # SHARED cross-domain packages — no domain deps
│   ├── errs/, metrics/, consumer/, http/
│   ├── base/                       # SHARED entities (change/, messagequeue/, …)
│   └── extension/                  # SHARED extension contracts + backends (counter/, messagequeue/, …)
├── submitqueue/                    # SubmitQueue domain
│   ├── gateway/                    # Gateway service (port 8081) - entry point
│   ├── orchestrator/               # Orchestrator service (port 8082) - coordinates jobs
│   ├── entity/                     # SubmitQueue-specific domain entities
│   ├── extension/                  # SubmitQueue-specific extension impls (storage, counter, mergechecker, …)
│   └── core/                       # SubmitQueue-internal shared infra (consumer wiring, request, topickey, …)
├── stovepipe/                      # Stovepipe domain (single Ping-only service for now)
│   └── controller/                 # Business logic (currently just Ping); entity/extension/core added as it grows
├── runway/                         # Runway domain (single service — the domain *is* the service)
│   └── controller/                 # Runway service controllers (consumes the merge queues; no gateway/orchestrator split)
├── tool/                           # Development and CI tooling
├── service/                        # Runnable server/client wiring (entry points + Docker Compose)
│   ├── submitqueue/                # Runnable SubmitQueue servers/clients + Docker Compose
│   ├── stovepipe/                  # Runnable Stovepipe server/client + Docker Compose
│   └── runway/                     # Runnable Runway server/client + Docker Compose
├── test/
│   ├── e2e/submitqueue/            # End-to-end tests (full stack)
│   ├── integration/                # Integration tests (platform/, submitqueue/, stovepipe/, …)
│   └── testutil/                   # Test utilities (ComposeStack, MySQL helpers)
└── doc/                            # Documentation
```

The `platform/` tree holds code reused across domains (infrastructure, shared entities, shared extension contracts). A multi-service **domain** (e.g. `submitqueue/`) keeps the same internal layout (`gateway/`, `orchestrator/`, `entity/`, `extension/`, `core/`); a domain's own `core/` (e.g. `submitqueue/core/`) holds infra shared only between that domain's services. A **single-service domain** collapses that split — the domain *is* the service, so its controllers live directly under the domain root (e.g. `runway/controller/`, `stovepipe/controller/`) with no `gateway/`/`orchestrator/` segment, and its wire contract is service-segment-free (`api/{domain}/`). `runway` is a consumer-only landing service with no gateway; `stovepipe` is currently a single Ping-only service that can grow the other layers (`entity/`, `extension/`, `core/`) as it gains real behavior.

The `api/` tree holds **published** wire contracts — those depended on from outside the owning domain. RPC contracts live at `api/{domain}/{service}/` (`proto/` for `.proto` sources, `protopb/` for committed generated Go); for a single-service domain the service segment is dropped, so the contract lives directly at `api/{domain}/` (e.g. `api/runway/{proto,protopb}/`). A service package may hold multiple `.proto` files, all generating into the same `protopb/`. External message-queue contracts live at `api/{domain}/messagequeue/` (see Message Queue Contracts below). Internal queue contracts do **not** go here — they live under `{domain}/core/messagequeue/`.

### Platform notes

- Import path `github.com/uber/submitqueue/platform/http` uses Go package name `http` and aliases the standard library as `nethttp` inside the package. Source files that also import `net/http` should import the platform package with a distinct alias (for example `phttp "github.com/uber/submitqueue/platform/http"`) and call `phttp.NewClient`, `phttp.BaseURLTransport`, etc.
- `platform/base` is the shared entity root; subpackages (`change`, `messagequeue`, …) hold concrete types. The root `base` package is documentation-only.

### Services

Each service follows the same layout:

```
<service>/
└── controller/          # Business logic (pure, transport-agnostic)
    ├── {method}.go      # RPC controllers (e.g., land.go, ping.go)
    ├── {method}_test.go
    └── {step}/          # Queue message controllers (e.g., request/)
        ├── {step}.go
        └── {step}_test.go
```

Wire contracts for a service live separately under `api/{domain}/{service}/` (see Project Layout): `proto/` holds `.proto` sources and `protopb/` holds the committed generated stubs. For a single-service domain the service root *is* the domain root (e.g. `runway/controller/`), and its wire contract lives at `api/{domain}/` (e.g. `api/runway/`) with no service segment.

### Controllers

Two types, both containing pure business logic independent of infrastructure:

**RPC Controllers** — in `{service}/controller/`, accept protobuf types:
```go
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error)
```

**Queue Message Controllers** — in `{service}/controller/{step}/`, implement `consumer.Controller`:
```go
// Return nil to ack, error to nack. Consumer handles ack/nack automatically.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error
```

Controllers receive `consumer.Delivery` (subset interface without Ack/Nack) to enforce separation of business logic from infrastructure.

**Queue payloads: IDs within a boundary, full payloads across one.** When producer and consumer share a store (same service — e.g. `build`→`buildsignal`, `validate`→`mergeconflict`), put only the entity **ID** on the queue and reload from storage (the store is the source of truth, messages stay small, redelivery is idempotent). When a queue **crosses a service boundary** (the consumer cannot read the producer's store — e.g. orchestrator→runway), publish the **full payload** the consumer needs, and have the **client own the correlation ID** so it can match the async result back to the work it is tracking. The queue's **owner defines the wire contract and topic keys** (in its own domain package); the other side imports them.

### Entities

Domain objects live under each domain's `entity/` tree, or under `platform/base/` when shared across domains. Guidelines:
1. Pure and framework-agnostic — no external dependencies
2. Value types, not references
3. `int64` milliseconds for timestamps (`CreatedAt int64`) and durations (`TimeoutMs int64`)
4. Every field must have a comment
5. Reference other entities by ID (string or int), not directly
6. String enums with sentinel values (`""` for unknown)
7. Docs describe the data, not the choreography — say what a type or field *is* and its invariants (immutability, uniqueness scope, units, valid range), never which controller/stage/seam reads or writes it. Ownership and write-path rules live with the code that owns them (controller, store, or extension docs). Lifecycle enums may define states in terms of pipeline stages where that *is* the state's meaning (e.g. "admitted under the build budget"), but must not name the components that perform transitions.

### Extensions

Vendor-agnostic, pluggable interfaces with implementations in subdirectories:
1. **Shared across domains** — define interfaces at `platform/extension/{ext}/`, implementations at `platform/extension/{ext}/{impl}/`.
2. **Domain-specific** — define at `{domain}/extension/{ext}/`, implementations at `{domain}/extension/{ext}/{impl}/`.
3. Factory interface for dependency injection and lifecycle management (constructed in wiring, not inside `platform/extension` packages).

**Extensions hold contracts and implementations only — not factories or routing.**

A `{domain}/extension/{ext}` or `platform/extension/{ext}` package contains the behavioral interface, its `Config`, the `Factory` *interface*, and impl constructors `New(...)` that return the interface. It must **not** contain `Factory` *implementations* (`NewFactory()` constructors or factory structs) or any queue-selection logic.

Why: an impl package (e.g. `scorer/heuristic`) can't know the queue topology or the other impls, so a "which impl for which queue" decision doesn't belong there. Per-queue routing — and the small adapters that wrap a `New(...)` impl in the `Factory` interface — live in the wiring layer (e.g. `service/{domain}/{service}/server/main.go`), the one place that knows the full queue set. That's where you route on `Config.QueueName`.

Rule of thumb: if you're about to add a `NewFactory()` or a `map[queue]impl` under `{domain}/extension/` or `platform/extension/`, it belongs in the wiring layer instead.

**Design interfaces for the technology *space*, not the implementation in front of you.** The interface is a contract every backend will have to satisfy — SQL, key-value (DynamoDB, Bigtable), document, message queue, search, RPC, in-memory, mocks. If the contract assumes a capability that some plausible backend can't provide cheaply, you've baked the current impl's strengths into the API.

Common over-constraints to avoid:
- **Batch atomicity** (multi-row inserts as one transaction) — many KV stores can't do this. Prefer single-record primitives + caller loops + idempotency-on-retry.
- **Multi-key queries** (`WHERE x IN (...)`) — fine in SQL, awkward elsewhere. Prefer per-key reads.
- **Query-by-attribute / secondary indexes** (`WHERE attr = ?`, `ListByX(attr)`) — a plain KV store cannot look up by anything but the primary key. The mechanical smell test: **if a schema change adds a `KEY idx_*` to make a store method viable, the contract has stopped being get/put-by-key.** Instead, derive the primary key from the composite identity the caller already holds (e.g. `{parentID}/{hash(child identity)}`), and remember that domain state is often already the index — an entity that references its children (a tree listing its paths) enumerates their keys for free. When neither applies, a genuinely needed reverse lookup gets its own first-class mapping store keyed by the attribute — in the KV space that is the mechanism, not a workaround. See the decision path in [submitqueue/extension/storage/README.md](submitqueue/extension/storage/README.md#key-value-contract).
- **Server-side filters** (joins, sub-queries, complex predicates) — push filtering and aggregation to the caller; keep the store responsible only for "get/put by key" semantics.
- **Transactions across entities** — virtually no distributed store offers this. Use eventual consistency + idempotency.
- **Strict ordering / exactly-once** in messaging — most queues are at-least-once with best-effort ordering. Make consumers idempotent.
- **Synchronous, low-latency calls** for things that may run remotely — design for retry/backoff and timeouts, not assumed-fast.

The cost of "callers loop over a small batch" is usually negligible. The cost of forcing every future backend to fake a capability the API demanded is permanent.

When in doubt, ask: *"If the next implementation were DynamoDB / Kafka / Bigtable / a remote RPC service / an in-memory map, could it satisfy this signature without contortion?"* If the answer is no, simplify the contract.

**Input contract — identity in, resolve internally.** A decision/action extension takes the orchestrator's thin reference entity at its pipeline-stage granularity — `entity.Request` (request stage) or `entity.Batch` / `[]entity.Batch` (batch stage) — never controller-pre-resolved data. It resolves the granular content it needs (changes, diffs, targets) through dependencies injected at its `Factory` (e.g. a request store, a change provider), not a global aggregator. Stores (`storage`, `changestore`) and config (`queueconfig`) are the exception — they are the resolution *targets* and stay key/value-shaped per the rule above. `conflict.Analyzer` is the reference shape; every new extension or signature change must follow it. See [doc/rfc/submitqueue/extension-contract.md](doc/rfc/submitqueue/extension-contract.md).

### Import Paths

Paths follow the directory layout: shared packages live under `platform/` at the repo root; domain code nests under `submitqueue/`, `stovepipe/`, and other domain folders.

- RPC Controllers: `github.com/uber/submitqueue/{domain}/{service}/controller` (e.g. `.../submitqueue/gateway/controller`; single-service domains drop the `{service}` segment, e.g. `.../runway/controller`)
- Queue Controllers: `github.com/uber/submitqueue/{domain}/{service}/controller/{step}` (single-service: `.../runway/controller/{step}`, e.g. `.../runway/controller/merge`)
- Proto (generated): `github.com/uber/submitqueue/api/{domain}/{service}/protopb` (single-service: `.../api/{domain}/protopb`, e.g. `.../api/runway/protopb`)
- Queue contracts: external `github.com/uber/submitqueue/api/{domain}/messagequeue`; internal `github.com/uber/submitqueue/{domain}/core/messagequeue`
- Domain entities: `github.com/uber/submitqueue/{domain}/entity` (e.g. `.../submitqueue/entity`)
- Domain extensions: `github.com/uber/submitqueue/{domain}/extension/{ext}[/{impl}]` (e.g. `.../submitqueue/extension/storage/mysql`)
- Cross-domain consumer framework: `github.com/uber/submitqueue/platform/consumer`; internal pipeline topic keys: `github.com/uber/submitqueue/{domain}/core/topickey` (external queue topic keys live with their contract package, e.g. `api/runway/messagequeue`)
- Domain-internal infra: `github.com/uber/submitqueue/{domain}/core/{pkg}` (e.g. `.../submitqueue/core/request`)
- Shared entities: `github.com/uber/submitqueue/platform/base/{pkg}` (e.g. `.../platform/base/messagequeue`)
- Shared extensions: `github.com/uber/submitqueue/platform/extension/{ext}[/{impl}]` (e.g. `.../platform/extension/messagequeue/mysql`)
- Cross-domain infra: `github.com/uber/submitqueue/platform/{pkg}` (e.g. `.../platform/errs`, `.../platform/metrics`, `.../platform/http`)

## Development

### Build System

Bazel with Bzlmod (NOT WORKSPACE).

- **Dependencies**: `MODULE.bazel` + `go.mod` (both must be updated)
- **Bazel wrapper**: `./tool/bazel` (Bazelisk). With direnv (`.envrc`), use `bazel` directly.
- **BUILD files**: Every Go package needs `BUILD.bazel`. Run `make gazelle` after adding/removing Go files.
- **CI enforces** BUILD files are in sync — always run `make gazelle` before committing.

### Proto Generation

Generated proto files are committed. When modifying `.proto` files:
1. Edit in `api/{domain}/{service}/proto/` (e.g. `api/submitqueue/gateway/proto/`)
2. `make proto` (generates `*.pb.go`, `*_grpc.pb.go`, `*.pb.yarpc.go` into `api/{domain}/{service}/protopb/`)
3. Commit all generated files

To add a new `.proto` to a service, drop it in the service's `api/{domain}/{service}/proto/` dir, add it to that package's `srcs` in `api/{domain}/{service}/proto/BUILD.bazel` and its `exports_files`, then `make proto && make gazelle`. The codegen and `make proto` copy loop already handle multiple `.proto` files per package.

### Message Queue Contracts

Queue payloads are defined in **proto3** (`.proto` under `proto/`, generated Go in `protopb/` as the binding) and serialized as **protobuf JSON** (protojson) so the queue keeps storing self-describing JSON. Location follows audience: external/cross-domain contracts go under `api/{domain}/messagequeue/`; internal contracts (used only within the owning domain) go under `{domain}/core/messagequeue/`. Bazel `visibility` enforces the split — internal targets are domain-scoped, `api/` targets are public. See [doc/rfc/messagequeue-contract.md](doc/rfc/messagequeue-contract.md).

The message types are generated; the contract package adds only generic `protojson` glue — `Marshal(m)` / `Unmarshal[T](b, m)` — owning the wire conventions: `UseProtoNames` (snake_case fields), UPPER_SNAKE enum values, int64-as-string, unknown fields discarded on read (additive evolution). The topic key(s) carrying a message are declared on the message via the `topic_keys` proto option — a `google.protobuf.MessageOptions` extension defined in `api/base/messagequeue`. A topic key is a stable logical name, not a concrete wire topic; each implementer maps it to its backend's topic name, and a `TopicKeys(msg)` reflection helper reads the option back. It is contract metadata, not the hot path — publish/consume still routes on `consumer.TopicKey` + `TopicRegistry`. The contract package owns both halves: the proto payload and the `TopicKey` constants for its topic keys. A contract test round-trips the payloads and asserts every topic key is bound to exactly one message. Shared field types (`Change`, `Strategy`) are shared protos under `api/base/{change,mergestrategy}`. `api/runway/messagequeue/` is the reference example.

### Naming Conventions

- **Directories**: singular (`mock/`, `entity/`, not `mocks/`, `entities/`)
- **Files**: `{method}.go`, `{entity}.go`, `{file}_test.go`, `BUILD.bazel`
- **Proto files**: `{service}.proto`
- **Test compose contexts**: the `testContext` passed to `NewComposeStack` (and thus the `sq-test-{context}-…` Docker project/container names) must be **domain-qualified** — `{category}-{domain}-{name}` where `{category}` is `svc`/`ext`/`core`/`e2e` and `{domain}` is `submitqueue`/`stovepipe`/… (omit the domain only for shared/cross-domain suites, e.g. `ext-messagequeue-sql`). This keeps containers unambiguous and lets suites run in parallel. See [doc/howto/TESTING.md](doc/howto/TESTING.md#container-naming).
- **README files**: Do not duplicate interface or type definitions as code blocks in READMEs. Describe behavior in prose and let readers navigate to the source. Only include code samples when explicitly instructed.
- **Markdown prose width**: Do not hard-wrap prose in Markdown docs (RFCs under `doc/`, READMEs). Write one line per paragraph and one line per list item, and let the editor soft-wrap — hard wrapping at a fixed column renders as a narrow fixed-width column regardless of window size. Code blocks, tables, and ASCII diagrams keep their own line breaks.

### Makefile

Targets are **alphabetically sorted**. Each target has `## Description` for auto-generated help and shell completion:
```makefile
integration-test: build-all-linux ## Run all integration tests (auto-builds binaries)
	@$(BAZEL) test //test/integration/... --test_output=streamed
```

### Common Make Targets

```bash
make build              # Build all services
make test               # Run unit tests
make lint               # Run all linters (fmt + YAML)
make fmt                # Format Go and YAML code
make check-mocks        # Check mock files are up to date
make check-tidy         # Check go.mod and MODULE.bazel are tidy
make check-gazelle      # Check BUILD.bazel files are up to date
make tidy               # Run go mod tidy + bazel mod tidy
make gazelle            # Update BUILD.bazel files
make mocks              # Generate mock files using mockgen
make integration-test   # Run all integration tests (Docker-based)
make e2e-test           # Run end-to-end tests
make proto              # Regenerate proto files
make local-submitqueue-start        # Start full stack with Docker Compose
make local-submitqueue-ps           # Show running containers and ports
make local-submitqueue-logs         # View logs from all services
make local-stop         # Stop all services
make clean              # Clean Bazel cache
```

### Common Workflows

**Add new RPC method:**
1. Edit `api/{domain}/{service}/proto/*.proto` → `make proto`
2. Add controller in `{domain}/{service}/controller/`
3. Wire up in `service/{domain}/{service}/server/main.go`

**Add new queue message controller:**
1. Create `{domain}/{service}/controller/{step}/` implementing `consumer.Controller`
2. Wire up in `service/{domain}/{service}/server/main.go`

**Add new extension:**
1. Create the extension under `{domain}/extension/{ext}/{impl}/` (domain-specific, e.g. `submitqueue/extension/...`) or `platform/extension/{ext}/{impl}/` (shared across domains) with factory and interfaces
2. Add `BUILD.bazel`, tests, and README.md

**Add new entity:**
1. Create `{domain}/entity/{entity}.go` (domain-specific) or add packages under `platform/base/` (shared) with test file and `BUILD.bazel`

**Add gomock for an extension interface:**

Mocks are checked-in files generated by [mockgen](https://github.com/uber-go/mock). Run `make mocks` to regenerate, then `make gazelle` to update BUILD files. See `submitqueue/extension/storage/mock/` for the canonical example.

To add a mock for a new interface file in an existing mock package (e.g., `submitqueue/extension/storage/new_store.go`):

1. Add a `//go:generate` directive to the interface file:
   ```go
   //go:generate mockgen -source=new_store.go -destination=mock/new_store_mock.go -package=mock
   ```
2. Run `make mocks` to generate the mock file.
3. Run `make gazelle` to update `BUILD.bazel` files.
4. Commit the generated mock file.

To create a mock package for a new extension (e.g., `submitqueue/extension/newext/mock/`):

1. Add `//go:generate` directives to each interface file (same pattern as above).
2. Create the `mock/` directory: `mkdir submitqueue/extension/newext/mock/`.
3. Run `make mocks` to generate mock files into the new directory.
4. Run `make gazelle` to create the `BUILD.bazel` file automatically.

For inline mocks (mock in the same package, e.g., `platform/extension/messagequeue/mysql/mock_stores.go`):

1. Add a `//go:generate` directive with `-package=mypkg` and `-destination=mock_file.go`.
2. Run `make mocks` and `make gazelle`.

**Using mocks in tests:**
```go
import storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"

ctrl := gomock.NewController(t)
mockStore := storagemock.NewMockRequestStore(ctrl)
mockStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
```

Test `BUILD.bazel` deps:
```starlark
deps = [
    "//submitqueue/extension/storage/mock",
    "@org_uber_go_mock//gomock",
]
```

### Testing

- **Unit test package naming** — unit tests must use the same package name as the code under test (not a `*_test` package) so tests can access implementation details when needed.
- **Table-driven tests** — prefer table-driven tests with `t.Run` subtests over individual test functions.
- **Avoid asserting on error messages** — assert on error type or check the error with `require.Error`, do not `assert.Contains(t, err.Error(), message)`
- **No change detector tests** — don't assert on default values, internal structure, or implementation details that can change without affecting behavior. Test what the code *does*, not how it's constructed.
- **No `time.Sleep` for synchronization** — use channels, callbacks, condition variables.
- **Use testify** — `assert`/`require` instead of `t.Fatal()`.

**Integration tests** use Docker Compose via `testutil.ComposeStack`:
- Package naming: folder name as package (NOT `*_test` suffix)
- Bazel: add `tags = ["integration", "requires-network"]` and `data = [...]` for every input the test reads (compose file, schema dirs, Dockerfiles, Bazel-built `*_linux` service binaries). Tests are hermetic: never resolve the repo root — resolve inputs from runfiles via `testutil.Runfile` and stage docker build contexts with `testutil.WithBuildContext`.
- Use `testutil.NewComposeStack()` with meaningful context (e.g., `"ext-storage-mysql"`)

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for full testing guide.

### CI and Validation

CI runs on every PR and enforces all checks via a `required-checks` gate. **Before committing, validate locally:**

1. `make fmt` — format Go and YAML code (CI will reject unformatted code)
2. `make lint` — run all linters (formatting check)
3. `make check-tidy` — ensure `go.mod` and `MODULE.bazel` are tidy
4. `make check-gazelle` — ensure `BUILD.bazel` files are up to date

### Commit Style

1. Use conventional commits specification

### Code Style

1. **Structured logging** — `zap.SugaredLogger` with `Debugw`/`Infow`/`Errorw(msg, key, val, ...)`. Never unstructured methods.
2. **Interfaces for behavior, structs for data** — use interfaces for behavioral contracts (Consumer, Controller, Storage). Use structs for data containers, configs, and registries (TopicRegistry, SubscriptionConfig).
3. **Value types over pointers** — prefer value types for structs, configs, and return values. Use `(T, bool)` to signal absence instead of `*T`. Pointers only when mutation or shared ownership is needed.
4. **Errors for failures, not control flow** — reserve `error` returns for unexpected or infrastructure failures. Use result types (structs, bools) for expected outcomes like `(Result, error)` or `(T, bool)`. Avoid sentinel errors that represent non-failure states.

### Error Classification (`platform/errs`)

Errors are classified by origin (user vs infra) and retryability. The framework lives in `platform/errs/`. See [platform/errs/README.md](platform/errs/README.md) for full details.

**Key rules:**
1. **Non-retryable by default** — a plain `fmt.Errorf(...)` is non-retryable. Retryability is opted into explicitly, but that decision is almost always made by a classifier, not a controller (see rule 4).
2. **Infra by default** — any error not wrapped with `NewUserError` is infra. There is no `NewInfraError`.
3. **Extensions return plain errors** — extension interfaces (`MergeChecker`, `Storage`, `Publisher`) return standard `error` values with their own domain sentinels (e.g. `storage.ErrNotFound`). They do NOT classify errors as user or infra.
4. **Classifiers do the bulk of classification; controllers override only with knowledge a classifier lacks** — primary pipeline consumers compose per-backend classifiers into `errs.NewClassifierProcessor(...)`; the processor runs once per chain in the consumer and decides retryability from the raw error. So the common case is a controller returning the raw error (`fmt.Errorf("...: %w", err)`) and letting the classifier verdict stand. Reserve an explicit `errs.New*Error` wrap for the rare case where the controller knows something the classifier cannot infer from the error value alone (e.g. `storage.ErrNotFound` meaning "user asked for a missing resource" *in this call site*). Do **not** wrap a failure as retryable just because replaying it is convenient (e.g. a failed queue publish) — that turns permanent failures into infinite retries instead of dead-lettering. DLQ reconciliation consumers use `errs.AlwaysRetryableProcessor` instead. See [platform/errs/README.md](platform/errs/README.md).
5. **Error chain works end-to-end** — extensions wrap custom errors, controllers wrap with `errs.New*Error`, and `errors.Is`/`errors.As` walks the full chain.
