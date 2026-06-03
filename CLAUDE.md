# SubmitQueue Repository Guide for Claude

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
├── core/                       # SHARED cross-domain infrastructure (errs, httpclient, metrics) — no domain deps
├── entity/                     # SHARED domain entities
│   └── queue/                  # Queue-specific entities (Message)
├── extension/                  # SHARED extensions
│   └── queue/                  # Messaging queue abstraction (interface + mysql/)
├── submitqueue/                # SubmitQueue domain
│   ├── gateway/                # Gateway service (port 8081) - entry point
│   ├── orchestrator/           # Orchestrator service (port 8082) - coordinates jobs
│   ├── entity/                 # SubmitQueue-specific domain entities
│   ├── extension/              # SubmitQueue-specific extension impls (storage, counter, mergechecker, ...)
│   └── core/                   # SubmitQueue-internal shared infra (consumer, request)
├── stovepipe/                  # Stovepipe domain
│   ├── gateway/                # Gateway service: commit deployment verification entry point
│   ├── orchestrator/           # Orchestrator service: commit verification pipeline
│   ├── entity/                 # Stovepipe-specific domain entities
│   ├── extension/              # Stovepipe-specific extension impls
│   └── core/                   # Stovepipe-internal shared infra (placeholder; mirrors submitqueue/core)
├── tool/                       # Development and CI tooling
├── example/
│   ├── submitqueue/            # Runnable SubmitQueue servers/clients + Docker Compose
│   └── stovepipe/              # Runnable Stovepipe servers/clients
├── test/
│   ├── e2e/submitqueue/        # End-to-end tests (full stack)
│   ├── integration/            # Integration tests (core/, submitqueue/, stovepipe/)
│   └── testutil/               # Test utilities (ComposeStack, MySQL helpers)
└── doc/                        # Documentation
```

The repo hosts shared building blocks at the top level — cross-domain infrastructure in `core/`, shared entities in `entity/`, shared extensions in `extension/` — followed by one folder per **domain** (`submitqueue/`, `stovepipe/`). Each domain owns the same internal layout (`gateway/`, `orchestrator/`, `entity/`, `extension/`, `core/`); a domain's own `core/` (e.g. `submitqueue/core/`) holds infra shared only between that domain's services.

### Services

Each service follows the same layout:

```
<service>/
├── controller/          # Business logic (pure, transport-agnostic)
│   ├── {method}.go      # RPC controllers (e.g., land.go, ping.go)
│   ├── {method}_test.go
│   └── {step}/          # Queue message controllers (e.g., request/)
│       ├── {step}.go
│       └── {step}_test.go
├── proto/               # Proto definitions (.proto files)
└── protopb/             # Generated proto code (committed to repo)
```

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

### Entities

Domain objects in `entity/`, organized by domain. Guidelines:
1. Pure and framework-agnostic — no external dependencies
2. Value types, not references
3. `int64` milliseconds for timestamps (`CreatedAt int64`) and durations (`TimeoutMs int64`)
4. Every field must have a comment
5. Reference other entities by ID (string or int), not directly
6. String enums with sentinel values (`""` for unknown)

### Extensions

Vendor-agnostic, pluggable interfaces with implementations in subdirectories:
1. Define interfaces at `extension/{ext}/`
2. Implementations at `extension/{ext}/{impl}/`
3. Factory interface for dependency injection and lifecycle management

**Design interfaces for the technology *space*, not the implementation in front of you.** The interface is a contract every backend will have to satisfy — SQL, key-value (DynamoDB, Bigtable), document, message queue, search, RPC, in-memory, mocks. If the contract assumes a capability that some plausible backend can't provide cheaply, you've baked the current impl's strengths into the API.

Common over-constraints to avoid:
- **Batch atomicity** (multi-row inserts as one transaction) — many KV stores can't do this. Prefer single-record primitives + caller loops + idempotency-on-retry.
- **Multi-key queries** (`WHERE x IN (...)`) — fine in SQL, awkward elsewhere. Prefer per-key reads.
- **Server-side filters** (joins, sub-queries, complex predicates) — push filtering and aggregation to the caller; keep the store responsible only for "get/put by key" semantics.
- **Transactions across entities** — virtually no distributed store offers this. Use eventual consistency + idempotency.
- **Strict ordering / exactly-once** in messaging — most queues are at-least-once with best-effort ordering. Make consumers idempotent.
- **Synchronous, low-latency calls** for things that may run remotely — design for retry/backoff and timeouts, not assumed-fast.

The cost of "callers loop over a small batch" is usually negligible. The cost of forcing every future backend to fake a capability the API demanded is permanent.

When in doubt, ask: *"If the next implementation were DynamoDB / Kafka / Bigtable / a remote RPC service / an in-memory map, could it satisfy this signature without contortion?"* If the answer is no, simplify the contract.

### Import Paths

Paths follow the directory layout: shared code is top-level, domain code nests under the domain folder (`submitqueue/`, `stovepipe/`).

- RPC Controllers: `github.com/uber/submitqueue/{domain}/{service}/controller` (e.g. `.../submitqueue/gateway/controller`)
- Queue Controllers: `github.com/uber/submitqueue/{domain}/{service}/controller/{step}`
- Proto (generated): `github.com/uber/submitqueue/{domain}/{service}/protopb`
- Domain entities: `github.com/uber/submitqueue/{domain}/entity` (e.g. `.../submitqueue/entity`)
- Domain extensions: `github.com/uber/submitqueue/{domain}/extension/{ext}[/{impl}]` (e.g. `.../submitqueue/extension/storage/mysql`)
- Domain-internal infra: `github.com/uber/submitqueue/{domain}/core/{pkg}` (e.g. `.../submitqueue/core/consumer`, `.../submitqueue/core/request`)
- Shared entities: `github.com/uber/submitqueue/entity/{name}` (e.g. `.../entity/queue`)
- Shared extensions: `github.com/uber/submitqueue/extension/{name}` (e.g. `.../extension/queue`)
- Cross-domain infra: `github.com/uber/submitqueue/core/{pkg}` (e.g. `.../core/errs`, `.../core/metrics`)

## Development

### Build System

Bazel with Bzlmod (NOT WORKSPACE).

- **Dependencies**: `MODULE.bazel` + `go.mod` (both must be updated)
- **Bazel wrapper**: `./tool/bazel` (Bazelisk). With direnv (`.envrc`), use `bazel` directly.
- **BUILD files**: Every Go package needs `BUILD.bazel`. Run `make gazelle` after adding/removing Go files.
- **CI enforces** BUILD files are in sync — always run `make gazelle` before committing.

### Proto Generation

Generated proto files are committed. When modifying `.proto` files:
1. Edit in `{domain}/{service}/proto/` (e.g. `submitqueue/gateway/proto/`)
2. `make proto` (generates `*.pb.go`, `*_grpc.pb.go`, `*.pb.yarpc.go`)
3. Commit all generated files

### Naming Conventions

- **Directories**: singular (`mock/`, `entity/`, not `mocks/`, `entities/`)
- **Files**: `{method}.go`, `{entity}.go`, `{file}_test.go`, `BUILD.bazel`
- **Proto files**: `{service}.proto`
- **Test compose contexts**: the `testContext` passed to `NewComposeStack` (and thus the `sq-test-{context}-…` Docker project/container names) must be **domain-qualified** — `{category}-{domain}-{name}` where `{category}` is `svc`/`ext`/`core`/`e2e` and `{domain}` is `submitqueue`/`stovepipe`/… (omit the domain only for shared/cross-domain suites, e.g. `ext-queue-sql`). This keeps containers unambiguous and lets suites run in parallel. See [doc/howto/TESTING.md](doc/howto/TESTING.md#container-naming).
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
1. Edit `{domain}/{service}/proto/*.proto` → `make proto`
2. Add controller in `{domain}/{service}/controller/`
3. Wire up in `example/{domain}/{service}/server/main.go`

**Add new queue message controller:**
1. Create `{domain}/{service}/controller/{step}/` implementing `consumer.Controller`
2. Wire up in `example/{domain}/{service}/server/main.go`

**Add new extension:**
1. Create the extension under `{domain}/extension/{ext}/{impl}/` (domain-specific, e.g. `submitqueue/extension/...`) or top-level `extension/{ext}/{impl}/` (shared across domains) with factory and interfaces
2. Add `BUILD.bazel`, tests, and README.md

**Add new entity:**
1. Create `{domain}/entity/{entity}.go` (domain-specific) or top-level `entity/{name}/{entity}.go` (shared) with test file and `BUILD.bazel`

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

For inline mocks (mock in the same package, e.g., `extension/queue/mysql/mock_stores.go`):

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

- **Table-driven tests** — prefer table-driven tests with `t.Run` subtests over individual test functions.
- **Avoid asserting on error messages** — assert on error type or check the error with `require.Error`, do not `assert.Contains(t, err.Error(), message)`
- **No change detector tests** — don't assert on default values, internal structure, or implementation details that can change without affecting behavior. Test what the code *does*, not how it's constructed.
- **No `time.Sleep` for synchronization** — use channels, callbacks, condition variables.
- **Use testify** — `assert`/`require` instead of `t.Fatal()`.

**Integration tests** use Docker Compose via `testutil.ComposeStack`:
- Package naming: folder name as package (NOT `*_test` suffix)
- Bazel: add `tags = ["integration"]` and `data = [...]` for compose/schema files
- Use `testutil.NewComposeStack()` with meaningful context (e.g., `"ext-storage-mysql"`)

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for full testing guide.

### CI and Validation

CI runs on every PR and enforces all checks via a `required-checks` gate. **Before committing, validate locally:**

1. `make fmt` — format Go and YAML code (CI will reject unformatted code)
2. `make lint` — run all linters (formatting check)
3. `make check-tidy` — ensure `go.mod` and `MODULE.bazel` are tidy
4. `make check-gazelle` — ensure `BUILD.bazel` files are up to date

### Code Style

1. **Structured logging** — `zap.SugaredLogger` with `Debugw`/`Infow`/`Errorw(msg, key, val, ...)`. Never unstructured methods.
2. **Interfaces for behavior, structs for data** — use interfaces for behavioral contracts (Consumer, Controller, Storage). Use structs for data containers, configs, and registries (TopicRegistry, SubscriptionConfig).
3. **Value types over pointers** — prefer value types for structs, configs, and return values. Use `(T, bool)` to signal absence instead of `*T`. Pointers only when mutation or shared ownership is needed.
4. **Errors for failures, not control flow** — reserve `error` returns for unexpected or infrastructure failures. Use result types (structs, bools) for expected outcomes like `(Result, error)` or `(T, bool)`. Avoid sentinel errors that represent non-failure states.

### Error Classification (`core/errs`)

Errors are classified by origin (user vs infra) and retryability. The framework lives in `core/errs/`. See [core/errs/README.md](core/errs/README.md) for full details.

**Key rules:**
1. **Non-retryable by default** — a plain `fmt.Errorf(...)` is non-retryable. Wrap with `errs.NewRetryableError(...)` to opt in to retry.
2. **Infra by default** — any error not wrapped with `NewUserError` is infra. There is no `NewInfraError`.
3. **Extensions return plain errors** — extension interfaces (`MergeChecker`, `Storage`, `Publisher`) return standard `error` values with their own domain sentinels (e.g. `storage.ErrNotFound`). They do NOT classify errors as user or infra.
4. **Controllers classify errors** — the service controller that calls an extension decides whether the failure is user-caused or infrastructure-caused. The same extension error may be classified differently depending on context.
5. **Error chain works end-to-end** — extensions wrap custom errors, controllers wrap with `errs.New*Error`, and `errors.Is`/`errors.As` walks the full chain.
