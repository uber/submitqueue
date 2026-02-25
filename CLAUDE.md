# SubmitQueue Repository Guide for Claude

## Key Concepts

SubmitQueue is a distributed system for managing code submission workflows. It follows clean architecture with interface-driven extensibility.

**Immutability and Eventual Consistency:**

1. **Immutable entities** — once created, don't modify in place. Create new versions with updated fields.
2. **Eventual consistency** — handle stale reads, idempotent operations, and convergence over time.
3. **Event sourcing** — store events (what happened) rather than just current state for critical changes.
4. **Optimistic locking** — use version numbers instead of pessimistic locks. Avoid transactions; prefer optimistic concurrency and retries.
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

// Instead of mutating, create new version
func (r Request) WithStatus(status Status) Request {
    return Request{
        ID:        r.ID,
        Version:   r.Version + 1,
        Status:    status,
        CreatedAt: r.CreatedAt,
        UpdatedAt: time.Now().UnixMilli(),
    }
}
```

## Architecture

### Project Layout

```
submitqueue/
├── gateway/                    # Gateway service (port 8081) - entry point
├── orchestrator/               # Orchestrator service (port 8082) - coordinates jobs
├── entity/                     # Domain entities (Request, Change, enums)
│   └── queue/                  # Queue-specific entities (Message)
├── extension/                  # Pluggable backend implementations
│   ├── counter/                # Sequential number generation (interface + mysql/)
│   ├── queue/                  # Messaging queue abstraction (interface + sql/)
│   └── storage/                # Storage abstraction (interface + mysql/)
├── core/                        # Shared infrastructure packages reused across services
│   └── consumer/               # Queue consumption framework (lifecycle, ack/nack, routing)
├── example/server/             # Runnable servers with Docker Compose
├── test/
│   ├── e2e/                    # End-to-end tests (full stack)
│   ├── integration/            # Integration tests (per-service + extensions)
│   └── testutil/               # Test utilities (ComposeStack, MySQL helpers)
└── doc/                        # Documentation
```

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

### Import Paths

- RPC Controllers: `github.com/uber/submitqueue/{service}/controller`
- Queue Controllers: `github.com/uber/submitqueue/{service}/controller/{step}`
- Consumer: `github.com/uber/submitqueue/core/consumer`
- Proto (generated): `github.com/uber/submitqueue/{service}/protopb`
- Extensions: `github.com/uber/submitqueue/extension/{extension}`
- Extension impl: `github.com/uber/submitqueue/extension/{extension}/{impl}`
- Entities: `github.com/uber/submitqueue/entity/{domain}`

## Development

### Build System

Bazel with Bzlmod (NOT WORKSPACE).

- **Dependencies**: `MODULE.bazel` + `go.mod` (both must be updated)
- **Bazel wrapper**: `./tool/bazel` (Bazelisk). With direnv (`.envrc`), use `bazel` directly.
- **BUILD files**: Every Go package needs `BUILD.bazel`. Run `make gazelle` after adding/removing Go files.
- **CI enforces** BUILD files are in sync — always run `make gazelle` before committing.

### Proto Generation

Generated proto files are committed. When modifying `.proto` files:
1. Edit in `{service}/proto/`
2. `make proto` (generates `*.pb.go`, `*_grpc.pb.go`, `*.pb.yarpc.go`)
3. Commit all generated files

### Naming Conventions

- **Directories**: singular (`mock/`, `entity/`, not `mocks/`, `entities/`)
- **Files**: `{method}.go`, `{entity}.go`, `{file}_test.go`, `BUILD.bazel`
- **Proto files**: `{service}.proto`

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
make integration-test   # Run all integration tests (Docker-based)
make e2e-test           # Run end-to-end tests
make proto              # Regenerate proto files
make gazelle            # Update BUILD.bazel files
make local-start        # Start full stack with Docker Compose
make local-ps           # Show running containers and ports
make local-logs         # View logs from all services
make local-stop         # Stop all services
make clean              # Clean Bazel cache
```

### Common Workflows

**Add new RPC method:**
1. Edit `{service}/proto/*.proto` → `make proto`
2. Add controller in `{service}/controller/`
3. Wire up in `example/server/{service}/main.go`

**Add new queue message controller:**
1. Create `{service}/controller/{step}/` implementing `consumer.Controller`
2. Wire up in `example/server/{service}/main.go`

**Add new extension:**
1. Create `extension/{ext}/{impl}/` with factory and interfaces
2. Add `BUILD.bazel`, tests, and README.md

**Add new entity:**
1. Create `entity/{domain}/{entity}.go` with test file and `BUILD.bazel`

### Testing

- **Avoid asserting on error messages** — assert on error type or generic error.
- **No change detector tests** — don't assert on default values, internal structure, or implementation details that can change without affecting behavior. Test what the code *does*, not how it's constructed.
- **No `time.Sleep` for synchronization** — use channels, callbacks, condition variables.
- **Use testify** — `assert`/`require` instead of `t.Fatal()`.

**Integration tests** use Docker Compose via `testutil.ComposeStack`:
- Package naming: folder name as package (NOT `*_test` suffix)
- Bazel: add `tags = ["integration"]` and `data = [...]` for compose/schema files
- Use `testutil.NewComposeStack()` with meaningful context (e.g., `"ext-storage-mysql"`)

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for full testing guide.

### Code Style

1. **Structured logging** — `zap.SugaredLogger` with `Debugw`/`Infow`/`Errorw(msg, key, val, ...)`. Never unstructured methods.
2. **Interfaces for behavior, structs for data** — use interfaces for behavioral contracts (Consumer, Controller, Storage). Use structs for data containers, configs, and registries (TopicRegistry, SubscriptionConfig).
3. **Value types over pointers** — prefer value types for structs, configs, and return values. Use `(T, bool)` to signal absence instead of `*T`. Pointers only when mutation or shared ownership is needed.
4. **Errors for failures, not control flow** — reserve `error` returns for unexpected or infrastructure failures. Use result types (structs, bools) for expected outcomes like `(Result, error)` or `(T, bool)`. Avoid sentinel errors that represent non-failure states.
