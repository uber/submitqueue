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

### Services

Three services, each following the same layout:

- **Gateway** (port 8081): Entry point for external requests
- **Orchestrator** (port 8082): Coordinates job execution
- **Speculator** (port 8083): Performs speculative builds

```
<service>/
├── controller/          # Business logic (pure, transport-agnostic)
│   ├── {method}.go      # RPC controllers (e.g., land.go, ping.go)
│   ├── {method}_test.go
│   └── {step}/          # Queue message controllers (e.g., request/)
│       ├── {step}.go    # Step in workflow
│       └── {step}_test.go
├── proto/               # Proto definitions (.proto files)
├── protopb/             # Generated proto code (committed to repo)
└── integration_test/
```

### Controllers

Controllers contain pure business logic, independent of infrastructure. There are two types:

**RPC Controllers** - Handle synchronous API requests in `{service}/controller/`. Accept protobuf types, independent of gRPC/YARPC transport.

```go
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (*pb.LandResponse, error)
```

**Queue Message Controllers** - Process async queue messages in `{service}/controller/{step}/`. Implement `consumer.Controller` interface.

```go
// Receives consumer.Delivery (NOT extension/queue.Delivery)
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
    // Return nil to ack, error to nack. Consumer handles ack/nack automatically.
}
```

Controllers receive `consumer.Delivery` (subset interface without Ack/Nack methods) to enforce separation: controllers do business logic, consumer framework handles infrastructure.

### Entities

Domain objects in `entity/`, organized by domain. Top-level entities live directly in `entity/`; domain-specific ones go in subdirectories.

```
entity/
├── request.go           # Request, Change, enums (RequestState, RequestLandStrategy)
└── queue/
    └── message.go       # Message entity
```

**Entity guidelines:**
1. Keep entities pure and framework-agnostic — no external dependencies
2. Use value types, not references
3. Prefer `int64` milliseconds over `time.Time` and `time.Duration`:
   - Timestamps: Unix epoch milliseconds (e.g., `CreatedAt int64`) — use `time.UnixMilli()` method
   - Durations/timeouts: milliseconds (e.g., `TimeoutMs int64`, `DelayMs int64`)
   - Use `time.Duration(ms) * time.Millisecond` to convert to `time.Duration` when needed
4. Every field must have a comment explaining its meaning
5. Reference other entities by ID (string or int), not directly
6. Use string enums with clear names; assign sentinel values (`""` for strings, `0` for ints) to unreachable/unknown enum variants

### Extensions

Extensions are **vendor-agnostic, pluggable interfaces** for backend implementations. Each defines interfaces at the top level with implementations in subdirectories.

```
extension/
├── counter/             # Atomic sequential number generation
│   ├── counter.go       # Counter interface
│   └── mysql/           # MySQL implementation
├── queue/               # Messaging queue abstraction
│   ├── queue.go         # Queue (factory) interface
│   ├── publisher.go     # Publisher interface
│   ├── subscriber.go    # Subscriber interface
│   ├── delivery.go      # Delivery interface
│   └── sql/             # SQL (MySQL) implementation
└── storage/             # Storage abstraction
    ├── storage.go       # Storage (factory) interface + sentinel errors
    ├── request_store.go # RequestStore interface
    └── mysql/           # MySQL implementation
```

**Extension pattern:**
1. Define vendor-agnostic interfaces at `extension/{ext}/`
2. Implementations go in `extension/{ext}/{impl}/`
3. Most extensions use a Factory interface for dependency injection and lifecycle management
4. Include a README.md documenting interfaces and usage

### Import Paths

- RPC Controllers: `github.com/uber/submitqueue/{service}/controller`
- Queue Controllers: `github.com/uber/submitqueue/{service}/controller/{step}`
- Consumer: `github.com/uber/submitqueue/consumer`
- Proto (generated): `github.com/uber/submitqueue/{service}/protopb`
- Extensions: `github.com/uber/submitqueue/extension/{extension}`
- Extension impl: `github.com/uber/submitqueue/extension/{extension}/{impl}`
- Entities: `github.com/uber/submitqueue/entity/{domain}`

## Development

### Directory Structure

```
submitqueue/
├── MODULE.bazel              # Bzlmod dependencies
├── go.mod                    # Go module dependencies
├── BUILD.bazel               # Root build configuration
├── Makefile                  # Build automation
├── .bazelversion             # Pinned Bazel version
├── .envrc                    # direnv configuration
├── tool/bazel                # Bazelisk wrapper
├── gateway/                  # Gateway service
├── orchestrator/             # Orchestrator service
├── speculator/               # Speculator service
├── extension/                # Pluggable backend implementations
├── entity/                   # Domain entities
├── example/                  # Server and client examples
│   ├── server/{service}/
│   └── client/{service}/
├── e2e_test/                 # Cross-service hermetic tests (Testcontainers)
├── doc/                      # Documentation
└── bin/                      # Compiled binaries (gitignored)
```

### Build System

This repository uses **Bazel with Bzlmod** (NOT WORKSPACE) for dependency management.

- **Version pinning**: `.bazelversion` pins the Bazel version
- **Dependencies**: Managed in `MODULE.bazel` (NOT a WORKSPACE file)
- **Go version**: Defined in `go.mod`, read by `MODULE.bazel` via `go_sdk.from_file()`
- **Bazel wrapper**: `./tool/bazel` (Bazelisk wrapper). With direnv (`.envrc`), use `bazel` directly.
- **External dependencies**: Must be added to both `go.mod` AND `MODULE.bazel`
- **BUILD files**: Every Go package must have a `BUILD.bazel` file

### Proto Generation

All generated proto files are **committed to the repository**. When modifying `.proto` files:

1. Edit the `.proto` file in `{service}/proto/`
2. Run `make proto` to regenerate all three file types: `*.pb.go`, `*_grpc.pb.go`, `*.pb.yarpc.go`
3. Update controller implementations if needed
4. Commit all generated files

### File Naming

- Proto files: `{service}.proto`
- Controllers: `{method}.go` or `{feature}.go`
- Entities: `{entity}.go`
- Tests: `{file}_test.go`
- BUILD files: Always `BUILD.bazel`

### Directory Naming

- Use **singular** names for directories (e.g., `mock/` not `mocks/`, `entity/` not `entities/`)
- This applies to all folders including test mocks, extensions, entities, and service directories

### Common Make Targets

```bash
make build                    # Build all services
make proto                    # Regenerate proto files
make test                     # Run unit tests
make integration-test         # Run service integration tests
make e2e-test                 # Run hermetic tests with Testcontainers
make run-gateway              # Run gateway (port 8081)
make run-orchestrator         # Run orchestrator (port 8082)
make run-speculator           # Run speculator (port 8083)
make run-client-gateway       # Run gateway client
make gazelle                  # Update BUILD.bazel files
make clean                    # Remove binaries and Bazel cache
make clean-proto              # Remove generated proto files
```

### Common Workflows

**Add new RPC method:**
1. Edit `{service}/proto/*.proto`
2. `make proto`
3. Add controller in `{service}/controller/`
4. Wire up in `example/server/{service}/main.go`

**Add new queue message controller:**
1. Create `{service}/controller/{step}/` with controller implementing `consumer.Controller`
2. Wire up in `example/server/{service}/main.go`: register → start → stop on shutdown

**Add new extension implementation:**
1. Create `extension/{extension}/{impl}/` directory
2. Implement factory and core interfaces
3. Add `BUILD.bazel`
4. Add tests and document in README.md

**Add new entity:**
1. Create `entity/{domain}/{entity}.go` with test file
2. Add `BUILD.bazel` with `go_library` and `go_test` targets

### Testing Guidelines

1. **Avoid asserting on error messages** — assert on error type if it is part of the contract, or assert generic error otherwise.
2. **Avoid blocking operations for synchronization** — do not use `time.Sleep`. Design the tested routine to signal back (channels, callbacks, condition variables).
3. **Use testify assertions** — use `stretchr/assert` or `require` instead of `t.Fatal()`.

### Code Style Guidelines

1. **Use SugaredLogger for structured logging** — always use `zap.SugaredLogger` with structured logging methods:
   - `logger.Debugw(msg, key1, val1, key2, val2, ...)` for debug logs
   - `logger.Infow(msg, key1, val1, key2, val2, ...)` for info logs
   - `logger.Errorw(msg, key1, val1, key2, val2, ...)` for error logs
   - Never use unstructured methods like `Debug()`, `Info()`, `Error()`, or `Printf()`
   - Example: `logger.Infow("starting consumer", "subscriber_name", subscriberName, "controller_count", len(controllers))`

2. **Use interfaces for contracts** — define interfaces for public APIs and dependencies:
   - Public components should return/accept interfaces, not concrete structs
   - Unexported structs implement the interfaces
   - Makes testing easier through mocking
   - Example: `func New(...) Consumer` returns interface, not `*consumer`
   - Implementation struct is unexported: `type consumer struct { ... }`

