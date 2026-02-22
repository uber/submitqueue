# Project Structure

This document describes the organization of the SubmitQueue repository.

## Directory Layout

```
submitqueue/
├── .bazelversion               # Pins Bazel version
├── .envrc                      # direnv configuration
├── .docker-bin/                # Linux binaries for Docker (gitignored)
├── MODULE.bazel                # Bzlmod dependency management
├── go.mod                      # Go module dependencies
├── Makefile                    # Build automation
├── BUILD.bazel                 # Root build file
│
├── tool/                       # Bazel tooling (Bazelisk wrapper)
│
├── gateway/                    # Gateway service - entry point for external requests
│   ├── controller/             # Business logic (land, ping)
│   ├── proto/                  # Proto definitions
│   └── protopb/                # Generated proto code (*.pb.go, *_grpc.pb.go, *.pb.yarpc.go)
│
├── orchestrator/               # Orchestrator service - processes requests via queues
│   ├── controller/             # Business logic (request, ping)
│   ├── proto/                  # Proto definitions
│   └── protopb/                # Generated proto code
│
├── entity/                     # Domain entities (Request, Change, enums)
│   └── queue/                  # Queue-specific entities (Message)
│
├── extension/                  # Pluggable backend implementations
│   ├── counter/                # Sequential number generation interface
│   │   └── mysql/              # MySQL implementation + schema
│   │
│   ├── queue/                  # Messaging queue abstraction
│   │   └── sql/                # SQL (MySQL) implementation + schema
│   │
│   └── storage/                # Storage abstraction
│       └── mysql/              # MySQL implementation + schema
│
├── consumer/                   # Reusable queue consumer infrastructure
│                               # (Handler interface, Consumer)
│
├── example/                    # Runnable examples
│   ├── server/                 # Server implementations with Docker Compose
│   │   ├── gateway/            # Gateway server + Dockerfile
│   │   └── orchestrator/       # Orchestrator server + Dockerfile
│   └── client/                 # Client examples (gateway, orchestrator)
│
├── test/                       # All tests
│   ├── e2e/                    # End-to-end tests (full stack)
│   ├── integration/            # Integration tests
│   │   ├── gateway/            # Gateway service tests
│   │   ├── orchestrator/       # Orchestrator service tests
│   │   └── extension/          # Extension implementation tests
│   │       ├── counter/mysql/
│   │       ├── queue/sql/
│   │       └── storage/mysql/
│   └── testutil/               # Test utilities (Docker Compose, MySQL, servers)
│
└── doc/                        # Documentation
    ├── CLAUDE.md               # Development guidelines
    ├── PROJECT_STRUCTURE.md    # This file
    ├── howto/                  # How-to guides
    │   └── TESTING.md          # Testing guide
    └── rfc/                    # Design documents and proposals
```

## Key Design Principles

### 1. Clean Architecture with Interface-Driven Extensions

**Extensions** are pluggable, vendor-agnostic interfaces:
- `extension/{extension}/` - Interface definitions
- `extension/{extension}/{impl}/` - Implementations (e.g., `mysql/`)
- Each extension has its own schema files

**Examples:**
- `extension/storage/` - Storage interface, MySQL implementation
- `extension/queue/` - Queue interface, SQL implementation
- `extension/counter/` - Counter interface, MySQL implementation

### 2. Service Structure

Each service follows a consistent layout:
- `controller/` - Pure business logic (transport-agnostic)
- `proto/` - Proto definitions (`.proto` files)
- `protopb/` - Generated proto code (committed to repo)

**Controllers** contain pure business logic, independent of gRPC/YARPC transport layer.

### 3. Separate `proto/` and `protopb/` Directories

Each service has:
- `proto/` - Contains the `.proto` file(s)
- `protopb/` - Contains all generated files (`.pb.go`, `_grpc.pb.go`, `.pb.yarpc.go`)

This separation makes it clear what is source vs. generated. **All generated files are committed** to the repository.

### 4. YARPC Support

All proto files generate three types of files:
- `*.pb.go` - Standard protobuf code
- `*_grpc.pb.go` - gRPC service code
- `*.pb.yarpc.go` - YARPC service code for Uber's RPC framework

This allows services to support both gRPC and YARPC clients.

### 5. Entity-Driven Design

Domain entities live in `entity/`:
- Pure, framework-agnostic value types
- Use `int64` for timestamps (Unix milliseconds)
- Reference other entities by ID, not directly
- String enums with clear names

### 6. Consumer Infrastructure

The `consumer/` package provides reusable queue consumer infrastructure:
- `Handler` interface - Business logic for processing messages
- `Manager` - Orchestrates multiple consumers across different topics
- Services register handlers and the manager handles subscriptions, polling, ack/nack

### 7. Docker-Based Testing

All integration and e2e tests use Docker Compose:
- Tests in `test/integration/` for services and extensions
- Tests in `test/e2e/` for full stack
- `test/testutil/` provides Docker Compose helpers
- Hermetic, parallel-safe, auto cleanup

### 8. Python-Based Bazel Wrapper

The `tool/bazel` script is a Python implementation of Bazelisk that:
- Reads `.bazelversion` to determine which Bazel version to use
- Downloads and caches the appropriate Bazel binary
- Delegates to the correct version automatically

### 9. Two Separate Databases

SubmitQueue demonstrates proper architectural separation:
- **Application DB** (port 3306) - Business data (requests, counters)
- **Queue DB** (port 3307) - Messaging infrastructure (messages, offsets, leases)

This allows the queue to be swapped for other technologies (Kafka, SQS, etc.) in production.

## Build System

- **Bazel with Bzlmod** (NOT WORKSPACE) for dependency management
- **Version pinning**: `.bazelversion` pins the Bazel version
- **Go version**: Defined in `go.mod`, read by `MODULE.bazel` via `go_sdk.from_file()`
- **External dependencies**: Must be added to both `go.mod` AND `MODULE.bazel`
- **BUILD files**: Every Go package must have a `BUILD.bazel` file

## Services

### Gateway
Entry point for external requests. Receives land requests, stores in DB, publishes to queue.

### Orchestrator
The engine that processes requests through a multi-stage pipeline via queues.

## Testing

See [howto/TESTING.md](howto/TESTING.md) for comprehensive testing guide.

**Quick overview:**
- **Unit tests** - Co-located with code, fast, no Docker
- **Integration tests** - `test/integration/`, Docker-based, hermetic
- **E2E tests** - `test/e2e/`, full stack, Docker-based

All automated tests use Docker Compose with unique container prefixes for parallel execution.

## See Also

- [CLAUDE.md](../CLAUDE.md) - Development guidelines and coding conventions
- [howto/TESTING.md](howto/TESTING.md) - Comprehensive testing documentation
