# SubmitQueue Repository Guide for Claude

## Overview

SubmitQueue is a distributed system for managing code submission workflows. The project follows clean architecture principles with three main services:

- **Gateway** (port 8081): Entry point for external requests
- **Orchestrator** (port 8082): Coordinates job execution
- **Speculator** (port 8083): Performs speculative builds

## Build System

### Bazel with Bzlmod

This repository uses **Bazel 8.4.1** with **Bzlmod** (NOT WORKSPACE) for dependency management.

- **Version pinning**: `.bazelversion` pins Bazel to 8.4.1
- **Dependencies**: Managed in `MODULE.bazel` (NOT in a WORKSPACE file)
- **Go version**: 1.24.5 (defined in MODULE.bazel)
- **Bazel wrapper**: `./tools/bazel` (Python-based Bazelisk wrapper)
- **direnv**: When enabled via `.envrc`, use `bazel` directly; otherwise use `./tools/bazel`

### Key Bazel Commands

```bash
# Build everything
bazel build //...

# Build specific service
bazel build //gateway/protopb
bazel build //examples/server/gateway:gateway

# Run a service
bazel run //examples/server/gateway:gateway

# Run tests
bazel test //...
```

### Go Module

The project has both `go.mod` (for Go dependencies) and `MODULE.bazel` (for Bazel). Dependencies are:
- **YARPC**: Uber's RPC framework
- **gRPC**: Standard gRPC
- **Zap**: Structured logging
- **Tally**: Metrics collection
- **Protobuf**: Protocol buffers

## Architecture

### Core Principles

**Immutability and Eventual Consistency:**

When working with databases and distributed systems, follow these principles:

1. **Immutable Entities**: Prefer immutable data structures. Once created, entities should not be modified in place. Instead, create new versions with updated fields.

2. **Eventual Consistency**: Design for eventual consistency rather than strong consistency. Services should handle:
   - Stale reads gracefully
   - Idempotent operations (safe to retry)
   - Convergence over time

3. **Event Sourcing Pattern**: For critical state changes:
   - Store events (what happened) rather than just current state
   - Derive current state from event history
   - Enables audit trails and replay capabilities

4. **Database Operations**:
   - Use optimistic locking (version numbers) instead of pessimistic locks
   - Design schemas for append-only patterns where possible
   - Avoid in-place updates; prefer creating new records and marking old ones as superseded
   - Handle concurrent modifications with conflict resolution strategies

5. **Idempotency Keys**: For operations that modify state:
   - Include unique request IDs
   - Check for duplicate requests before executing
   - Return same result for repeated requests with same ID

**Example:**
```go
// Immutable entity pattern
type Request struct {
    ID        string
    Version   int       // For optimistic locking
    Status    Status
    CreatedAt time.Time
    UpdatedAt time.Time
}

// Instead of mutating, create new version
func (r Request) WithStatus(status Status) Request {
    return Request{
        ID:        r.ID,
        Version:   r.Version + 1,
        Status:    status,
        CreatedAt: r.CreatedAt,
        UpdatedAt: time.Now(),
    }
}
```

### Service Structure

Each service follows the same layout:

```
<service>/
├── controller/          # Business logic (pure, transport-agnostic)
│   ├── BUILD.bazel
│   └── *.go            # Controller implementations
├── proto/              # Proto definitions (.proto files)
│   ├── BUILD.bazel
│   └── *.proto
├── protopb/            # Generated proto code (committed to repo)
│   ├── BUILD.bazel
│   ├── *.pb.go         # Standard protobuf
│   ├── *_grpc.pb.go    # gRPC service code
│   └── *.pb.yarpc.go   # YARPC service code
└── integration_tests/  # Integration tests
```

**Key principle**: Controllers contain pure business logic and are independent of transport layer (gRPC/YARPC).

### Proto File Generation

All generated proto files are **committed to the repository**. When modifying `.proto` files:

1. Edit the `.proto` file
2. Run `make proto` to regenerate all three file types:
   - `*.pb.go` (protobuf code)
   - `*_grpc.pb.go` (gRPC service code)
   - `*.pb.yarpc.go` (YARPC service code)
3. Update controller implementations if needed
4. Commit all generated files

## Extension System

Extensions are **vendor-agnostic, pluggable interfaces** for different backend implementations. This is a core architectural pattern in the repository.

### Current Extensions

```
extensions/
├── queue/              # Messaging queue abstraction
│   ├── queue.go        # Factory interface
│   ├── publisher.go    # Publisher interface
│   ├── subscriber.go   # Subscriber interface
│   └── README.md       # Documentation
└── storage/            # Storage abstraction
    ├── factory.go      # Factory interface
    ├── request_store.go # RequestStore interface
    └── mysql/          # MySQL implementation
        ├── factory.go
        └── request_store.go
```

### Extension Interface Pattern

Each extension typically defines:
1. **Factory** interface for creating instances (usually needed, but not always required for simple extensions)
2. **Core interfaces** for the functionality (e.g., Publisher, Subscriber, RequestStore)
3. **Implementation directories** under `extensions/{extension}/{impl}/`

**Note on Factories:** Most extensions benefit from a Factory pattern for dependency injection and lifecycle management. However, simpler extensions with straightforward initialization may not require a separate factory interface.

### Adding New Extension Implementations

When implementing a new backend for an existing extension:

**Structure:**
```
extensions/{extension}/{impl}/
├── BUILD.bazel
├── factory.go          # Implements Factory interface
└── {interface}.go      # Implements core interfaces
```

**Examples:**
- Queue implementations: `extensions/queue/sql/`, `extensions/queue/kafka/`
- Storage implementations: `extensions/storage/postgres/`, `extensions/storage/cassandra/`

**Steps:**
1. Create `extensions/{extension}/{impl}/` directory
2. Implement the Factory interface from `extensions/{extension}/`
3. Implement all required interfaces (Publisher/Subscriber for queue, RequestStore for storage)
4. Add BUILD.bazel with appropriate go_library target
5. Map domain entities to/from backend format
6. Wire up lifecycle methods (Close, Ack/Nack, etc.)

### Adding New Extension Types

When adding a completely new extension category (e.g., cache, auth, etc.):

**Structure:**
```
extensions/{new_extension}/
├── BUILD.bazel
├── README.md           # Document the interfaces and usage
├── factory.go          # Factory interface
├── {interface}.go      # Core interfaces
└── {first_impl}/       # First implementation
    ├── BUILD.bazel
    ├── factory.go
    └── {interface}.go
```

**Pattern to follow:**
1. Define vendor-agnostic interfaces at `extensions/{new_extension}/`
2. Document interfaces and usage patterns in README.md
3. Create first implementation under `extensions/{new_extension}/{impl}/`
4. Consider whether a Factory pattern is needed for dependency injection and lifecycle management

## Entities

Entities are domain objects used across the project. They live in the `entities/` directory, organized by domain:

**Note:** Entities are organized hierarchically by domain (queue, storage, workflow, etc.) to maintain clear boundaries and separation of concerns. This organization helps:
- Group related entities together
- Make dependencies explicit
- Scale as the number of entities grows
- Maintain domain-driven design principles

```
entities/
├── queue/              # Queue domain entities
│   ├── BUILD.bazel
│   ├── message.go      # Message entity
│   ├── message_test.go
│   ├── delivery.go     # Delivery entity
│   └── delivery_test.go
└── storage/            # Storage domain entities
    ├── BUILD.bazel
    └── land_request.go # LandRequest entity
```

### Adding New Entities

When adding new domain entities:

**Structure:**
```
entities/{domain}/
├── BUILD.bazel
├── {entity}.go
└── {entity}_test.go
```

**Guidelines:**
1. Group entities by domain (queue, storage, workflow, etc.)
2. Keep entities pure and framework-agnostic
3. Add corresponding tests
4. Update BUILD.bazel with go_library and go_test targets
5. Import entities using: `github.com/uber/submitqueue/entities/{domain}`

**Examples:**
- Queue entities: `entities/queue/message.go`, `entities/queue/delivery.go`
- Storage entities: `entities/storage/land_request.go`
- New workflow entity: `entities/workflow/job.go`

## Directory Structure

```
submitqueue/
├── MODULE.bazel              # Bzlmod dependencies
├── go.mod                    # Go module dependencies
├── BUILD.bazel              # Root build configuration
├── Makefile                 # Build automation
├── .bazelversion            # Bazel version (8.4.1)
├── .envrc                   # direnv configuration
│
├── tools/                   # Bazel tooling
│   └── bazel               # Bazelisk wrapper
│
├── gateway/                # Gateway service
│   ├── controller/         # Business logic
│   ├── proto/              # Proto definitions
│   ├── protopb/            # Generated proto code
│   └── integration_tests/
│
├── orchestrator/           # Orchestrator service
│   ├── controller/
│   ├── proto/
│   ├── protopb/
│   └── integration_tests/
│
├── speculator/             # Speculator service
│   ├── controller/
│   ├── proto/
│   ├── protopb/
│   └── integration_tests/
│
├── extensions/             # Pluggable backend implementations
│   ├── queue/              # Queue abstraction
│   │   └── {impl}/         # Implementation (sql, kafka, etc.)
│   └── storage/            # Storage abstraction
│       └── {impl}/         # Implementation (mysql, postgres, etc.)
│
├── entities/               # Domain entities
│   ├── queue/              # Queue entities
│   └── storage/            # Storage entities
│
├── examples/               # Example implementations
│   ├── server/             # Server examples
│   │   ├── gateway/
│   │   ├── orchestrator/
│   │   └── speculator/
│   └── client/             # Client examples
│       ├── gateway/
│       ├── orchestrator/
│       └── speculator/
│
├── integration_tests/      # Cross-service integration tests
├── docs/                   # Documentation
│   ├── architecture/       # Architecture docs
│   └── designs/            # Design documents
└── bin/                    # Compiled binaries (gitignored)
```

## Development Workflow

### Making Changes

**1. Modifying Proto Files:**
```bash
# Edit proto file
vim gateway/proto/gateway.proto

# Regenerate proto code
make proto

# Update controller implementation
vim gateway/controller/*.go

# Rebuild
make build
```

**2. Adding New RPC Method:**
1. Update proto file with new method
2. Run `make proto`
3. Create controller in `{service}/controller/`
4. Wire up controller in `examples/server/{service}/main.go`
5. Test with client

**3. Adding New Extension Implementation:**
1. Create `extensions/{extension}/{impl}/` directory
2. Implement Factory and core interfaces
3. Add BUILD.bazel
4. Add tests
5. Document in extension's README.md

**4. Adding New Entity:**
1. Create `entities/{domain}/{entity}.go`
2. Add corresponding test file
3. Update BUILD.bazel
4. Use entity in extensions/controllers as needed

### Testing

```bash
# Build everything
make build

# Run a service
make run-gateway

# Test with client (in another terminal)
make run-client-gateway MESSAGE="hello"

# Use grpcurl for manual testing
grpcurl -plaintext -d '{"message": "hello"}' \
  localhost:8081 uber.devexp.submitqueue.gateway.SubmitQueueGateway/Ping
```

### Common Make Targets

```bash
make build              # Build all services
make proto              # Regenerate proto files
make run-gateway        # Run gateway service
make run-orchestrator   # Run orchestrator service
make run-speculator     # Run speculator service
make clean              # Remove binaries
make clean-proto        # Remove generated proto files
make test               # Run tests
```

## Key Conventions

### Import Paths

- Controllers: `github.com/uber/submitqueue/{service}/controller`
- Proto (generated): `github.com/uber/submitqueue/{service}/protopb`
- Extensions: `github.com/uber/submitqueue/extensions/{extension}`
- Extension impl: `github.com/uber/submitqueue/extensions/{extension}/{impl}`
- Entities: `github.com/uber/submitqueue/entities/{domain}`

### Code Organization

1. **Separation of Concerns**: Controllers are pure business logic, independent of transport
2. **Interface-Driven**: Extensions define interfaces, implementations live in subdirectories
3. **Generated Code Committed**: All proto-generated files are committed to repo
4. **Build Files**: Every Go package has a BUILD.bazel file
5. **Testing**: Each package should have corresponding tests

### Dependencies

- **External dependencies**: Add to both `go.mod` AND `MODULE.bazel`
- **Internal dependencies**: Reference via import paths and Bazel deps
- **Proto dependencies**: Defined in BUILD.bazel files

### File Naming

- Proto files: `{service}.proto`
- Controllers: `{method}.go` or `{feature}.go`
- Entities: `{entity}.go`
- Tests: `{file}_test.go`
- BUILD files: Always `BUILD.bazel`

## Important Notes

1. **Never use WORKSPACE**: This repo uses Bzlmod exclusively
2. **Commit generated files**: All `*pb.go` files are committed
3. **Use interfaces for extensions**: Keep implementations swappable
4. **Factory pattern**: Most extensions use Factory interface for dependency injection, though simple extensions may not require it
5. **Keep entities pure**: No framework dependencies in entity types
6. **Test coverage**: Add tests for new functionality
7. **Update BUILD.bazel**: When adding new Go files, update BUILD.bazel

## Quick Reference

**Add new service method:**
1. Edit `{service}/proto/*.proto`
2. Run `make proto`
3. Add controller in `{service}/controller/`
4. Wire up in `examples/server/{service}/main.go`

**Add new extension implementation:**
1. Create `extensions/{extension}/{impl}/`
2. Implement Factory and interfaces
3. Add BUILD.bazel
4. Document usage

**Add new entity:**
1. Create `entities/{domain}/{entity}.go`
2. Add test file
3. Update BUILD.bazel

**Run/test locally:**
1. `make run-{service}` to start service
2. `make run-client-{service}` to test
3. Or use `grpcurl` for ad-hoc testing
