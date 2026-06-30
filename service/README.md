# Service Wiring

Runnable gRPC servers and clients that wire each domain's controllers and extensions together and run them locally. This is the composition root — the one layer that knows the full queue topology, picks concrete extension implementations, and owns per-queue routing. Controllers, entities, and extensions stay transport- and wiring-agnostic; everything here turns them into a process you can start.

Each domain has its own subdirectory with a dedicated README:

- [`submitqueue/`](submitqueue/README.md) — the multi-service SubmitQueue domain (Gateway + Orchestrator).
- [`stovepipe/`](stovepipe/README.md) — the single-service Stovepipe domain (ingest → process pipeline).
- [`runway/`](runway/README.md) — the single-service Runway landing service (consumes the merge queues).

## Services

| Service | Port | Domain | RPCs | Backing stores |
|---------|------|--------|------|----------------|
| **SubmitQueue Gateway** | 8081 | `submitqueue` | `Ping`, `Land` | MySQL app + queue |
| **SubmitQueue Orchestrator** | 8082 | `submitqueue` | `Ping` (+ consumes 9 pipeline topics) | MySQL app + queue |
| **Stovepipe** | 8083 | `stovepipe` | `Ping`, `Ingest` (+ consumes the process topic) | MySQL storage + queue |
| **Runway** | 8086 | `runway` | `Ping` (+ consumes merge-conflict-check & merge topics) | MySQL queue |

Ports above are the `go run` defaults; under Docker Compose each server listens on `:8080` inside its container and is published on a random ephemeral host port (use `make local-*-ps` / `docker port` to discover it).

## Directory Structure

```
service/
├── submitqueue/
│   ├── docker-compose.yml              # Full stack (Gateway + Orchestrator + 2x MySQL)
│   ├── gateway/
│   │   ├── server/                     # Gateway server entry point + Dockerfile + compose
│   │   │   └── queues.yaml             # Per-queue extension profiles
│   │   └── client/                     # Gateway ping client
│   └── orchestrator/
│       ├── server/                     # Orchestrator server entry point + Dockerfile + compose
│       └── client/                     # Orchestrator ping client
├── stovepipe/
│   ├── docker-compose.yml              # Stovepipe service + storage MySQL + queue MySQL
│   ├── server/                         # Stovepipe gRPC server + Dockerfile
│   └── client/                         # Stovepipe ping client
└── runway/
    ├── server/                         # Runway gRPC server + Dockerfile + compose
    └── client/                         # Runway ping client
```

## Running

### Docker Compose (recommended)

```bash
# Full SubmitQueue stack (Gateway + Orchestrator + MySQL)
make local-submitqueue-start
make local-submitqueue-gateway-start        # gateway-only stack
make local-submitqueue-orchestrator-start   # orchestrator-only stack

# Stovepipe service (gRPC service + storage MySQL + queue MySQL)
make local-stovepipe-start

# Runway service (consumer + queue MySQL)
make local-runway-start

# Logs and status (SubmitQueue)
make local-submitqueue-logs
make local-submitqueue-ps

# Stop everything (SubmitQueue + Stovepipe + Runway)
make local-stop
```

`make local-stop` stops the SubmitQueue, Stovepipe, and Runway stacks; the per-domain `make local-stovepipe-stop` / `make local-runway-stop` targets stop just one. Each `build-*-linux` target copies a distinct Linux binary into `.docker-bin/` so the compose stacks don't clobber each other's artifacts.

### Bazel

```bash
# Servers
bazel build //service/submitqueue/gateway/server:gateway
bazel build //service/submitqueue/orchestrator/server:orchestrator
bazel build //service/stovepipe/server:stovepipe
bazel build //service/runway/server:runway

# Clients
bazel build //service/submitqueue/gateway/client:gateway
bazel build //service/submitqueue/orchestrator/client:orchestrator
bazel build //service/stovepipe/client:stovepipe
bazel build //service/runway/client:runway
```

### Go

```bash
go run ./service/submitqueue/gateway/server
go run ./service/submitqueue/orchestrator/server
go run ./service/stovepipe/server
go run ./service/runway/server
```

## Testing with Clients

```bash
# Go clients
go run ./service/submitqueue/gateway/client -addr localhost:8081 -message "hello"
go run ./service/submitqueue/orchestrator/client -addr localhost:8082 -message "hello"
go run ./service/stovepipe/client -addr localhost:8083 -message "hello"
go run ./service/runway/client -addr localhost:8086 -message "hello"

# Bazel-run clients (honor SERVER_ADDR / MESSAGE)
make run-client-submitqueue-gateway
make run-client-submitqueue-orchestrator
make run-client-stovepipe
make run-client-runway
```

Client flags:
- `-addr`: Server address (default: service-specific port)
- `-message`: Message to send in the ping request
- `-timeout`: Request timeout (default: 5s)

### grpcurl

Install grpcurl if you don't have it:
```bash
brew install grpcurl  # macOS
# or
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

```bash
# Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.submitqueue.gateway.SubmitQueueGateway/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8082 uber.submitqueue.orchestrator.SubmitQueueOrchestrator/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8083 uber.submitqueue.stovepipe.Stovepipe/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8086 uber.runway.Runway/Ping

# List / describe services (reflection is registered on every server)
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext localhost:8081 describe uber.submitqueue.gateway.SubmitQueueGateway
```

## API Reference

### Gateway Service

**Service**: `uber.submitqueue.gateway.SubmitQueueGateway`
**Proto**: `api/submitqueue/gateway/proto/gateway.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check, returns service name and timestamp |
| `Land` | Submit a land request for code changes |

### Orchestrator Service

**Service**: `uber.submitqueue.orchestrator.SubmitQueueOrchestrator`
**Proto**: `api/submitqueue/orchestrator/proto/orchestrator.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check, returns service name and timestamp |

### Stovepipe

**Service**: `uber.submitqueue.stovepipe.Stovepipe`
**Proto**: `api/stovepipe/proto/stovepipe.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check |
| `Ingest` | Ingest a queue's head commit, persist the Request, and publish it to the process stage |

### Runway

**Service**: `uber.runway.Runway`
**Proto**: `api/runway/proto/runway.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check |
</content>
