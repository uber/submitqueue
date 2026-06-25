# Examples

Example gRPC servers and clients for running the submitqueue services locally.

## Services

- **SubmitQueue Gateway** (port 8081) — entry point for land requests. Exposes `Ping` and `Land` RPCs.
- **SubmitQueue Orchestrator** (port 8082) — coordinates the pipeline. Exposes `Ping` RPC and consumes queue messages across 9 pipeline topics.
- **Stovepipe** (port 8083) - single Ping-only service. Exposes `Ping` RPC.

Services require MySQL (app database + queue database). Docker Compose handles this automatically.

## Directory Structure

```
example/
├── submitqueue/
│   ├── docker-compose.yml          # Full stack (Gateway + Orchestrator + 2x MySQL)
│   ├── gateway/
│   │   ├── server/
│   │   │   ├── main.go             # Gateway server entry point
│   │   │   ├── Dockerfile
│   │   │   └── docker-compose.yml  # Gateway-only stack
│   │   └── client/main.go          # Gateway ping client
│   └── orchestrator/
│       ├── server/
│       │   ├── main.go             # Orchestrator server entry point
│       │   ├── Dockerfile
│       │   └── docker-compose.yml  # Orchestrator-only stack
│       └── client/main.go          # Orchestrator ping client
└── stovepipe/
    ├── docker-compose.yml          # Single Ping-only service stack
    ├── server/
    │   ├── main.go                 # Stovepipe gRPC server (Docker: :8080; go run default :8083)
    │   └── Dockerfile
    └── client/main.go              # Stovepipe ping client
```

## Running

### Docker Compose (recommended)


```bash
# Start full SubmitQueue stack (Gateway + Orchestrator + MySQL)
make local-submitqueue-start
make local-submitqueue-gateway-start
make local-submitqueue-orchestrator-start

# Start Stovepipe service (single Ping-only gRPC service)
make local-stovepipe-start

# View logs and status
make local-submitqueue-logs
make local-submitqueue-ps

# Stop (SubmitQueue + Stovepipe default projects)
make local-stop
```

For Docker, `make build-stovepipe-linux` copies a Linux binary to `.docker-bin/stovepipe` so it does not overwrite SubmitQueue’s `.docker-bin/gateway`. Stovepipe is currently a single Ping-only service with no storage or queue dependencies, so the compose stack has no MySQL. The compose service key is **`stovepipe-service`**, so with default project **`stovepipe`** you should see **`stovepipe-stovepipe-service-1`**. `make local-stop` stops the SubmitQueue, Stovepipe, and Runway stacks; `make local-stovepipe-stop` stops only Stovepipe.

### Bazel

```bash
# Build servers
bazel build //example/submitqueue/gateway/server:gateway
bazel build //example/submitqueue/orchestrator/server:orchestrator
bazel build //example/stovepipe/server:stovepipe

# Build clients
bazel build //example/submitqueue/gateway/client:gateway
bazel build //example/submitqueue/orchestrator/client:orchestrator
bazel build //example/stovepipe/client:stovepipe
```

### Go

```bash
go run example/submitqueue/gateway/server/main.go
go run example/submitqueue/orchestrator/server/main.go
go run example/stovepipe/server/main.go
```

## Testing with Clients

```bash
# Go clients
go run example/submitqueue/gateway/client/main.go -addr localhost:8081 -message "hello"
go run example/submitqueue/orchestrator/client/main.go -addr localhost:8082 -message "hello"
go run example/stovepipe/client/main.go -addr localhost:8083 -message "hello"
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

# List services
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext localhost:8082 list
grpcurl -plaintext localhost:8083 list

# Describe a service
grpcurl -plaintext localhost:8081 describe uber.submitqueue.gateway.SubmitQueueGateway
grpcurl -plaintext localhost:8082 describe uber.submitqueue.orchestrator.SubmitQueueOrchestrator
grpcurl -plaintext localhost:8083 describe uber.submitqueue.stovepipe.Stovepipe
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
