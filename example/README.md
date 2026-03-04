# Examples

Example gRPC servers and clients for running the submitqueue services locally.

## Services

- **Gateway** (port 8081) — entry point for land requests. Exposes `Ping` and `Land` RPCs.
- **Orchestrator** (port 8082) — coordinates the pipeline. Exposes `Ping` RPC and consumes queue messages across 9 pipeline topics.

Both services require MySQL (app database + queue database). Docker Compose handles this automatically.

## Directory Structure

```
example/
├── server/
│   ├── docker-compose.yml          # Full stack (Gateway + Orchestrator + 2x MySQL)
│   ├── gateway/
│   │   ├── main.go                 # Gateway server entry point
│   │   ├── Dockerfile
│   │   └── docker-compose.yml      # Gateway-only stack
│   └── orchestrator/
│       ├── main.go                 # Orchestrator server entry point
│       ├── Dockerfile
│       └── docker-compose.yml      # Orchestrator-only stack
└── client/
    ├── gateway/main.go             # Gateway ping client
    └── orchestrator/main.go        # Orchestrator ping client
```

## Running

### Docker Compose (Recommended)

```bash
# Start full stack (Gateway + Orchestrator + MySQL)
make local-start

# Start individual services
make local-gateway-start
make local-orchestrator-start

# View logs and status
make local-logs
make local-ps

# Stop
make local-stop
```

### Bazel

```bash
# Build servers
bazel build //example/server/gateway:gateway
bazel build //example/server/orchestrator:orchestrator

# Build clients
bazel build //example/client/gateway:gateway
bazel build //example/client/orchestrator:orchestrator
```

### Go

```bash
go run example/server/gateway/main.go
go run example/server/orchestrator/main.go
```

## Testing with Clients

```bash
# Go clients
go run example/client/gateway/main.go -addr localhost:8081 -message "hello"
go run example/client/orchestrator/main.go -addr localhost:8082 -message "hello"
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

# List services
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext localhost:8082 list

# Describe a service
grpcurl -plaintext localhost:8081 describe uber.submitqueue.gateway.SubmitQueueGateway
grpcurl -plaintext localhost:8082 describe uber.submitqueue.orchestrator.SubmitQueueOrchestrator
```

## API Reference

### Gateway Service

**Service**: `uber.submitqueue.gateway.SubmitQueueGateway`
**Proto**: `gateway/proto/gateway.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check, returns service name and timestamp |
| `Land` | Submit a land request for code changes |

### Orchestrator Service

**Service**: `uber.submitqueue.orchestrator.SubmitQueueOrchestrator`
**Proto**: `orchestrator/proto/orchestrator.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check, returns service name and timestamp |
