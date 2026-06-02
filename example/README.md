# Examples

Example gRPC servers and clients for running the submitqueue services locally.

## Services

- **SubmitQueue Gateway** (port 8081) — entry point for land requests. Exposes `Ping` and `Land` RPCs.
- **SubmitQueue Orchestrator** (port 8082) — coordinates the pipeline. Exposes `Ping` RPC and consumes queue messages across 9 pipeline topics.
- **Stovepipe Gateway** (port 8083) - entry point for commit deployment verification requests. Exposes `Ping` RPC.
- **Stovepipe Orchestrator** (port 8084) - coordinates the commit verification pipeline. Exposes `Ping` RPC.

Services require MySQL (app database + queue database). Docker Compose handles this automatically.

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
├── client/
│   ├── gateway/main.go             # Gateway ping client
│   └── orchestrator/main.go        # Orchestrator ping client
└── stovepipe/
    ├── docker-compose.yml          # Full stack (Stovepipe Gateway + Orchestrator + 2x MySQL)
    ├── gateway/
    │   ├── server/
    │   │   ├── main.go             # Stovepipe gateway gRPC server (Docker: :8080; go run default :8083)
    │   │   ├── Dockerfile
    │   │   └── docker-compose.yml  # Gateway-only stack
    │   └── client/main.go          # Stovepipe gateway ping client
    └── orchestrator/
        ├── server/
        │   ├── main.go             # Stovepipe orchestrator gRPC server (Docker: :8080; go run default :8084)
        │   ├── Dockerfile
        │   └── docker-compose.yml  # Orchestrator-only stack
        └── client/main.go          # Stovepipe orchestrator ping client
```

## Running

### Docker Compose (recommended)


```bash
# Start full SubmitQueue stack (Gateway + Orchestrator + MySQL)
make local-start
make local-gateway-start
make local-orchestrator-start

# Start full Stovepipe stack (Gateway + Orchestrator + MySQL)
make local-stovepipe-start
make local-stovepipe-gateway-start
make local-stovepipe-orchestrator-start

# View logs and status
make local-logs
make local-ps

# Stop (SubmitQueue + Stovepipe default projects)
make local-stop
```

For Docker, `make build-stovepipe-gateway-linux` copies a Linux binary to `.docker-bin/stovepipe-gateway` so it does not overwrite SubmitQueue’s `.docker-bin/gateway`. Stovepipe `make local-stovepipe-gateway-start` applies **only the queue schema** on `mysql-queue` (`make local-init-stovepipe-queue-schema`); SubmitQueue storage/counter schemas on `mysql-app` are skipped until Stovepipe has its own app schema. Compose service keys are **`gateway-service`** and **`orchestrator-service`** (same as `example/server`), so with default project **`stovepipe`** you should see **`stovepipe-gateway-service-1`** and **`stovepipe-orchestrator-service-1`** — not `stovepipe-stovepipe-*` (that pattern was from older service names; run **`make local-stop`** to run `docker compose down --remove-orphans` and drop orphans). `make local-stop` also stops the SubmitQueue stack. SubmitQueue examples use project **`submitqueue`** by default (`make SUBMITQUEUE_LOCAL_PROJECT=myname ...`). Stovepipe containers are named like `stovepipe-mysql-app-1`, not `submitqueue-*`.

### Bazel

```bash
# Build servers
bazel build //example/server/gateway:gateway
bazel build //example/server/orchestrator:orchestrator
bazel build //example/stovepipe/gateway/server:gateway
bazel build //example/stovepipe/orchestrator/server:orchestrator

# Build clients
bazel build //example/client/gateway:gateway
bazel build //example/client/orchestrator:orchestrator
bazel build //example/stovepipe/gateway/client:gateway
bazel build //example/stovepipe/orchestrator/client:orchestrator
```

### Go

```bash
go run example/server/gateway/main.go
go run example/server/orchestrator/main.go
go run example/stovepipe/gateway/server/main.go
go run example/stovepipe/orchestrator/server/main.go
```

## Testing with Clients

```bash
# Go clients
go run example/client/gateway/main.go -addr localhost:8081 -message "hello"
go run example/client/orchestrator/main.go -addr localhost:8082 -message "hello"
go run example/stovepipe/gateway/client/main.go -addr localhost:8083 -message "hello"
go run example/stovepipe/orchestrator/client/main.go -addr localhost:8084 -message "hello"
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
grpcurl -plaintext -d '{"message": "hello"}' localhost:8083 uber.submitqueue.stovepipe.StovepipeGateway/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8084 uber.submitqueue.stovepipe.orchestrator.StovepipeOrchestrator/Ping

# List services
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext localhost:8082 list
grpcurl -plaintext localhost:8083 list
grpcurl -plaintext localhost:8084 list

# Describe a service
grpcurl -plaintext localhost:8081 describe uber.submitqueue.gateway.SubmitQueueGateway
grpcurl -plaintext localhost:8082 describe uber.submitqueue.orchestrator.SubmitQueueOrchestrator
grpcurl -plaintext localhost:8083 describe uber.submitqueue.stovepipe.StovepipeGateway
grpcurl -plaintext localhost:8084 describe uber.submitqueue.stovepipe.orchestrator.StovepipeOrchestrator
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

### Stovepipe Gateway

**Service**: `uber.submitqueue.stovepipe.StovepipeGateway`
**Proto**: `stovepipe/gateway/proto/gateway.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check |

### Stovepipe Orchestrator

**Service**: `uber.submitqueue.stovepipe.orchestrator.StovepipeOrchestrator`
**Proto**: `stovepipe/orchestrator/proto/orchestrator.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check |
