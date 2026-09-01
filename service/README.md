# Service Wiring

Runnable gRPC servers and clients that wire each domain's controllers and extensions together and run them locally. This is the composition root — the one layer that knows the full queue topology, picks concrete extension implementations, and owns per-queue routing. Controllers, entities, and extensions stay transport- and wiring-agnostic; everything here turns them into a process you can start.

Each domain has its own subdirectory with a dedicated README:

- [`submitqueue/`](submitqueue/README.md) — the multi-service SubmitQueue domain (Gateway + Orchestrator).
- [`stovepipe/`](stovepipe/README.md) — the single-service Stovepipe domain (ingest → process → build → buildsignal → record).
- [`runway/`](runway/README.md) — the single-service Runway landing service (consumes the merge queues).

## Services

| Service | Port | Domain | RPCs | Backing stores |
|---------|------|--------|------|----------------|
| **SubmitQueue Gateway** | 8081 | `submitqueue` | `Ping`, `Land`, `Cancel`, `GetRequestSummaryByID`, `GetRequestSummaryByChangeURI`, `List`, `GetRequestHistoryByID`, `GetRequestHistoryByChangeURI` | MySQL app + queue |
| **SubmitQueue Orchestrator** | 8082 | `submitqueue` | `Ping` (+ consumes start, cancel, validate, merge-conflict-check-signal, batch, dependency-analysis, speculate, build, buildsignal, submitqueue-merge, merge-signal, conclude, submitqueue-hook, and paired DLQ topics) | MySQL app + queue |
| **Stovepipe** | 8083 | `stovepipe` | `Ping`, `Ingest` (+ consumes process, build, buildsignal, record, stovepipe-hook, and paired DLQ topics) | MySQL storage + queue |
| **Runway** | 8086 | `runway` | `Ping` (+ consumes merge-conflict-check & runway-merge topics) | MySQL queue |

Ports above are the `go run` defaults; under Docker Compose each server listens on `:8080` inside its container and is published on a random ephemeral host port (use `make local-*-ps` / `docker port` to discover it).

## Directory Structure

```
service/
├── submitqueue/
│   ├── docker-compose.yml              # Full stack (Gateway + Orchestrator + Runway + 2x MySQL)
│   ├── gateway/
│   │   ├── server/                     # Gateway server entry point + Dockerfile + compose
│   │   │   └── queues.yaml             # Gateway valid-queue registry
│   │   └── client/                     # Gateway command-line client
│   └── orchestrator/
│       ├── server/                     # Orchestrator server entry point + Dockerfile + compose
│       └── client/                     # Orchestrator ping client
├── stovepipe/
│   ├── docker-compose.yml              # Stovepipe service + storage MySQL + queue MySQL
│   ├── docker-compose.debug.yml        # Debug variant with delve
│   ├── server/                         # Stovepipe gRPC server + Dockerfile + compose
│   └── client/                         # Stovepipe ping client
└── runway/
    ├── server/                         # Runway gRPC server + Dockerfile + compose
    └── client/                         # Runway ping client
```

## Running

### Docker Compose (recommended)

```bash
# Full SubmitQueue workflow stack (Gateway + Orchestrator + Runway + MySQL)
make local-submitqueue-start
make local-submitqueue-gateway-start        # gateway-only stack
make local-submitqueue-orchestrator-start   # orchestrator-only stack

# Stovepipe service (gRPC service + storage MySQL + queue MySQL)
make local-stovepipe-start
make local-stovepipe-logs

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
go run ./service/submitqueue/gateway/client -addr localhost:8081 ping -message "hello"
go run ./service/submitqueue/orchestrator/client -addr localhost:8082 -message "hello"
go run ./service/stovepipe/client -addr localhost:8083 -message "hello"
go run ./service/runway/client -addr localhost:8086 -message "hello"

# Bazel-run clients (honor SERVER_ADDR / MESSAGE)
make run-client-submitqueue-gateway
make run-client-submitqueue-orchestrator
make run-client-stovepipe
make run-client-runway
```

The Gateway client is command-oriented (`ping`, `land`, `status`, `list`, and `watch`); run it without arguments for command-specific usage. The other clients are Ping clients with `-addr`, `-message`, and `-timeout` flags.

### grpcurl

Install grpcurl if you don't have it:
```bash
brew install grpcurl  # macOS
# or
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

```bash
# Ping services whose reflection descriptors resolve normally
grpcurl -plaintext -d '{"message": "hello"}' localhost:8082 uber.submitqueue.orchestrator.SubmitQueueOrchestrator/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8083 uber.submitqueue.stovepipe.Stovepipe/Ping
grpcurl -plaintext -d '{"message": "hello"}' localhost:8086 uber.runway.Runway/Ping

# Gateway: use the repository client, or supply its proto explicitly
go run ./service/submitqueue/gateway/client -addr localhost:8081 ping -message "hello"
grpcurl -plaintext -import-path . -proto api/submitqueue/gateway/proto/gateway.proto \
  -d '{"message": "hello"}' localhost:8081 uber.submitqueue.gateway.SubmitQueueGateway/Ping
```

The Gateway registers reflection, but its generated descriptor currently cannot resolve one imported file name. Do not use Gateway reflection-based `list` or `describe`; use the client or pass `-import-path` and `-proto` to grpcurl.

## API Reference

### Gateway Service

**Service**: `uber.submitqueue.gateway.SubmitQueueGateway`
**Proto**: `api/submitqueue/gateway/proto/gateway.proto`

| Method | Description |
|--------|-------------|
| `Ping` | Health check, returns service name and timestamp |
| `Land` | Submit a land request for code changes |
| `Cancel` | Record and publish a best-effort cancellation intent |
| `GetRequestSummaryByID` | Read one authoritative current request summary by sqid |
| `GetRequestSummaryByChangeURI` | Read authoritative current summaries for one exact pinned change URI |
| `List` | Page through queue-scoped request receipt history |
| `GetRequestHistoryByID` | Read every retained request-log event for one sqid |
| `GetRequestHistoryByChangeURI` | Read retained request histories for one exact pinned change URI |

The gateway owns request receipts, current-status projections, and request-log reads. `List` is ordered by immutable gateway receipt time. The queue projection may briefly lag the authoritative summaries returned by the request-summary methods while materialization converges.

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
