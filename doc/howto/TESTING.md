# Testing

All testing (automated and manual) uses **containerized environments** for consistency and reproducibility.

## Prerequisites

**Docker must be running** for all integration, e2e, and manual testing.

```bash
# Check Docker is running
docker ps

# If not running:
# - macOS: Start Docker Desktop
# - Linux: sudo systemctl start docker
```

---

## Database Architecture

SubmitQueue uses **two separate databases** to demonstrate proper architectural separation:

### 1. Application Database
- **Purpose**: Business data (requests, counters, batches)
- **Schema**: `extension/storage/mysql/schema`, `extension/counter/mysql/schema`
- **Used by**: Gateway (stores requests), Orchestrator (reads/updates request state)
- **Connection**: `MYSQL_DSN`

### 2. Queue Database
- **Purpose**: Messaging infrastructure (queue messages, offsets, partition leases)
- **Schema**: `extension/queue/sql/schema`
- **Used by**: Gateway (publishes), Orchestrator (consumes)
- **Connection**: `QUEUE_MYSQL_DSN`

**Why separate?**
- Queue is **pluggable infrastructure** - you can swap MySQL queue for Kafka, SQS, etc.
- Application data and messaging concerns scale independently
- Clear architectural boundary between business logic and infrastructure
- In production, queue infrastructure often runs separately (e.g., managed Kafka cluster)

**Note:** Both use MySQL in examples for simplicity, but in production the queue could use a different technology entirely.

---

## Automated Testing

### Quick Reference

```bash
# Unit tests (no Docker required)
make test

# Integration tests (Docker required)
make integration-test-gateway       # Gateway in isolation
make integration-test-orchestrator  # Orchestrator in isolation
make integration-test-extensions    # All extension tests
make integration-test               # All integration tests

# E2E tests (Docker required)
make e2e-test

# Build
make build                          # Build all targets
make build-all-linux                # Build Linux binaries for Docker
```

### Testing Levels

**1. Unit Tests** - Fast, no containers
- Location: Co-located with code (`{package}/*_test.go`)
- Run: `make test`
- Speed: Fast (< 1s typically)

**2. Integration Tests** - Service in isolation with real dependencies
- Location: `test/integration/{service}/`
- Run: `make integration-test-{service}`
- Containers: MySQL + one service
- Tests one service isolated from others

**3. E2E Tests** - Complete workflows across all services
- Location: `test/e2e/`
- Run: `make e2e-test`
- Containers: MySQL + all services
- Tests cross-service communication

### How Automated Tests Work

Tests use **docker-compose** to spin up containers automatically:

1. `SetupSuite()` - Creates MySQL + service containers **once** per test suite
2. All tests run against those containers
3. `TearDownSuite()` - Cleans up containers automatically

---

## Container Naming

Test containers use **meaningful, context-rich names** for easy debugging and correlation.

### Naming Format

```
{project-name}-{service-name}-{instance}
     └─┬─┘      └────┬────┘    └──┬──┘
   From test   From compose   Docker adds
```

Project name format:
```
sq-test-{context}-{shortid}
│       │         │
│       │         └─ 6-char hex timestamp (unique per test run)
│       └─────────── Test context (ext-counter, gateway, e2e, etc.)
└─────────────────── Namespace prefix
```

### Real Examples

**Extension Test - Counter:**
```bash
Container: sq-test-ext-counter-2ce1d0-mysql-1
           │       │           │      │     │
           │       │           │      │     └─ Instance number
           │       │           │      └─ Service from docker-compose.yml
           │       │           └─ Short unique ID
           │       └─ Test context
           └─ Namespace prefix

# From:
NewComposeStack(t, log, ctx, "docker-compose.yml", "ext-counter")
```

**Service Test - Gateway (Multiple Containers):**
```bash
# All share same project prefix, different services:
sq-test-gateway-abc123-mysql-app-1
sq-test-gateway-abc123-mysql-queue-1
sq-test-gateway-abc123-gateway-service-1
└──────────┬───────────┘
      Same project (same test run)

# From:
NewComposeStack(t, log, ctx, composeFile, "gateway")
```

**E2E Test - Full Stack (4 Containers):**
```bash
sq-test-e2e-def456-mysql-app-1
sq-test-e2e-def456-mysql-queue-1
sq-test-e2e-def456-gateway-service-1
sq-test-e2e-def456-orchestrator-service-1
└────┬─────┘
  All same project
```

### Container Name Reference

| Test Type | Context | Service Names | Example Container |
|-----------|---------|---------------|-------------------|
| Counter extension | `ext-counter` | `mysql` | `sq-test-ext-counter-2ce1d0-mysql-1` |
| Storage extension | `ext-storage` | `mysql` | `sq-test-ext-storage-ecff68-mysql-1` |
| Queue extension | `ext-queue` | `mysql` | `sq-test-ext-queue-a1b2c3-mysql-1` |
| Gateway service | `gateway` | `mysql-app`, `mysql-queue`, `gateway-service` | `sq-test-gateway-abc123-gateway-service-1` |
| Orchestrator service | `orchestrator` | `mysql-app`, `mysql-queue`, `orchestrator-service` | `sq-test-orchestrator-xyz789-orchestrator-service-1` |
| E2E full stack | `e2e` | `mysql-app`, `mysql-queue`, `gateway-service`, `orchestrator-service` | `sq-test-e2e-def456-gateway-service-1` |

### Debugging with Container Names

```bash
# See what tests are currently running
docker ps --format "table {{.Names}}\t{{.Status}}" | grep sq-test

# Find all containers from gateway test
docker ps | grep sq-test-gateway

# View logs from specific test container
docker logs sq-test-gateway-abc123-mysql-app-1

# Inspect a specific test's MySQL
docker exec -it sq-test-ext-counter-2ce1d0-mysql-1 \
  mysql -uroot -proot submitqueue -e "SHOW TABLES;"
```

---

## Manual Testing

### Quick Start

```bash
# Start all services (Gateway + Orchestrator + 2 MySQL DBs)
make local-start

# See running containers and endpoints
make local-ps

# View logs
make local-logs

# Stop all services
make local-stop
```

### Testing Individual Services

**Gateway Only:**
```bash
# Start Gateway in isolation (Gateway + 2 MySQL DBs)
make local-gateway-start

# Test Ping API (port shown by make local-ps)
grpcurl -plaintext -d '{"message": "hello"}' localhost:<PORT> submitqueue.SubmitQueueGateway/Ping

# Test Land API
grpcurl -plaintext -d '{
  "queue": "test-queue",
  "change": {"source": "github", "ids": ["PR-123"]},
  "strategy": "REBASE"
}' localhost:<PORT> submitqueue.SubmitQueueGateway/Land

# Stop
make local-gateway-stop
```

**Orchestrator Only:**
```bash
# Start Orchestrator in isolation (Orchestrator + 2 MySQL DBs)
make local-orchestrator-start

# Test Ping API (port shown by make local-ps)
grpcurl -plaintext -d '{"message": "hello"}' localhost:<PORT> submitqueue.SubmitQueueOrchestrator/Ping

# Stop
make local-orchestrator-stop
```

**Note:** All ports are ephemeral (randomly assigned). Use `make local-ps` to see the actual port mappings.

### After Code Changes

```bash
# Rebuild and restart all services
make local-restart

# Or stop and start fresh
make local-stop
make local-start
```

### Inspecting the Databases

```bash
# Find the port (shown by make local-ps)
make local-ps

# Connect to application DB
mysql -h127.0.0.1 -P<APP_PORT> -uroot -proot submitqueue

# Connect to queue DB
mysql -h127.0.0.1 -P<QUEUE_PORT> -uroot -proot submitqueue
```

### Using grpcurl

```bash
# Install grpcurl if not already installed
brew install grpcurl  # macOS
# OR: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# List services (use port from make local-ps)
grpcurl -plaintext localhost:<PORT> list

# Describe a service
grpcurl -plaintext localhost:<PORT> describe submitqueue.SubmitQueueGateway

# Call Ping
grpcurl -plaintext -d '{"message": "test"}' \
  localhost:<PORT> submitqueue.SubmitQueueGateway/Ping

# Call Land
grpcurl -plaintext -d '{
  "queue": "my-queue",
  "change": {"source": "github", "ids": ["PR-456"]},
  "strategy": "REBASE"
}' localhost:<PORT> submitqueue.SubmitQueueGateway/Land
```

### Available Commands

| Command | Description |
|---------|-------------|
| `make local-start` | Start all services (full stack) |
| `make local-gateway-start` | Start Gateway in isolation |
| `make local-orchestrator-start` | Start Orchestrator in isolation |
| `make local-ps` | Show running containers and ports |
| `make local-logs` | Follow logs from all services |
| `make local-restart` | Rebuild and restart all services |
| `make local-stop` | Stop all services (keep data) |
| `make local-gateway-stop` | Stop Gateway service |
| `make local-orchestrator-stop` | Stop Orchestrator service |
| `make local-clean` | Stop and remove all services, volumes, and images |

---

## Troubleshooting

### Docker Not Running
```bash
# Error: "Cannot connect to the Docker daemon"
# Solution: Start Docker Desktop or Docker daemon
docker ps  # Should not error
```

### Services Not Starting
```bash
# Check logs
make local-logs

# Rebuild from scratch
make local-clean
make local-start
```

### Port Already in Use
```bash
# Docker Compose uses ephemeral ports, so conflicts are rare.
# If a test left containers behind:
docker ps | grep sq-test
docker rm -f <container-id>
```

### Database Schema Not Applied
```bash
# Re-apply schemas manually
make local-init-schemas

# Or recreate everything
make local-clean
make local-start
```

### Tests Timing Out
```bash
# Clean Docker cache
docker system prune -a

# Clean Bazel cache
make clean

# Re-run tests
make integration-test
```

### Containers Not Cleaning Up
```bash
# List all test containers
docker ps -a | grep sq-test

# Remove all test containers
docker ps -a | grep sq-test | awk '{print $1}' | xargs docker rm -f

# Remove all test networks
docker network ls | grep sq-test | awk '{print $1}' | xargs docker network rm
```

---

## Writing New Tests

### Adding Unit Tests

1. Create `{file}_test.go` next to production code
2. Use table-driven tests
3. Run: `make test`

### Adding Integration Tests

1. Add test to `test/integration/{service}/suite_test.go`
2. Use suite's resources (`s.client`, `s.db`)
3. Run: `make integration-test-{service}`

Example:
```go
func (s *GatewayIntegrationSuite) TestNewFeature() {
resp, err := s.client.NewAPI(s.ctx, &pb.Request{...})
require.NoError(s.T(), err)
assert.Equal(s.T(), "expected", resp.Value)
}
```

### Adding E2E Tests

1. Add test to `test/e2e/suite_test.go`
2. Use all service clients
3. Use `require.Eventually()` for async operations
4. Run: `make e2e-test`

---

## See Also

- [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) - Project organization
- [CLAUDE.md](../../CLAUDE.md) - Development guidelines
- [example/server/docker-compose.yml](../../example/server/docker-compose.yml) - Full stack service definitions
- [example/server/gateway/docker-compose.yml](../../example/server/gateway/docker-compose.yml) - Gateway isolation
- [example/server/orchestrator/docker-compose.yml](../../example/server/orchestrator/docker-compose.yml) - Orchestrator isolation
