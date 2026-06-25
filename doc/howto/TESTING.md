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
- **Schema**: `submitqueue/extension/storage/mysql/schema`, `platform/extension/counter/mysql/schema`
- **Used by**: Gateway (stores requests), Orchestrator (reads/updates request state)
- **Connection**: `MYSQL_DSN`

### 2. Queue Database
- **Purpose**: Messaging infrastructure (queue messages, offsets, partition leases)
- **Schema**: `platform/extension/messagequeue/mysql/schema`
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
make integration-test-submitqueue-gateway       # Gateway in isolation
make integration-test-submitqueue-orchestrator  # Orchestrator in isolation
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
- Location: `test/integration/submitqueue/{service}/`
- Run: `make integration-test-{service}`
- Containers: MySQL + one service
- Tests one service isolated from others

**3. E2E Tests** - Complete workflows across all services
- Location: `test/e2e/submitqueue/`
- Run: `make e2e-test`
- Containers: MySQL + all services
- Tests cross-service communication

### How Automated Tests Work

Tests use **docker-compose** via `ComposeStack` to spin up containers automatically:

1. `NewComposeStack()` registers cleanup (stop log tailing, tear down containers)
2. `Up()` starts containers, waits for healthchecks (`--wait`), and auto-tails container logs to stderr
3. Tests run against those containers with **real-time log output**
4. On cleanup, containers are torn down automatically (set `SKIP_CLEANUP=true` to keep them for inspection)

---

## Container Naming

Test containers use **meaningful, context-rich, domain-qualified names** for easy debugging and correlation — and so suites from different domains never collide when run in parallel.

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
│       └─────────── Test context (domain-qualified — see convention)
└─────────────────── Namespace prefix
```

### Context naming convention

The `{context}` passed to `NewComposeStack` is **domain-qualified** so that the same kind of suite in different domains yields distinct, self-describing names:

```
{category}-{domain}-{name}
```

- `{category}` — `svc` (service), `ext` (extension), `core` (domain-internal infra), or `e2e`
- `{domain}` — `submitqueue`, `stovepipe`, … — **omit for shared/cross-domain code**
- `{name}` — the specific service/extension (e.g. `gateway`, `storage-mysql`)

Shared (cross-domain) suites carry no domain segment — e.g. the shared queue extension uses `ext-messagequeue-sql`.

### Context reference

| Suite | Context | Example container |
|-------|---------|-------------------|
| SubmitQueue gateway | `svc-submitqueue-gateway` | `sq-test-svc-submitqueue-gateway-abc123-gateway-service-1` |
| SubmitQueue orchestrator | `svc-submitqueue-orchestrator` | `sq-test-svc-submitqueue-orchestrator-xyz789-orchestrator-service-1` |
| Stovepipe | `svc-stovepipe` | `sq-test-svc-stovepipe-abc123-stovepipe-service-1` |
| SubmitQueue storage extension | `ext-submitqueue-storage-mysql` | `sq-test-ext-submitqueue-storage-mysql-2ce1d0-mysql-1` |
| Counter extension (shared) | `ext-counter-mysql` | `sq-test-ext-counter-mysql-…-mysql-1` |
| SubmitQueue changestore extension | `ext-submitqueue-changestore-mysql` | `sq-test-ext-submitqueue-changestore-mysql-…-mysql-1` |
| Shared queue extension | `ext-messagequeue-sql` | `sq-test-ext-messagequeue-sql-a1b2c3-mysql-1` |
| SubmitQueue consumer (core) | `core-submitqueue-consumer` | `sq-test-core-submitqueue-consumer-…-mysql-1` |
| SubmitQueue e2e (full stack) | `e2e-submitqueue` | `sq-test-e2e-submitqueue-def456-gateway-service-1` |

### Parallel execution

Every suite gets a unique project name (`{context}-{shortid}`) and every compose service publishes **ephemeral host ports** (`- "3306"`, `- "8080"`), so suites are fully isolated and run **in parallel**. `make integration-test` runs all suites concurrently via `--test_output=errors` (`--test_output=streamed` would force bazel to serialize them). The domain-qualified context is what keeps container names unambiguous when many run at once.

### Debugging with Container Names

Container logs are **automatically streamed to stderr** during test runs, so you'll see service output (startup messages, errors, zap logs) in real time — both locally and in CI.

For additional manual inspection:

```bash
# See what tests are currently running
docker ps --format "table {{.Names}}\t{{.Status}}" | grep sq-test

# Find all containers from gateway test
docker ps | grep sq-test-gateway

# Inspect a specific test's MySQL
docker exec -it sq-test-ext-counter-2ce1d0-mysql-1 \
  mysql -uroot -proot submitqueue -e "SHOW TABLES;"
```

---

## Manual Testing

### Quick Start

```bash
# Start all services (Gateway + Orchestrator + 2 MySQL DBs)
make local-submitqueue-start

# See running containers and endpoints
make local-submitqueue-ps

# View logs
make local-submitqueue-logs

# Stop all services
make local-stop
```

### Testing Individual Services

**Gateway Only:**
```bash
# Start Gateway in isolation (Gateway + 2 MySQL DBs)
make local-submitqueue-gateway-start

# Test Ping API (port shown by make local-submitqueue-ps)
grpcurl -plaintext -d '{"message": "hello"}' localhost:<PORT> submitqueue.SubmitQueueGateway/Ping

# Test Land API
grpcurl -plaintext -d '{
  "queue": "test-queue",
  "change": {"source": "github", "ids": ["PR-123"]},
  "strategy": "REBASE"
}' localhost:<PORT> submitqueue.SubmitQueueGateway/Land

# Stop
make local-submitqueue-gateway-stop
```

**Orchestrator Only:**
```bash
# Start Orchestrator in isolation (Orchestrator + 2 MySQL DBs)
make local-submitqueue-orchestrator-start

# Test Ping API (port shown by make local-submitqueue-ps)
grpcurl -plaintext -d '{"message": "hello"}' localhost:<PORT> submitqueue.SubmitQueueOrchestrator/Ping

# Stop
make local-submitqueue-orchestrator-stop
```

**Note:** All ports are ephemeral (randomly assigned). Use `make local-submitqueue-ps` to see the actual port mappings.

### After Code Changes

```bash
# Rebuild and restart all services
make local-submitqueue-restart

# Or stop and start fresh
make local-stop
make local-submitqueue-start
```

### Inspecting the Databases

```bash
# Find the port (shown by make local-submitqueue-ps)
make local-submitqueue-ps

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

# List services (use port from make local-submitqueue-ps)
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
| `make local-submitqueue-start` | Start all services (full stack) |
| `make local-submitqueue-gateway-start` | Start Gateway in isolation |
| `make local-submitqueue-orchestrator-start` | Start Orchestrator in isolation |
| `make local-submitqueue-ps` | Show running containers and ports |
| `make local-submitqueue-logs` | Follow logs from all services |
| `make local-submitqueue-restart` | Rebuild and restart all services |
| `make local-stop` | Stop all services (keep data) |
| `make local-submitqueue-gateway-stop` | Stop Gateway service |
| `make local-submitqueue-orchestrator-stop` | Stop Orchestrator service |
| `make local-submitqueue-clean` | Stop and remove all services, volumes, and images |

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
make local-submitqueue-logs

# Rebuild from scratch
make local-submitqueue-clean
make local-submitqueue-start
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
make local-init-submitqueue-schemas

# Or recreate everything
make local-submitqueue-clean
make local-submitqueue-start
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

Containers are torn down automatically after each test. Set `SKIP_CLEANUP=true` to keep them for inspection.

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

1. Add test to `test/integration/submitqueue/{service}/suite_test.go`
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

1. Add test to `test/e2e/submitqueue/suite_test.go`
2. Use all service clients
3. Use `require.Eventually()` for async operations
4. Run: `make e2e-test`

---

## See Also

- [CLAUDE.md](../../CLAUDE.md) - Development guidelines and project structure
- [example/submitqueue/docker-compose.yml](../../example/submitqueue/docker-compose.yml) - Full stack service definitions
- [example/submitqueue/gateway/server/docker-compose.yml](../../example/submitqueue/gateway/server/docker-compose.yml) - Gateway isolation
- [example/submitqueue/orchestrator/server/docker-compose.yml](../../example/submitqueue/orchestrator/server/docker-compose.yml) - Orchestrator isolation
