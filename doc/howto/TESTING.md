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

### 1. Application Database (port 3306)
- **Purpose**: Business data (requests, counters, batches)
- **Schema**: `extension/storage/mysql/schema`, `extension/counter/mysql/schema`
- **Used by**: Gateway (stores requests), Orchestrator (reads/updates request state)
- **Connection**: `MYSQL_DSN`

### 2. Queue Database (port 3307)
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
make integration-test-queue         # Queue infrastructure
make integration-test               # All integration tests

# E2E tests (Docker required)
make e2e-test

# Everything (Docker required)
make test-all
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

**Benefits:**
- ✅ Hermetic (isolated environment per suite)
- ✅ Fast (containers reused across tests)
- ✅ No manual setup required
- ✅ Reproducible (same behavior everywhere)
- ✅ Parallel execution (tests don't conflict)

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
│       └─────────── Test context (storage, gateway, e2e, etc.)
└─────────────────── Namespace prefix
```

### Real Examples

**Extension Test - Storage:**
```bash
Container: sq-test-storage-2ce1d0-mysql-storage-1
           │       │       │      │              │
           │       │       │      │              └─ Instance number
           │       │       │      └─ Service from docker-compose.yml
           │       │       └─ Short unique ID
           │       └─ Test context
           └─ Namespace prefix

# From:
NewComposeStack(t, log, ctx, "docker-compose.yml", "storage")
                                                     └─────┘
# docker-compose.yml:
services:
  mysql-storage:  # ← Service name
```

**Service Test - Gateway (Multiple Containers):**
```bash
# All share same project prefix, different services:
sq-test-gateway-abc123-mysql-app-1
sq-test-gateway-abc123-mysql-queue-1
sq-test-gateway-abc123-gateway-1
└──────────┬───────────┘
      Same project (same test run)

# From:
NewComposeStack(t, log, ctx, composeFile, "gateway")

# docker-compose.yml:
services:
  mysql-app:    # ← Service name
  mysql-queue:  # ← Service name
  gateway:      # ← Service name
```

**E2E Test - Full Stack (4 Containers):**
```bash
sq-test-e2e-def456-mysql-app-1
sq-test-e2e-def456-mysql-queue-1
sq-test-e2e-def456-gateway-1
sq-test-e2e-def456-orchestrator-1
└────┬─────┘
  All same project
```

### Container Name Reference

| Test Type | Context | Service Names | Example Container |
|-----------|---------|---------------|-------------------|
| Storage extension | `storage` | `mysql-storage` | `sq-test-storage-2ce1d0-mysql-storage-1` |
| Counter extension | `counter` | `mysql-counter` | `sq-test-counter-ecff68-mysql-counter-1` |
| Queue extension | `queue` | `mysql-queue` | `sq-test-queue-a1b2c3-mysql-queue-1` |
| Gateway service | `gateway` | `mysql-app`, `mysql-queue`, `gateway` | `sq-test-gateway-abc123-gateway-1` |
| Orchestrator service | `orchestrator` | `mysql-app`, `mysql-queue`, `orchestrator` | `sq-test-orchestrator-xyz789-orchestrator-1` |
| E2E full stack | `e2e` | `mysql-app`, `mysql-queue`, `gateway`, `orchestrator` | `sq-test-e2e-def456-gateway-1` |

### Why This Naming?

**Before** (opaque timestamps):
```bash
$ docker ps
test-1771786794254426000-mysql-storage-1  # What test is this?
test-1771786795123456789-gateway-1        # What's being tested?
```

**After** (context-rich):
```bash
$ docker ps
sq-test-storage-2ce1d0-mysql-storage-1    # Storage extension test
sq-test-gateway-abc123-mysql-app-1        # Gateway test - app database
sq-test-gateway-abc123-gateway-1          # Gateway test - gateway service
```

**Benefits:**
- ✅ **Easy correlation** - Instantly know which test created each container
- ✅ **Meaningful context** - Shows what's being tested (`storage`, `gateway`, `e2e`)
- ✅ **Parallel-safe** - Unique ID prevents conflicts when tests run simultaneously
- ✅ **Grouped containers** - Same project prefix = related containers from one test
- ✅ **Debugging** - Quickly identify containers in `docker ps` or logs

### Debugging with Container Names

```bash
# See what tests are currently running
docker ps --format "table {{.Names}}\t{{.Status}}" | grep sq-test

# Find all containers from gateway test
docker ps | grep sq-test-gateway

# View logs from specific test container
docker logs sq-test-gateway-abc123-mysql-app-1

# Inspect a specific test's MySQL
docker exec -it sq-test-storage-2ce1d0-mysql-storage-1 \
  mysql -uroot -proot submitqueue -e "SHOW TABLES;"
```

---

## Manual Testing

### Quick Start

```bash
# Start all services (Gateway + Orchestrator + 2 MySQL DBs)
make start-all-services

# Test with client
make client-gateway

# View logs
make logs

# Stop all services
make stop-all-services
```

### Testing Individual Services

**Gateway Only:**
```bash
# Start Gateway in isolation (Gateway + 2 MySQL DBs)
make start-gateway

# Test Ping API
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 submitqueue.SubmitQueueGateway/Ping

# Test Land API
grpcurl -plaintext -d '{
  "queue": "test-queue",
  "change": {"source": "github", "ids": ["PR-123"]},
  "strategy": "REBASE"
}' localhost:8081 submitqueue.SubmitQueueGateway/Land

# Stop
docker-compose -f example/server/gateway/docker-compose.yml down
```

**Orchestrator Only:**
```bash
# Start Orchestrator in isolation (Orchestrator + 2 MySQL DBs)
make start-orchestrator

# Test Ping API
grpcurl -plaintext -d '{"message": "hello"}' localhost:8082 submitqueue.SubmitQueueOrchestrator/Ping

# View consumer logs
docker-compose -f example/server/orchestrator/docker-compose.yml logs -f orchestrator

# Stop
docker-compose -f example/server/orchestrator/docker-compose.yml down
```

### After Code Changes

```bash
# Rebuild and restart all services
make rebuild-services

# Or rebuild specific service
docker-compose -f example/server/docker-compose.yml build gateway
docker-compose -f example/server/docker-compose.yml up -d gateway
```

### Inspecting the Databases

**Application Database** (requests, counters):
```bash
# Connect to application DB (port 3306)
docker exec -it submitqueue-mysql-app mysql -uroot -proot submitqueue

# Show tables
SHOW TABLES;

# View requests
SELECT * FROM requests;

# Exit
exit
```

**Queue Database** (messages, offsets):
```bash
# Connect to queue DB (port 3307)
docker exec -it submitqueue-mysql-queue mysql -uroot -proot submitqueue

# Show tables
SHOW TABLES;

# View queue messages
SELECT * FROM queue_messages;

# View partition offsets
SELECT * FROM queue_offsets;

# View partition leases
SELECT * FROM queue_partition_leases;

# Exit
exit
```

### Publishing Test Messages

Manually publish a message to test Orchestrator consumer:

```bash
# Insert message into queue database
docker exec -it submitqueue-mysql-queue mysql -uroot -proot submitqueue -e "
INSERT INTO queue_messages (id, topic, partition_key, payload, created_at, invisible_until)
VALUES (
  'test-msg-123',
  'land_request',
  'test-queue',
  '{\"id\":\"test-msg-123\",\"queue\":\"test-queue\",\"state\":\"pending\"}',
  NOW(),
  NOW()
);
"

# Watch Orchestrator process it
docker-compose -f example/server/docker-compose.yml logs -f orchestrator
```
```

### Using grpcurl

```bash
# Install grpcurl if not already installed
brew install grpcurl  # macOS
# OR: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# List services
grpcurl -plaintext localhost:8081 list

# Describe a service
grpcurl -plaintext localhost:8081 describe submitqueue.SubmitQueueGateway

# Call Ping
grpcurl -plaintext -d '{"message": "test"}' \
  localhost:8081 submitqueue.SubmitQueueGateway/Ping

# Call Land
grpcurl -plaintext -d '{
  "queue": "my-queue",
  "change": {"source": "github", "ids": ["PR-456"]},
  "strategy": "REBASE"
}' localhost:8081 submitqueue.SubmitQueueGateway/Land
```

### Available Commands

| Command | Description |
|---------|-------------|
| `make start-all-services` | Start all services (full stack) |
| `make start-gateway` | Start Gateway in isolation |
| `make start-orchestrator` | Start Orchestrator in isolation |
| `make logs` | Follow logs from all services |
| `make stop-all-services` | Stop all services |
| `make rebuild-services` | Rebuild after code changes |
| `make clean-services` | Stop and remove volumes (fresh start) |

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
docker-compose logs

# Check specific service
docker-compose logs gateway

# Rebuild from scratch
make dev-clean
make dev-up
```

### Port Already in Use
```bash
# Error: "port is already allocated"
# Find conflicting container
docker ps | grep 8081  # (or 8082, 8083, 3306)

# Stop it
docker stop <container-id>

# Or stop all
make dev-down
```

### Database Schema Not Applied
```bash
# Recreate database with fresh schema
make clean-services  # Removes volumes
make start-all-services     # Recreates everything
```

### Tests Timing Out
```bash
# Clean Docker cache
docker system prune -a

# Clean Bazel cache
make clean

# Re-run tests
make test-all
```

### Containers Not Cleaning Up
```bash
# List all test containers
docker ps -a | grep sq-test

# Remove all test containers
docker ps -a | grep sq-test | awk '{print $1}' | xargs docker rm -f

# Remove all test networks
docker network ls | grep sq-test | awk '{print $1}' | xargs docker network rm

# Or use docker-compose to clean up (from any test directory)
cd test/integration/gateway
docker-compose down -v

# Nuclear option (removes everything)
docker system prune -af --volumes
```

---

## Architecture

### Consistency Across Testing

**Both automated and manual testing use the same setup:**

1. **Docker Compose** defines services (`docker-compose.yml`)
2. **Automated tests** use docker-compose programmatically via `testutil.ComposeStack`
3. **Manual testing** uses `make start-all-services` (same docker-compose files)

**Result:** Tests and manual testing are identical - no surprises!

### Test vs Manual

| Aspect | Automated Tests | Manual Testing |
|--------|----------------|----------------|
| **Setup** | docker-compose (auto) | `make start-all-services` |
| **Containers** | Per test suite | Persistent |
| **Cleanup** | Automatic | `make stop-all-services` |
| **Container names** | `sq-test-{context}-{id}` | `submitqueue-{service}` |
| **Use case** | CI, pre-commit | Debugging, exploration |
| **Speed** | Fast (parallel) | Fast (persistent) |

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
    
    // Verify in database
    var result string
    s.db.QueryRow("SELECT ...").Scan(&result)
    assert.Equal(s.T(), "expected", result)
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
- [CLAUDE.md](../CLAUDE.md) - Development guidelines
- [example/server/docker-compose.yml](../example/server/docker-compose.yml) - Full stack service definitions
- [example/server/gateway/docker-compose.yml](../example/server/gateway/docker-compose.yml) - Gateway isolation
- [example/server/orchestrator/docker-compose.yml](../example/server/orchestrator/docker-compose.yml) - Orchestrator isolation
