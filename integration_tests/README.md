# End-to-End Tests

This directory contains end-to-end (e2e) tests for the SubmitQueue system. These tests validate that all services work together correctly as a complete system.

**Note:** For testing individual services in isolation, see the integration tests in each service's directory:
- `gateway/integration_tests/` - Gateway service tests
- `orchestrator/integration_tests/` - Orchestrator service tests
- `speculator/integration_tests/` - Speculator service tests

## Running Tests

### Prerequisites

All servers must be running before executing the tests. Start them in separate terminals or in the background:

```bash
# Build everything first
make build

# Start all servers
./bin/gateway_server &
./bin/orchestrator_server &
./bin/speculator_server &
```

### Using Make (Recommended)

```bash
# Run end-to-end tests (all servers must be running)
make e2e-test

# Run integration tests for individual services
make integration-test-gateway        # Just Gateway
make integration-test-orchestrator   # Just Orchestrator
make integration-test-speculator     # Just Speculator
make integration-test                # All services

```

### Using Bazel directly

```bash
# Run end-to-end tests
./tools/bazel test //integration_tests:e2e_test --test_output=all

# Run individual service integration tests
./tools/bazel test //gateway:integration_test --test_output=all
./tools/bazel test //orchestrator:integration_test --test_output=all
./tools/bazel test //speculator:integration_test --test_output=all

# Run all service integration tests
./tools/bazel test //gateway:integration_test //orchestrator:integration_test //speculator:integration_test --test_output=all

# The tests are tagged as 'manual' so they won't run with 'bazel test //...'
# This is intentional since they require servers to be running
```

## Test Structure

### End-to-End Tests (this directory)

- `TestEndToEndAllServices` - Validates all services are running and responding correctly

### Service Integration Tests (in each service directory)

- `gateway/integration_tests/` - Tests Gateway service in isolation
- `orchestrator/integration_tests/` - Tests Orchestrator service in isolation
- `speculator/integration_tests/` - Tests Speculator service in isolation

## Adding New Tests

### 1. Add a test for a new API endpoint in an existing service

Add the test to the service's `integration_tests/` folder (e.g., create `gateway/integration_tests/submit_test.go`):

```go
func TestNewMethod(t *testing.T) {
    addr := getEnvOrDefault("GATEWAY_ADDR", "localhost:8081")

    conn, err := waitForServer(t, addr, serverReadyTimeout)
    if err != nil {
        t.Fatalf("Gateway server not ready: %v", err)
    }
    defer conn.Close()

    client := pb.NewSubmitQueueGatewayClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
    defer cancel()

    req := &pb.NewMethodRequest{
        Field: "value",
    }

    resp, err := client.NewMethod(ctx, req)
    if err != nil {
        t.Fatalf("NewMethod failed: %v", err)
    }

    // Add your assertions here
}
```

### 2. Add integration tests for a new service

1. Create `newservice/integration_tests/` folder
2. Add test files like `newservice/integration_tests/ping_test.go`
3. Add `go_test` rule to `newservice/BUILD.bazel`:
```python
go_test(
    name = "integration_test",
    srcs = ["integration_tests/ping_test.go"],
    deps = [
        "//newservice/protopb",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
    ],
    tags = ["manual", "integration"],
)
```
4. Update Makefile to add `integration-test-newservice` target
5. Update CI workflow to start the new service and run its tests

### 3. Add end-to-end tests

For system-wide tests that involve multiple services, add them to `integration_tests/api_test.go`.

## Environment Variables

Tests support the following environment variables to customize server addresses:

- `GATEWAY_ADDR` - Gateway server address (default: `localhost:8081`)
- `ORCHESTRATOR_ADDR` - Orchestrator server address (default: `localhost:8082`)
- `SPECULATOR_ADDR` - Speculator server address (default: `localhost:8083`)

Example with Bazel:
```bash
GATEWAY_ADDR=10.0.0.1:8081 ./tools/bazel test //integration_tests:integration_tests --test_output=all
```

Example with Go:
```bash
GATEWAY_ADDR=10.0.0.1:8081 go test ./integration_tests -v
```

## CI Usage

In GitHub Actions CI, the workflow:
1. Builds all services
2. Starts all servers in the background
3. Runs integration tests
4. Stops servers

See `.github/workflows/build_and_test.yml` for the full workflow.

## Troubleshooting

**Tests fail with "connection refused":**
- Ensure all servers are running
- Check that servers are listening on the expected ports: `lsof -i :8081`
- Verify servers are healthy using the clients: `./bin/gateway_client -message test`

**Tests timeout:**
- Servers may be slow to start
- Increase `serverReadyTimeout` in the test code
- Check server logs for startup errors

**Import errors:**
- Run `go mod tidy` to ensure all dependencies are downloaded
- Regenerate proto files if needed: `make proto`
