# Integration Tests

This directory contains hermetic integration tests for the SubmitQueue system. All infrastructure (MySQL, gRPC servers) is managed automatically via [Testcontainers-Go](https://golang.testcontainers.org/) — no manual setup required.

## Architecture

Tests run as a `testify/suite` that manages the full lifecycle:

1. **Docker network** is created for inter-container communication
2. **MySQL container** starts on the network (alias `mysql`), schema is applied
3. **Server containers** (gateway, orchestrator, speculator) are built from the actual `go_binary` targets in `examples/server/` and started on the network
4. **gRPC clients** connect to the mapped host ports
5. **Tests execute** against the real server binaries
6. **Cleanup** tears down all containers and the network

All servers listen on port `8080` inside their containers. Docker maps each to a random host port, so there are no port conflicts even when tests run in parallel. The fixed internal port also simplifies inter-service communication on the Docker network — services reach each other at `<alias>:8080` (e.g., `orchestrator:8080`).

## Files

| File | Purpose |
|------|---------|
| `suite_test.go` | Test suite with `SetupSuite`/`TearDownSuite` and all test methods |
| `servers.go` | Helpers to build Docker images from server binaries and start containers |
| `mysql.go` | MySQL container setup, schema application, and test logger |
| `BUILD.bazel` | Bazel test target with binary and schema data dependencies |

## Running Tests

```bash
# Run with Bazel
bazel test //integration_tests:integration_test --test_output=all

# Run with verbose output
bazel test //integration_tests:integration_test --test_output=all --test_arg=-test.v

# Run with Go (from repo root)
go test ./integration_tests -v
```

The test target is tagged `integration` (not `manual`), so it is discovered by `bazel test //integration_tests/...`.

## Test Cases

- `TestPingGateway` — Ping gateway, assert `service_name="gateway"`
- `TestPingOrchestrator` — Ping orchestrator, assert `service_name="orchestrator"`
- `TestPingSpeculator` — Ping speculator, assert `service_name="speculator"`
- `TestLandRequest` — Send `LandRequest` through gateway gRPC, assert `sqid` is returned

## Adding New Tests

Add a method to `IntegrationSuite` in `suite_test.go`:

```go
func (s *IntegrationSuite) TestNewEndpoint() {
    ctx := context.Background()
    resp, err := s.gatewayClient.NewMethod(ctx, &gatewaypb.NewRequest{...})
    require.NoError(s.T(), err)
    assert.Equal(s.T(), "expected", resp.Field)
}
```

If the servers need to communicate with each other, pass addresses via environment variables in `servers.go`:

```go
_, addr := startServerContainer(ctx, t, log, "gateway", map[string]string{
    "MYSQL_DSN":         "root:root@tcp(mysql:3306)/submitqueue?parseTime=true",
    "ORCHESTRATOR_ADDR": "orchestrator:8080",
    "SPECULATOR_ADDR":   "speculator:8080",
}, nw)
```

## Troubleshooting

**`$HOME is not defined`** — The Bazel sandbox doesn't set `HOME`. This is handled in `SetupSuite` by setting it to a temp directory.

**Ryuk reaper failure** — The Testcontainers reaper container may fail in Docker-in-Docker environments. This is handled by setting `TESTCONTAINERS_RYUK_DISABLED=true` in `SetupSuite`.

**Binary not found** — Ensure the `data` attribute in `BUILD.bazel` includes the server binary targets. Bazel places them in runfiles at `examples/server/<name>/<name>_/<name>`.

## TODO

- [ ] Speed up container setup (pre-built images, parallel container starts, image caching)
- [ ] Support Tracetest/Jaeger for trace-based assertions
