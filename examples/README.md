# How to Run gRPC Servers

This directory contains example gRPC servers and clients for each service in the submitqueue project.

## Services

Each service has its own proto definitions and gRPC API:

- **Gateway**: GatewayService API (port 8081)
- **Orchestrator**: OrchestratorService API (port 8082)
- **Speculator**: SpeculatorService API (port 8083)

## Building and Running

### Using Make (Recommended)

The easiest way to build and run the servers:

```bash
# Build everything (including proto generation if needed)
make build

# Run specific servers
make run-gateway        # Runs on port 8081
make run-orchestrator   # Runs on port 8082
make run-speculator     # Runs on port 8083

# Run clients
make run-client-gateway MESSAGE="hello"
make run-client-orchestrator MESSAGE="hello"
make run-client-speculator MESSAGE="hello"

# Clean everything
make clean
```

### Using Bazel

Build a specific server:
```bash
# Build gateway server
bazel build //examples/server/gateway:gateway

# Build orchestrator server
bazel build //examples/server/orchestrator:orchestrator

# Build speculator server
bazel build //examples/server/speculator:speculator

# Build clients
bazel build //examples/client/gateway:gateway
bazel build //examples/client/orchestrator:orchestrator
bazel build //examples/client/speculator:speculator
```

Run a specific server:
```bash
# Run gateway server
bazel run //examples/server/gateway:gateway

# Run orchestrator server
bazel run //examples/server/orchestrator:orchestrator

# Run speculator server
bazel run //examples/server/speculator:speculator

# Or use '.' from the directory
cd examples/server/gateway && bazel run .
cd examples/server/orchestrator && bazel run .
cd examples/server/speculator && bazel run .
```

Run clients:
```bash
# Run gateway client
bazel run //examples/client/gateway:gateway -- -message "hello"

# Run orchestrator client
bazel run //examples/client/orchestrator:orchestrator -- -message "hello"

# Run speculator client
bazel run //examples/client/speculator:speculator -- -message "hello"

# Or use '.' from the directory
cd examples/client/gateway && bazel run . -- -message "hello"
cd examples/client/orchestrator && bazel run . -- -message "hello"
cd examples/client/speculator && bazel run . -- -message "hello"
```

### Using Go directly

You can also run the servers directly with Go (from the repository root):

```bash
# Run gateway server
go run examples/server/gateway/main.go

# Run orchestrator server
go run examples/server/orchestrator/main.go

# Run speculator server
go run examples/server/speculator/main.go

# Run clients
go run examples/client/gateway/main.go -message "hello"
go run examples/client/orchestrator/main.go -message "hello"
go run examples/client/speculator/main.go -message "hello"
```

## Testing the Services

### Using the Go Clients

Build and run the clients:
```bash
# Using Make
make run-client-gateway MESSAGE="test from gateway client"
make run-client-orchestrator MESSAGE="test from orchestrator client"
make run-client-speculator MESSAGE="test from speculator client"

# Using Go
go run examples/client/gateway/main.go -addr localhost:8081 -message "hello"
go run examples/client/orchestrator/main.go -addr localhost:8082 -message "hello"
go run examples/client/speculator/main.go -addr localhost:8083 -message "hello"
```

Client flags:
- `-addr`: Server address (default: service-specific port)
- `-message`: Message to send in the ping request
- `-timeout`: Request timeout (default: 5s)

### Using grpcurl

Install grpcurl if you don't have it:
```bash
brew install grpcurl  # macOS
# or
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

Test the ping service:
```bash
# Test gateway (port 8081)
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.devexp.submitqueue.gateway.GatewayService/Ping

# Test orchestrator (port 8082)
grpcurl -plaintext -d '{"message": "hello"}' localhost:8082 uber.devexp.submitqueue.orchestrator.OrchestratorService/Ping

# Test speculator (port 8083)
grpcurl -plaintext -d '{"message": "hello"}' localhost:8083 uber.devexp.submitqueue.speculator.SpeculatorService/Ping
```

List available services:
```bash
grpcurl -plaintext localhost:8081 list
grpcurl -plaintext localhost:8082 list
grpcurl -plaintext localhost:8083 list
```

Describe a service:
```bash
grpcurl -plaintext localhost:8081 describe uber.devexp.submitqueue.gateway.GatewayService
grpcurl -plaintext localhost:8082 describe uber.devexp.submitqueue.orchestrator.OrchestratorService
grpcurl -plaintext localhost:8083 describe uber.devexp.submitqueue.speculator.SpeculatorService
```

## API Reference

### Service Interfaces

Each service exposes a Ping method with the same request/response structure but different service names:

#### Gateway Service

**Service**: `uber.devexp.submitqueue.gateway.GatewayService`
**Proto**: `gateway/proto/gateway.proto`

#### Orchestrator Service

**Service**: `uber.devexp.submitqueue.orchestrator.OrchestratorService`
**Proto**: `orchestrator/proto/orchestrator.proto`

#### Speculator Service

**Service**: `uber.devexp.submitqueue.speculator.SpeculatorService`
**Proto**: `speculator/proto/speculator.proto`

### Ping Method

**Request:**
```protobuf
message PingRequest {
    string message = 1;  // Optional message to include in the ping
}
```

**Response:**
```protobuf
message PingResponse {
    string message = 1;       // The response message
    string service_name = 2;  // The service name that handled the request
    int64 timestamp = 3;      // Timestamp of when the ping was received
}
```

**Example:**
```bash
grpcurl -plaintext -d '{"message": "test"}' localhost:8081 uber.devexp.submitqueue.gateway.GatewayService/Ping
```

Expected response:
```json
{
  "message": "echo: test",
  "service_name": "gateway",
  "timestamp": 1705234567
}
```

## Project Structure

Each service directory (in the repository root) contains:
- `proto/`: Protocol buffer definitions
  - `<service>.proto`: Service API definition
- `protopb/`: Generated Go code (committed to repository)
  - `*.pb.go`, `*_grpc.pb.go`, `*.pb.yarpc.go`
- `core/controller/`: Service implementation
  - `ping.go`: Ping service implementation
- `BUILD.bazel`: Bazel build rules

The `examples/` directory contains:
- `server/`: Example server implementations
  - `gateway/`, `orchestrator/`, `speculator/`: Server examples
- `client/`: Example client implementations
  - `gateway/`, `orchestrator/`, `speculator/`: Client examples

## Using the Services as a Library

The proto packages are designed to be consumed as a library. To use them in your own code:

```go
import (
    gatewaypb "github.com/uber/submitqueue/gateway/protopb"
    orchestratorpb "github.com/uber/submitqueue/orchestrator/protopb"
    speculatorpb "github.com/uber/submitqueue/speculator/protopb"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

// Use the clients
conn, _ := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := gatewaypb.NewGatewayServiceClient(conn)
resp, _ := client.Ping(context.Background(), &gatewaypb.PingRequest{Message: "hello"})
```

The generated proto files are committed to the repository, so you can import and use them directly without needing to regenerate them.
