# SubmitQueue

## Services

Submit Queue consists of three main services:

- **Gateway**: Entry point for external requests (port 8081)
- **Orchestrator**: Coordinates job execution (port 8082)
- **Speculator**: Performs speculative builds (port 8083)

## gRPC API

Each service has its own proto definitions and exposes its own gRPC API:
- **SubmitQueueGateway**: Gateway API with Ping method (port 8081)
- **SubmitQueueOrchestrator**: Orchestrator API with Ping method (port 8082)
- **SubmitQueueSpeculator**: Speculator API with Ping method (port 8083)

### Quick Start

Build and run a service:
```bash
# Using Make (recommended)
make run-gateway

# Using Go directly
go run examples/server/gateway/main.go

# Using Bazel (with direnv)
bazel run //examples/server/gateway:gateway

# Or without direnv
./tools/bazel run //examples/server/gateway:gateway
```

Test the service:
```bash
# Using the provided client (in another terminal)
make run-client-gateway MESSAGE="hello"

# Or using Go directly
go run examples/client/gateway/main.go -message "hello"

# Or using grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.devexp.submitqueue.gateway.SubmitQueueGateway/Ping
```

For detailed instructions, see [examples/README.md](examples/README.md).

## Project Structure

See [docs/architecture/STRUCTURE.md](docs/architecture/STRUCTURE.md) for a detailed breakdown of the project structure.

## Architecture

The project follows clean architecture principles with clear separation of concerns:

- **Controllers** (`core/controller/`): Pure business logic, independent of transport layer
  - Only depend on logger, metrics, and protobuf types
  - Example: `PingController` handles ping business logic

- **Server Adapters** (`examples/server/`): gRPC transport layer
  - Wrap controllers and implement gRPC service interfaces
  - Handle protocol-specific concerns (e.g., `UnimplementedServiceServer`)

- **Observability**: Built-in logging and metrics
  - Structured logging with [Zap](https://github.com/uber-go/zap)
  - Metrics collection with [Tally](https://github.com/uber-go/tally)
  - Development servers use human-readable console logging

## Development

### Prerequisites

- **Go 1.24 or later** (optional, Bazel manages its own Go toolchain)
- **protoc** and Go plugins (optional, only needed if modifying proto files)
- **grpcurl** (optional, for manual testing)
- **direnv** (recommended, to automatically load `.envrc`)

**Note**: The project includes `./tools/bazel` (bazelisk wrapper) and `.bazelversion`, so you don't need to install Bazel or Bazelisk separately.

#### Using direnv (Recommended)

Install direnv and allow the `.envrc` file:
```bash
# macOS
brew install direnv

# Add to your shell profile (~/.zshrc or ~/.bashrc)
eval "$(direnv hook zsh)"  # or bash, fish, etc.

# In the project directory
direnv allow
```

With direnv enabled, you can use `bazel` directly instead of `./tools/bazel`.

Install optional tools:
```bash
# macOS
brew install protobuf grpcurl

# Install Go plugins (only if you need to regenerate proto files)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go@latest
```

**Note**: Proto files are committed to the repository, so you don't need protoc unless you're modifying `.proto` files.

### Building

#### Using Make (Recommended)

```bash
# Build everything
make build

# Generate proto files (proto files are committed, so this is optional)
make proto

# Run servers
make run-gateway        # Gateway on port 8081
make run-orchestrator   # Orchestrator on port 8082
make run-speculator     # Speculator on port 8083

# Run clients
make run-client-gateway MESSAGE="hello"
make run-client-orchestrator MESSAGE="hello"
make run-client-speculator MESSAGE="hello"

# Clean binaries
make clean

# Clean proto files (not normally needed)
make clean-proto
```

#### Using Go directly

```bash
# Generate proto files (if needed)
make proto

# Build everything
go build ./...

# Build example servers
go build -o bin/gateway_server ./examples/server/gateway/
go build -o bin/orchestrator_server ./examples/server/orchestrator/
go build -o bin/speculator_server ./examples/server/speculator/

# Build clients
go build -o bin/gateway_client ./examples/client/gateway/
go build -o bin/orchestrator_client ./examples/client/orchestrator/
go build -o bin/speculator_client ./examples/client/speculator/

# Run a server
./bin/gateway_server

# Run a client (in another terminal)
./bin/gateway_client -message "hello"
```

#### Using Bazel

The project uses **Bzlmod** (not WORKSPACE) for dependency management. Bazel version is pinned at 8.4.1 in `.bazelversion`.

The project includes `./tools/bazel` which automatically downloads the correct Bazel version. If you're using `direnv`, you can simply use `bazel` instead of `./tools/bazel`.

```bash
# Build everything (with direnv)
bazel build //...

# Or without direnv
./tools/bazel build //...

# Build specific components
bazel build //gateway/protopb
bazel build //examples/server/gateway:gateway
bazel build //examples/client/gateway:gateway

# Run a server
bazel run //examples/server/gateway:gateway

# Run a client
bazel run //examples/client/gateway:gateway -- -message "hello"

# Or use the Makefile (recommended)
make build
make run-gateway
```

**Note**:
- The repository uses Bzlmod for modern dependency management
- All generated proto files are committed to the repository
- With `direnv` enabled, use `bazel` directly; otherwise use `./tools/bazel`

### Running Services

See the [examples directory](examples/) for examples of running each service.

## Development Workflow

### Modifying Proto Files

When you make changes to `.proto` files, you need to regenerate the protobuf code. The project generates three types of files for each proto:

1. `*.pb.go` - Standard protobuf code (protoc-gen-go)
2. `*_grpc.pb.go` - gRPC service code (protoc-gen-go-grpc)
3. `*.pb.yarpc.go` - YARPC service code (protoc-gen-yarpc-go) for Uber's RPC framework

**Step-by-step process:**

1. Edit the proto file (e.g., `gateway/proto/gateway.proto`)

2. Regenerate the protobuf code:
   ```bash
   make proto
   ```

3. Update service implementations if you added/changed fields:
   ```bash
   # For example, if you added a field to PingResponse:
   # Edit gateway/core/controller/ping.go to populate the new field
   ```

4. Update client examples to display new fields:
   ```bash
   # Edit examples/client/gateway/main.go to show the new field
   ```

5. Rebuild and test:
   ```bash
   make build
   ```

**Example: Adding a new field to PingResponse**

```protobuf
// In gateway/proto/gateway.proto
message PingResponse {
    string message = 1;
    string service_name = 2;
    int64 timestamp = 3;
    string hostname = 4;
    string new_field = 5;  // New field added
}
```

After editing the proto:
```bash
# Regenerate proto files
make proto

# The following files are updated automatically:
# - gateway/protopb/gateway.pb.go
# - gateway/protopb/gateway_grpc.pb.go
# - gateway/protopb/gateway.pb.yarpc.go

# Now update the controller implementation
# Edit gateway/core/controller/ping.go to populate the new field in the PingResponse
```

### Testing

#### Manual Testing

1. **Start a server:**
   ```bash
   make run-gateway
   ```

2. **Test with the client (in another terminal):**
   ```bash
   make run-client-gateway MESSAGE="test message"
   ```

3. **Or use grpcurl:**
   ```bash
   grpcurl -plaintext -d '{"message": "hello"}' \
     localhost:8081 uber.devexp.submitqueue.gateway.SubmitQueueGateway/Ping
   ```

#### Testing All Services

```bash
# Terminal 1: Start gateway
make run-gateway

# Terminal 2: Start orchestrator
make run-orchestrator

# Terminal 3: Start speculator
make run-speculator

# Terminal 4: Test each service
make run-client-gateway MESSAGE="test gateway"
make run-client-orchestrator MESSAGE="test orchestrator"
make run-client-speculator MESSAGE="test speculator"
```

#### Using Bazel for Testing

```bash
# Run tests (when tests are added)
bazel test //...

# Or with make
make test
```

### Common Development Tasks

#### Adding a New RPC Method

1. **Update the proto file:**
   ```protobuf
   // In gateway/proto/gateway.proto
   service SubmitQueueGateway {
       rpc Ping(PingRequest) returns (PingResponse) {}
       rpc NewMethod(NewRequest) returns (NewResponse) {}  // Add new method
   }

   message NewRequest { ... }
   message NewResponse { ... }
   ```

2. **Regenerate proto files:**
   ```bash
   make proto
   ```

3. **Implement the method in the controller:**
   ```go
   // In gateway/core/controller/ (create a new controller file)
   type NewController struct {
       logger       *zap.Logger
       metricsScope tally.Scope
   }

   func NewNewController(logger *zap.Logger, scope tally.Scope) *NewController {
       if logger == nil {
           logger = zap.NewNop()
       }
       if scope == nil {
           scope = tally.NoopScope
       }
       return &NewController{logger: logger, metricsScope: scope}
   }

   func (c *NewController) NewMethod(ctx context.Context, req *pb.NewRequest) (*pb.NewResponse, error) {
       // Business logic here
       c.logger.Info("new method called")
       c.metricsScope.Counter("new_method_total").Inc(1)
       // ...
   }
   ```

4. **Create server wrapper in example:**
   ```go
   // In examples/server/gateway/main.go
   // Add method delegation to GatewayServer struct:
   func (s *GatewayServer) NewMethod(ctx context.Context, req *pb.NewRequest) (*pb.NewResponse, error) {
       return s.newController.NewMethod(ctx, req)
   }
   ```

5. **Update clients to call the new method**

6. **Rebuild and test:**
   ```bash
   make build
   ```

#### Cleaning Up

```bash
# Remove built binaries
make clean

# Remove generated proto files (regenerate with 'make proto')
make clean-proto

# Clean Bazel cache
bazel clean
```

### Troubleshooting

**Proto generation fails:**
- Ensure all three protoc plugins are installed (see Prerequisites)
- Check that `protoc` is in your PATH: `which protoc`
- Check plugin versions: `protoc-gen-go --version`

**Build fails after proto changes:**
- Run `make proto` to regenerate proto files
- Ensure you updated all service implementations for new/changed fields
- Check import paths in generated files match your module path

**Server won't start:**
- Check if port is already in use: `lsof -i :8081`
- Kill existing process: `lsof -ti tcp:8081 | xargs kill -9`

**Bazel build issues:**
- Bazel version is pinned to 8.4.1 in `.bazelversion`
- With `direnv`, you can use `bazel` directly; otherwise use `./tools/bazel`
- Try `bazel shutdown` (or `./tools/bazel shutdown`) and rebuild
- The wrapper automatically downloads the correct Bazel version
