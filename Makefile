.PHONY: proto build test clean run-gateway run-orchestrator run-speculator run-client-gateway run-client-orchestrator run-client-speculator

# Bazel wrapper
BAZEL = ./tools/bazel

# Generate protobuf files for all services using protoc
proto:
	@echo "Generating protobuf files with protoc..."
	@protoc --go_out=gateway/protopb --go_opt=paths=source_relative \
	  --go-grpc_out=gateway/protopb --go-grpc_opt=paths=source_relative \
	  --yarpc-go_out=gateway/protopb --yarpc-go_opt=paths=source_relative \
	  --proto_path=gateway/proto gateway/proto/gateway.proto
	@protoc --go_out=orchestrator/protopb --go_opt=paths=source_relative \
	  --go-grpc_out=orchestrator/protopb --go-grpc_opt=paths=source_relative \
	  --yarpc-go_out=orchestrator/protopb --yarpc-go_opt=paths=source_relative \
	  --proto_path=orchestrator/proto orchestrator/proto/orchestrator.proto
	@protoc --go_out=speculator/protopb --go_opt=paths=source_relative \
	  --go-grpc_out=speculator/protopb --go-grpc_opt=paths=source_relative \
	  --yarpc-go_out=speculator/protopb --yarpc-go_opt=paths=source_relative \
	  --proto_path=speculator/proto speculator/proto/speculator.proto
	@echo "Protobuf files generated successfully!"

# Build all services and examples using Bazel
build:
	@echo "Building all services with Bazel..."
	@$(BAZEL) build //gateway:gateway
	@$(BAZEL) build //orchestrator:orchestrator
	@$(BAZEL) build //speculator:speculator
	@echo "Building example servers and clients..."
	@$(BAZEL) build //examples/server/gateway:gateway
	@$(BAZEL) build //examples/server/orchestrator:orchestrator
	@$(BAZEL) build //examples/server/speculator:speculator
	@$(BAZEL) build //examples/client/gateway:gateway
	@$(BAZEL) build //examples/client/orchestrator:orchestrator
	@$(BAZEL) build //examples/client/speculator:speculator
	@echo "Copying binaries to ./bin/..."
	@mkdir -p bin
	@cp -f bazel-bin/examples/server/gateway/gateway_/gateway bin/gateway_server 2>/dev/null || \
	 cp -f bazel-bin/examples/server/gateway/gateway bin/gateway_server 2>/dev/null || true
	@cp -f bazel-bin/examples/server/orchestrator/orchestrator_/orchestrator bin/orchestrator_server 2>/dev/null || \
	 cp -f bazel-bin/examples/server/orchestrator/orchestrator bin/orchestrator_server 2>/dev/null || true
	@cp -f bazel-bin/examples/server/speculator/speculator_/speculator bin/speculator_server 2>/dev/null || \
	 cp -f bazel-bin/examples/server/speculator/speculator bin/speculator_server 2>/dev/null || true
	@cp -f bazel-bin/examples/client/gateway/gateway_/gateway bin/gateway_client 2>/dev/null || \
	 cp -f bazel-bin/examples/client/gateway/gateway bin/gateway_client 2>/dev/null || true
	@cp -f bazel-bin/examples/client/orchestrator/orchestrator_/orchestrator bin/orchestrator_client 2>/dev/null || \
	 cp -f bazel-bin/examples/client/orchestrator/orchestrator bin/orchestrator_client 2>/dev/null || true
	@cp -f bazel-bin/examples/client/speculator/speculator_/speculator bin/speculator_client 2>/dev/null || \
	 cp -f bazel-bin/examples/client/speculator/speculator bin/speculator_client 2>/dev/null || true
	@echo "Build complete! Binaries are in ./bin/"

# Run tests using Bazel
test:
	@$(BAZEL) test //...

# Clean generated files and binaries
clean:
	@echo "Cleaning with Bazel..."
	@$(BAZEL) clean
	@rm -rf bin/
	@echo "Clean complete!"

# Clean generated proto files (normally not needed as they are checked in)
clean-proto:
	@echo "Cleaning generated proto files..."
	@rm -rf gateway/protopb/*.pb.go
	@rm -rf orchestrator/protopb/*.pb.go
	@rm -rf speculator/protopb/*.pb.go
	@echo "Proto clean complete!"

# Run gateway server using Bazel
run-gateway:
	@echo "Starting gateway server on port 8081..."
	@$(BAZEL) run //examples/server/gateway:gateway

# Run orchestrator server using Bazel
run-orchestrator:
	@echo "Starting orchestrator server on port 8082..."
	@$(BAZEL) run //examples/server/orchestrator:orchestrator

# Run speculator server using Bazel
run-speculator:
	@echo "Starting speculator server on port 8083..."
	@$(BAZEL) run //examples/server/speculator:speculator

# Run gateway client using Bazel
run-client-gateway:
	@$(BAZEL) run //examples/client/gateway:gateway -- -addr $(or $(SERVER_ADDR),localhost:8081) -message "$(or $(MESSAGE),ping)"

# Run orchestrator client using Bazel
run-client-orchestrator:
	@$(BAZEL) run //examples/client/orchestrator:orchestrator -- -addr $(or $(SERVER_ADDR),localhost:8082) -message "$(or $(MESSAGE),ping)"

# Run speculator client using Bazel
run-client-speculator:
	@$(BAZEL) run //examples/client/speculator:speculator -- -addr $(or $(SERVER_ADDR),localhost:8083) -message "$(or $(MESSAGE),ping)"

# Install dependencies (for go mod users)
deps:
	@echo "Installing Go dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies installed!"

# Bazel query helpers
query-targets:
	@$(BAZEL) query //...

query-deps:
	@$(BAZEL) query 'deps(//examples/server/gateway:gateway)'

# Help
help:
	@echo "Available targets:"
	@echo "  make proto              - Generate protobuf files (using Bazel)"
	@echo "  make build              - Build all services and examples (using Bazel)"
	@echo "  make test               - Run tests (using Bazel)"
	@echo "  make clean              - Clean generated files and binaries"
	@echo "  make run-gateway        - Run gateway server (port 8081)"
	@echo "  make run-orchestrator   - Run orchestrator server (port 8082)"
	@echo "  make run-speculator     - Run speculator server (port 8083)"
	@echo "  make run-client-gateway - Run gateway client (use SERVER_ADDR= and MESSAGE=)"
	@echo "  make run-client-orchestrator - Run orchestrator client"
	@echo "  make run-client-speculator - Run speculator client"
	@echo "  make deps               - Install Go dependencies"
	@echo "  make query-targets      - List all Bazel targets"
	@echo "  make query-deps         - Show dependencies for gateway server"
	@echo ""
	@echo "Examples:"
	@echo "  make run-gateway"
	@echo "  make run-client-gateway SERVER_ADDR=localhost:8081 MESSAGE='hello gateway'"
