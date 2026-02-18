.PHONY: proto build test integration-test integration-test-gateway integration-test-orchestrator integration-test-speculator e2e-test gazelle clean run-all start-servers stop-servers run-gateway run-orchestrator run-speculator run-client-gateway run-client-orchestrator run-client-speculator

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

# Build everything in the project using Bazel
build:
	@echo "Building all targets with Bazel..."
	@$(BAZEL) build //...
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

# Run unit tests using Bazel (excludes integration tests which require running servers)
test:
	@echo "Running unit tests..."
	@$(BAZEL) test //... --test_tag_filters=-manual,-integration || echo "No unit tests found (only integration tests exist)"

# Generate/update BUILD.bazel files using Gazelle
gazelle:
	@echo "Running Gazelle to update BUILD files..."
	@$(BAZEL) run //:gazelle

# Run integration tests for a specific service (requires that service to be running)
integration-test-gateway:
	@echo "Running Gateway integration tests..."
	@$(BAZEL) test //gateway/integration_tests:integration_tests_test --test_output=all

integration-test-orchestrator:
	@echo "Running Orchestrator integration tests..."
	@$(BAZEL) test //orchestrator/integration_tests:integration_tests_test --test_output=all

integration-test-speculator:
	@echo "Running Speculator integration tests..."
	@$(BAZEL) test //speculator/integration_tests:integration_tests_test --test_output=all

# Run all service integration tests (requires all services to be running)
integration-test:
	@echo "Running all service integration tests..."
	@$(BAZEL) test //gateway/integration_tests:integration_tests_test //orchestrator/integration_tests:integration_tests_test //speculator/integration_tests:integration_tests_test --test_output=all

# Run end-to-end integration tests (hermetic, no manual server setup needed)
e2e-test:
	@echo "Running integration tests..."
	@$(BAZEL) test //integration_tests:integration_test --test_output=all

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

# Start all servers in background (for testing)
start-servers:
	@echo "Starting all servers in background..."
	@./bin/gateway_server > /tmp/gateway.log 2>&1 & echo $$! > /tmp/gateway.pid
	@./bin/orchestrator_server > /tmp/orchestrator.log 2>&1 & echo $$! > /tmp/orchestrator.pid
	@./bin/speculator_server > /tmp/speculator.log 2>&1 & echo $$! > /tmp/speculator.pid
	@sleep 2
	@echo "All servers started:"
	@echo "  Gateway (PID: $$(cat /tmp/gateway.pid)) - http://localhost:8081"
	@echo "  Orchestrator (PID: $$(cat /tmp/orchestrator.pid)) - http://localhost:8082"
	@echo "  Speculator (PID: $$(cat /tmp/speculator.pid)) - http://localhost:8083"
	@echo ""
	@echo "Logs:"
	@echo "  tail -f /tmp/gateway.log"
	@echo "  tail -f /tmp/orchestrator.log"
	@echo "  tail -f /tmp/speculator.log"
	@echo ""
	@echo "To stop: make stop-servers"

# Stop all background servers
stop-servers:
	@echo "Stopping all servers..."
	@if [ -f /tmp/gateway.pid ]; then kill $$(cat /tmp/gateway.pid) 2>/dev/null || true; rm -f /tmp/gateway.pid; fi
	@if [ -f /tmp/orchestrator.pid ]; then kill $$(cat /tmp/orchestrator.pid) 2>/dev/null || true; rm -f /tmp/orchestrator.pid; fi
	@if [ -f /tmp/speculator.pid ]; then kill $$(cat /tmp/speculator.pid) 2>/dev/null || true; rm -f /tmp/speculator.pid; fi
	@echo "All servers stopped"

# Run all servers (for testing) - starts servers, waits for Ctrl+C, then stops
run-all: start-servers
	@echo ""
	@echo "Press Ctrl+C to stop all servers..."
	@trap 'make stop-servers' INT; while true; do sleep 1; done

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
	@echo ""
	@echo "Build & Test:"
	@echo "  make proto              - Generate protobuf files"
	@echo "  make build              - Build all services and examples"
	@echo "  make test               - Run unit tests"
	@echo "  make gazelle            - Update BUILD.bazel files"
	@echo "  make clean              - Clean generated files and binaries"
	@echo ""
	@echo "Run Servers:"
	@echo "  make run-all            - Run all servers (Ctrl+C to stop)"
	@echo "  make start-servers      - Start all servers in background"
	@echo "  make stop-servers       - Stop all background servers"
	@echo "  make run-gateway        - Run gateway server (port 8081)"
	@echo "  make run-orchestrator   - Run orchestrator server (port 8082)"
	@echo "  make run-speculator     - Run speculator server (port 8083)"
	@echo ""
	@echo "Integration Tests (requires servers to be running):"
	@echo "  make integration-test-gateway      - Test Gateway service"
	@echo "  make integration-test-orchestrator - Test Orchestrator service"
	@echo "  make integration-test-speculator   - Test Speculator service"
	@echo "  make integration-test   - Test all services"
	@echo ""
	@echo "End-to-End Tests (hermetic, no setup needed):"
	@echo "  make e2e-test           - Run integration tests with Testcontainers"
	@echo ""
	@echo "Run Clients:"
	@echo "  make run-client-gateway - Run gateway client"
	@echo "  make run-client-orchestrator - Run orchestrator client"
	@echo "  make run-client-speculator - Run speculator client"
	@echo ""
	@echo "Other:"
	@echo "  make deps               - Install Go dependencies"
	@echo "  make query-targets      - List all Bazel targets"
	@echo ""
	@echo "Examples:"
	@echo "  # Start all servers and run integration tests"
	@echo "  make build && make start-servers && make integration-test && make stop-servers"
	@echo ""
	@echo "  # Run a single server"
	@echo "  make run-gateway"
	@echo ""
	@echo "  # Test with custom message"
	@echo "  make run-client-gateway MESSAGE='hello gateway'"
