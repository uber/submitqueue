# Bazel wrapper
BAZEL = ./tool/bazel

# Docker Compose wrapper
COMPOSE = docker-compose
COMPOSE_FILE = example/server/docker-compose.yml
GATEWAY_COMPOSE_FILE = example/server/gateway/docker-compose.yml
ORCHESTRATOR_COMPOSE_FILE = example/server/orchestrator/docker-compose.yml

# Fixed project name for local manual testing (tests use unique random names)
LOCAL_PROJECT = submitqueue

# Set REPO_ROOT for docker-compose
export REPO_ROOT := $(shell pwd)

.PHONY: build build-all-linux build-gateway-linux build-orchestrator-linux clean clean-proto deps e2e-test gazelle integration-test integration-test-extensions integration-test-gateway integration-test-orchestrator local-clean local-gateway-start local-gateway-stop local-init-schemas local-logs local-orchestrator-start local-orchestrator-stop local-ps local-restart local-start local-stop proto query-deps query-targets run-client-gateway run-client-orchestrator run-queue-admin test test-no-cache help


build: ## Build all services and examples
	@echo "Building all targets with Bazel..."
	@$(BAZEL) build //...
	@echo "Build complete!"

# Build Linux binaries required for Docker containers
build-all-linux: build-gateway-linux build-orchestrator-linux ## Build all Linux binaries for Docker
	@echo "All Linux binaries ready for Docker"

build-gateway-linux: ## Build Gateway Linux binary for Docker
	@echo "Building Gateway Linux binary for Docker..."
	@$(BAZEL) build --platforms=@rules_go//go/toolchain:linux_amd64 //example/server/gateway:gateway
	@mkdir -p .docker-bin
	@cp -f bazel-bin/example/server/gateway/gateway_/gateway .docker-bin/gateway 2>/dev/null || \
	 cp -f bazel-bin/example/server/gateway/gateway .docker-bin/gateway
	@echo "Gateway Linux binary ready at .docker-bin/gateway"

build-orchestrator-linux: ## Build Orchestrator Linux binary for Docker
	@echo "Building Orchestrator Linux binary for Docker..."
	@$(BAZEL) build --platforms=@rules_go//go/toolchain:linux_amd64 //example/server/orchestrator:orchestrator
	@mkdir -p .docker-bin
	@cp -f bazel-bin/example/server/orchestrator/orchestrator_/orchestrator .docker-bin/orchestrator 2>/dev/null || \
	 cp -f bazel-bin/example/server/orchestrator/orchestrator .docker-bin/orchestrator
	@echo "Orchestrator Linux binary ready at .docker-bin/orchestrator"

clean: ## Clean generated files and binaries
	@echo "Cleaning with Bazel..."
	@$(BAZEL) clean
	@rm -rf bin/
	@echo "Clean complete!"

clean-proto: ## Clean generated proto files
	@echo "Cleaning generated proto files..."
	@rm -rf gateway/protopb/*.pb.go
	@rm -rf orchestrator/protopb/*.pb.go
	@echo "Proto clean complete!"

deps: ## Install Go dependencies
	@echo "Installing Go dependencies..."
	@go mod download
	@go mod tidy
	@echo "Dependencies installed!"

e2e-test: build-all-linux ## Run end-to-end tests (hermetic, auto-builds binaries)
	@echo "Running end-to-end tests..."
	@$(BAZEL) test //test/e2e:e2e_test --test_output=streamed

gazelle: ## Update BUILD.bazel files
	@echo "Running Gazelle to update BUILD files..."
	@$(BAZEL) run //:gazelle

integration-test: build-all-linux ## Run all integration tests (auto-builds binaries)
	@echo "Running all integration tests..."
	@$(BAZEL) test //test/integration/... --test_output=streamed

integration-test-extensions: ## Run extension integration tests
	@echo "Running extension integration tests..."
	@$(BAZEL) test //test/integration/extension/... --test_output=streamed

integration-test-gateway: build-gateway-linux ## Run Gateway integration tests (auto-builds binary)
	@echo "Running Gateway integration tests..."
	@$(BAZEL) test //test/integration/gateway:gateway_test --test_output=streamed

integration-test-orchestrator: build-orchestrator-linux ## Run Orchestrator integration tests (auto-builds binary)
	@echo "Running Orchestrator integration tests..."
	@$(BAZEL) test //test/integration/orchestrator:orchestrator_test --test_output=streamed

local-clean: ## Stop and remove all local services, volumes, and images
	@echo "Cleaning all services and data..."
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) down -v --rmi local
	@echo "All services, volumes, and images removed."

local-gateway-start: build-gateway-linux ## Start Gateway service locally (Gateway + 2 MySQL databases)
	@echo "Starting Gateway with docker-compose..."
	@$(COMPOSE) -f $(GATEWAY_COMPOSE_FILE) -p $(LOCAL_PROJECT) up -d --build --wait
	@echo "Applying database schemas..."
	@$(MAKE) -s local-init-schemas
	@echo ""
	@echo "✅ Gateway is running!"
	@echo ""
	@$(COMPOSE) -f $(GATEWAY_COMPOSE_FILE) -p $(LOCAL_PROJECT) ps
	@echo ""
	@echo "Gateway gRPC port: $$(docker port $(LOCAL_PROJECT)-gateway-service-1 8080 2>/dev/null | cut -d: -f2 || echo 'unknown')"
	@echo "MySQL App port:    $$(docker port $(LOCAL_PROJECT)-mysql-app-1 3306 2>/dev/null | cut -d: -f2 || echo 'unknown')"
	@echo "MySQL Queue port:  $$(docker port $(LOCAL_PROJECT)-mysql-queue-1 3306 2>/dev/null | cut -d: -f2 || echo 'unknown')"

local-gateway-stop: ## Stop Gateway service
	@echo "Stopping Gateway services..."
	@$(COMPOSE) -f $(GATEWAY_COMPOSE_FILE) -p $(LOCAL_PROJECT) down
	@echo "Gateway services stopped."

local-init-schemas: ## Manually apply all database schemas
	@echo "Applying storage schema to mysql-app..."
	@for file in extension/storage/mysql/schema/*.sql; do \
		echo "  - Applying $$(basename $$file)..."; \
		docker exec -i $(LOCAL_PROJECT)-mysql-app-1 mysql -uroot -proot submitqueue < $$file 2>&1 | grep -v "Using a password" || true; \
	done
	@echo "Applying counter schema to mysql-app..."
	@for file in extension/counter/mysql/schema/*.sql; do \
		echo "  - Applying $$(basename $$file)..."; \
		docker exec -i $(LOCAL_PROJECT)-mysql-app-1 mysql -uroot -proot submitqueue < $$file 2>&1 | grep -v "Using a password" || true; \
	done
	@echo "Applying queue schema to mysql-queue..."
	@for file in extension/queue/mysql/schema/*.sql; do \
		echo "  - Applying $$(basename $$file)..."; \
		docker exec -i $(LOCAL_PROJECT)-mysql-queue-1 mysql -uroot -proot submitqueue < $$file 2>&1 | grep -v "Using a password" || true; \
	done
	@echo "✅ All schemas applied successfully"

local-logs: ## View logs from all running services
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) logs -f

local-orchestrator-start: build-orchestrator-linux ## Start Orchestrator service locally (Orchestrator + 2 MySQL databases)
	@echo "Starting Orchestrator with docker-compose..."
	@$(COMPOSE) -f $(ORCHESTRATOR_COMPOSE_FILE) -p $(LOCAL_PROJECT) up -d --build --wait
	@echo "Applying database schemas..."
	@$(MAKE) -s local-init-schemas
	@echo ""
	@echo "✅ Orchestrator is running!"
	@echo ""
	@$(COMPOSE) -f $(ORCHESTRATOR_COMPOSE_FILE) -p $(LOCAL_PROJECT) ps
	@echo ""
	@echo "Orchestrator gRPC port: $$(docker port $(LOCAL_PROJECT)-orchestrator-service-1 8080 2>/dev/null | cut -d: -f2 || echo 'unknown')"
	@echo "MySQL App port:         $$(docker port $(LOCAL_PROJECT)-mysql-app-1 3306 2>/dev/null | cut -d: -f2 || echo 'unknown')"
	@echo "MySQL Queue port:       $$(docker port $(LOCAL_PROJECT)-mysql-queue-1 3306 2>/dev/null | cut -d: -f2 || echo 'unknown')"

local-orchestrator-stop: ## Stop Orchestrator service
	@echo "Stopping Orchestrator services..."
	@$(COMPOSE) -f $(ORCHESTRATOR_COMPOSE_FILE) -p $(LOCAL_PROJECT) down
	@echo "Orchestrator services stopped."

local-ps: ## Show running containers and their ports
	@echo "Running containers and ports:"
	@echo ""
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) ps
	@echo ""
	@echo "📡 Service Endpoints:"
	@echo "  Gateway gRPC:      localhost:$$(docker port $(LOCAL_PROJECT)-gateway-service-1 8080 2>/dev/null | cut -d: -f2 || echo 'not running')"
	@echo "  Orchestrator gRPC: localhost:$$(docker port $(LOCAL_PROJECT)-orchestrator-service-1 8080 2>/dev/null | cut -d: -f2 || echo 'not running')"
	@echo ""
	@echo "🗄️  Database Endpoints:"
	@echo "  MySQL App:    localhost:$$(docker port $(LOCAL_PROJECT)-mysql-app-1 3306 2>/dev/null | cut -d: -f2 || echo 'not running')"
	@echo "  MySQL Queue:  localhost:$$(docker port $(LOCAL_PROJECT)-mysql-queue-1 3306 2>/dev/null | cut -d: -f2 || echo 'not running')"
	@echo ""
	@echo "💡 Usage:"
	@echo "  # Connect to MySQL App DB"
	@echo "  mysql -h127.0.0.1 -P$$(docker port $(LOCAL_PROJECT)-mysql-app-1 3306 2>/dev/null | cut -d: -f2 || echo 'PORT') -uroot -proot submitqueue"
	@echo ""
	@echo "  # Call Gateway gRPC"
	@echo "  grpcurl -plaintext -d '{\"message\":\"test\"}' localhost:$$(docker port $(LOCAL_PROJECT)-gateway-service-1 8080 2>/dev/null | cut -d: -f2 || echo 'PORT') submitqueue.SubmitQueueGateway/Ping"
	@echo ""
	@echo "  # View logs"
	@echo "  make local-logs"

local-restart: build-all-linux ## Restart all services (rebuild and restart)
	@echo "Restarting all services..."
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) restart
	@echo "Services restarted!"
	@make local-ps

local-start: build-all-linux ## Start full stack (Gateway + Orchestrator + MySQL)
	@echo "Starting full stack with docker-compose..."
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) up -d --build --wait
	@echo "Applying database schemas..."
	@$(MAKE) -s local-init-schemas
	@echo ""
	@echo "✅ Full stack is running!"
	@echo ""
	@make local-ps

local-stop: ## Stop all services (keep data)
	@echo "Stopping all services..."
	@$(COMPOSE) -f $(COMPOSE_FILE) -p $(LOCAL_PROJECT) down
	@echo "Services stopped. Data volumes preserved."

proto: ## Generate protobuf files from .proto definitions
	@echo "Generating protobuf files with protoc..."
	@protoc --go_out=gateway/protopb --go_opt=paths=source_relative \
	  --go-grpc_out=gateway/protopb --go-grpc_opt=paths=source_relative \
	  --yarpc-go_out=gateway/protopb --yarpc-go_opt=paths=source_relative \
	  --proto_path=gateway/proto gateway/proto/gateway.proto
	@protoc --go_out=orchestrator/protopb --go_opt=paths=source_relative \
	  --go-grpc_out=orchestrator/protopb --go-grpc_opt=paths=source_relative \
	  --yarpc-go_out=orchestrator/protopb --yarpc-go_opt=paths=source_relative \
	  --proto_path=orchestrator/proto orchestrator/proto/orchestrator.proto
	@echo "Protobuf files generated successfully!"

# Bazel query helpers
query-deps:
	@$(BAZEL) query 'deps(//example/server/gateway:gateway)'

query-targets:
	@$(BAZEL) query //...

# Run gateway client (connects to any running gateway service)
run-client-gateway:
	@$(BAZEL) run //example/client/gateway:gateway -- -addr $(or $(SERVER_ADDR),localhost:8081) -message "$(or $(MESSAGE),ping)"

# Run orchestrator client (connects to any running orchestrator service)
run-client-orchestrator:
	@$(BAZEL) run //example/client/orchestrator:orchestrator -- -addr $(or $(SERVER_ADDR),localhost:8082) -message "$(or $(MESSAGE),ping)"

run-queue-admin: ## Run queue-admin CLI (use ARGS to pass arguments, e.g. make run-queue-admin ARGS="list-topics")
	@$(BAZEL) run //extension/queue/mysql/ctl -- $(ARGS)

test: ## Run unit tests
	@echo "Running unit tests..."
	@$(BAZEL) test //... --test_tag_filters=-manual,-integration || echo "No unit tests found (only integration tests exist)"

test-no-cache: ## Run unit tests without cache (force re-run)
	@echo "Running unit tests (no cache)..."
	@$(BAZEL) test //... --test_tag_filters=-manual,-integration --nocache_test_results

help: ## Show this help message
	@echo "Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2}'

