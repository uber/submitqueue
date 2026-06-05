# Development

## Prerequisites

- **Go 1.24+** — needed for `gopls`, `go mod`, and installing protoc plugins. Download from [go.dev/dl](https://go.dev/dl/). Note: Bazel manages its own Go toolchain for builds, but a local Go installation is required for editor tooling and dependency management.
- **Docker** and **Docker Compose** — for integration and e2e tests, and for running services locally.
- **direnv** (recommended) — automatically loads `.envrc` so you can use `bazel` directly instead of `./tool/bazel`.

The project includes `./tool/bazel` (Bazelisk wrapper) and `.bazelversion`, so you don't need to install Bazel separately. Bazel manages its own Go toolchain for building and testing.

### Setting up direnv

```bash
brew install direnv
```

Add the hook for your shell:

```bash
# zsh — add to ~/.zshrc
eval "$(direnv hook zsh)"

# bash — add to ~/.bashrc
eval "$(direnv hook bash)"

# fish — add to ~/.config/fish/config.fish
direnv hook fish | source
```

Then allow it in the project directory:

```bash
direnv allow
```

## Clone and Build

```bash
git clone https://github.com/uber/submitqueue.git
cd submitqueue

# Optional: allow direnv
direnv allow

# Build everything
make build

# Run unit tests
make test
```

## Try It Locally

After building, start the full stack to confirm everything works end to end:

```bash
# 1. Confirm Docker is running
docker ps

# 2. Start the full stack
make local-submitqueue-start

# 3. Check services are up (Gateway on :8081, Orchestrator on :8082)
make local-submitqueue-ps

# 4. Test Gateway with grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.submitqueue.gateway.SubmitQueueGateway/Ping

# 5. Stop services
make local-stop
```

If any step fails, see [Troubleshooting](#troubleshooting) below.

## IDE Setup

### VS Code

Install the [Go extension](https://marketplace.visualstudio.com/items?itemName=golang.Go), which uses `gopls` for code intelligence. It works with the project's `go.mod` out of the box.

### GoLand / IntelliJ

GoLand works with Go modules automatically. Open the project root and GoLand will detect `go.mod`.

## Optional Tools

```bash
# macOS
brew install grpcurl
```

## Common Make Targets

| Target | Description |
|--------|-------------|
| `make build` | Build all services |
| `make test` | Run unit tests |
| `make integration-test` | Run all integration tests (Docker-based) |
| `make e2e-test` | Run end-to-end tests |
| `make proto` | Regenerate protobuf files |
| `make gazelle` | Update BUILD.bazel files |
| `make local-submitqueue-start` | Start full stack (Gateway + Orchestrator + MySQL) |
| `make local-submitqueue-ps` | Show running containers and ports |
| `make local-submitqueue-logs` | View logs from all services |
| `make local-stop` | Stop all services |
| `make clean` | Clean generated files and binaries |
| `make help` | Show all available targets with descriptions |

## Running Specific Tests

```bash
# Run tests for a single package
bazel test //gateway/controller:controller_test

# Run a single test function
bazel test //gateway/controller:controller_test --test_filter=TestLand

# Run Gateway integration tests only
make integration-test-submitqueue-gateway

# Run Orchestrator integration tests only
make integration-test-submitqueue-orchestrator

# Run extension integration tests only
make integration-test-extensions

# Run unit tests without cache
make test-no-cache
```

See [TESTING.md](TESTING.md) for the full testing guide, including integration and end-to-end test patterns.

## Troubleshooting

**Proto generation fails:**
- Run `make proto`; Bazel provides the pinned `protoc` and plugin toolchain.
- If Bazel cannot fetch tools, check network access and the repository cache configuration in `.bazelrc`.

**Build fails after proto changes:**
- Run `make proto` to regenerate proto files
- Ensure you updated all service implementations for new/changed fields

**Server won't start:**
- Check if port is already in use: `lsof -i :8081`

**Bazel build issues:**
- Version is pinned in `.bazelversion`; use `./tool/bazel` or `bazel` with direnv
- Try `bazel shutdown` and rebuild

**`gopls` or `go mod tidy` errors:**
- Run `go mod download` to fetch all dependencies
- Check that your Go version matches what's in `go.mod` (currently Go 1.24)
- If using VS Code, restart the Go language server: `Ctrl+Shift+P` > "Go: Restart Language Server"

## Shell Auto-Completion

### zsh

Add to `~/.zshrc` for tab-completion of Makefile targets with descriptions:

```bash
autoload -Uz compinit
compinit

function _make_targets() {
  local -a targets
  local makefile_cache=".make_targets_cache"

  if [[ -f Makefile ]]; then
    if [[ ! -f $makefile_cache ]] || [[ Makefile -nt $makefile_cache ]]; then
      awk -F':.*?## ' '/^[a-zA-Z0-9_-]+:.*?## / {printf "%s:%s\n", $1, $2}' Makefile > $makefile_cache
    fi
    targets=(${(f)"$(<$makefile_cache)"})
    if [[ -s $makefile_cache ]] && grep -q ':' $makefile_cache 2>/dev/null; then
      _describe 'make targets' targets
    else
      awk -F: '/^[a-zA-Z0-9_-]+:/ {print $1}' Makefile > $makefile_cache
      targets=(${(f)"$(<$makefile_cache)"})
      _describe 'make targets' targets
    fi
  fi
}

compdef _make_targets make
```

The completion cache (`.make_targets_cache`) is gitignored and automatically regenerates when the Makefile changes.

### bash

If you use `bash-completion`, Makefile target completion typically works out of the box. Otherwise, add to `~/.bashrc`:

```bash
complete -W "\$(grep -oE '^[a-zA-Z0-9_-]+:' Makefile | sed 's/://')" make
```

### Universal alternative

Run `make help` to see all available targets and their descriptions at any time.
