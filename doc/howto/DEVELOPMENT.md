# Development

## Prerequisites

- **Go 1.25+** — needed for `gopls`, `go mod`, and installing protoc plugins. Download from [go.dev/dl](https://go.dev/dl/). Note: Bazel manages its own Go toolchain for builds, but a local Go installation is required for editor tooling and dependency management.
- **Docker** — for integration and e2e tests, and for running services locally. Bazel supplies Docker Compose.
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

After building, start the full stack and land a change through it:

```bash
# 1. Confirm Docker is running
docker ps

# 2. Start the full stack
make local-submitqueue-start

# 3. Create changes, enqueue them, and watch them land
make demo-requests

# 4. Stop services
make local-submitqueue-stop
```

[QUICKSTART.md](QUICKSTART.md) walks through the same run in detail, and on to `PROVIDER=git`, which lands real commits into a repository on disk — still with no credential.

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

`make help` lists every target. The table below is the day-to-day set.

CI runs `make lint`, `make check-tidy`, and `make check-gazelle`. `make lint` includes the format check, license headers, the tracked-binary check, the message-ID check, and the queue-shard check. `make fmt`, `make tidy`, and `make gazelle` apply the corresponding fixes. `make mocks` regenerates the checked-in mockgen files; `make check-mocks` fails when that output differs from the tree. `make clean` removes the Bazel cache and `bin/`. Generated protobuf Go files stay in the source tree until `make clean-proto`; `make proto` writes them again.

| Target | Description |
|--------|-------------|
| `make build` | Build all services |
| `make test` | Run unit tests |
| `make integration-test` | Run all integration tests (Docker-based) |
| `make e2e-test` | Run end-to-end tests |
| `make fmt` | Format Go and YAML |
| `make lint` | Run the linters CI runs |
| `make tidy` | Tidy `go.mod` and `MODULE.bazel` |
| `make check-tidy` | Fail if `go.mod` or `MODULE.bazel` is untidy |
| `make check-gazelle` | Fail if `BUILD.bazel` files are stale |
| `make gazelle` | Update `BUILD.bazel` files |
| `make mocks` | Regenerate mockgen files |
| `make check-mocks` | Fail if generated mocks are stale |
| `make web-build` | Build the web packages and deployable OCI image with Bazel |
| `make web-check` | Run web generation drift, package, lint, type, unit, and production image checks with Bazel |
| `make web-e2e-test` | Run the Bazel-managed real-stack Playwright and axe test |
| `make web-proto` | Regenerate the committed TypeScript protobuf API with Bazel |
| `make proto` | Regenerate protobuf files |
| `make clean` | Remove the Bazel cache and `bin/` |
| `make clean-proto` | Remove generated protobuf Go files |
| `make local-submitqueue-start` | Start full workflow stack (Gateway + Orchestrator + Runway + two MySQL databases) |
| `make local-submitqueue-ps` | Show running SubmitQueue containers and ports |
| `make local-submitqueue-logs` | View logs from all SubmitQueue services |
| `make local-submitqueue-stop` | Stop the SubmitQueue stack |
| `make local-stop` | Stop SubmitQueue, Stovepipe, and Runway |
| `make help` | List every target |

The web build does not require a host Node.js or pnpm installation: Bazel supplies Node, npm packages, Buf, TypeScript, Next.js, Playwright, Chromium, and the container image toolchain. `make web-install` is an optional convenience for editors and direct pnpm development only.

## Running Specific Tests

```bash
# Run tests for a single package
bazel test //submitqueue/gateway/controller:go_default_test

# Run a single test function
bazel test //submitqueue/gateway/controller:go_default_test --test_filter=TestLand

# Run Gateway integration tests only
make integration-test-submitqueue-gateway

# Run Orchestrator integration tests only
make integration-test-submitqueue-orchestrator

# Run SubmitQueue and shared extension integration tests
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
- Check that your Go version matches what's in `go.mod` (currently Go 1.25)
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
