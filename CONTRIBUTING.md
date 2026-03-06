# Contributing to SubmitQueue

Thank you for your interest in contributing to SubmitQueue! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- **Go 1.24 or later** (optional — Bazel manages its own Go toolchain)
- **Docker** and **Docker Compose** (for integration and e2e tests)
- **direnv** (recommended — automatically loads `.envrc` so you can use `bazel` directly)

The project includes `./tool/bazel` (Bazelisk wrapper) and `.bazelversion`, so you don't need to install Bazel separately.

### Clone and Build

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

### Optional Tools

```bash
# macOS
brew install protobuf grpcurl direnv

# Go protoc plugins (only if modifying .proto files)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go@latest
```

## Development Workflow

1. Fork the repository and create a feature branch from `main`.
2. Make your changes, following the code style and conventions below.
3. Add or update tests as appropriate.
4. Run `make gazelle` if you added or removed Go files.
5. Run `make test` to ensure unit tests pass.
6. Commit with a clear, descriptive message.
7. Open a pull request against `main`.

## Code Style

The project follows specific conventions for code style, error handling, logging, and entity design. See [CLAUDE.md](CLAUDE.md) for the full set of conventions, including:

- Structured logging with `zap.SugaredLogger`
- Interfaces for behavior, structs for data
- Value types over pointers
- Error classification with `core/errs`
- Immutable entities and optimistic locking

## Testing

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for the full testing guide, including integration and end-to-end tests.

Key commands:

```bash
make test                # Unit tests
make integration-test    # Integration tests (Docker-based, auto-builds binaries)
make e2e-test            # End-to-end tests
```

All tests use table-driven style with `t.Run` subtests. Use `assert`/`require` from testify.

## Proto Changes

When modifying `.proto` files:

1. Edit the proto file in `{service}/proto/`.
2. Run `make proto` to regenerate `*.pb.go`, `*_grpc.pb.go`, and `*.pb.yarpc.go`.
3. Update controllers and clients as needed.
4. Commit all generated files.

See [CLAUDE.md](CLAUDE.md) for detailed workflows (adding RPC methods, queue controllers, extensions, entities).

## Adding Extensions

Extensions follow a vendor-agnostic interface pattern:

1. Define the interface at `extension/{ext}/`.
2. Add implementations at `extension/{ext}/{impl}/`.
3. Include a factory interface for dependency injection.
4. Add `BUILD.bazel`, tests, and README.

See [CLAUDE.md](CLAUDE.md) for the full extension guide and mock setup instructions.

## Reporting Issues

Use the [issue templates](.github/ISSUE_TEMPLATE/) when filing bugs or requesting features. Blank issues are disabled — please choose the appropriate template.

## Code Review

- All submissions require review before merging.
- Maintainers may request changes or suggest improvements.
- Keep PRs focused — one logical change per PR.

## Shell Configuration (Optional)

### Using direnv (Recommended)

```bash
brew install direnv

# Add to ~/.zshrc or ~/.bashrc
eval "$(direnv hook zsh)"  # or bash, fish, etc.

# In the project directory
direnv allow
```

### Make Target Auto-Completion (zsh)

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

## Troubleshooting

**Proto generation fails:**
- Ensure all three protoc plugins are installed (see Optional Tools above)
- Check that `protoc` is in your PATH: `which protoc`

**Build fails after proto changes:**
- Run `make proto` to regenerate proto files
- Ensure you updated all service implementations for new/changed fields

**Server won't start:**
- Check if port is already in use: `lsof -i :8081`

**Bazel build issues:**
- Version is pinned in `.bazelversion`; use `./tool/bazel` or `bazel` with direnv
- Try `bazel shutdown` and rebuild
