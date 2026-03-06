# Contributing to SubmitQueue

Thank you for your interest in contributing to SubmitQueue! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

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
2. Make your changes, following the project conventions.
3. Add or update tests as appropriate.
4. Run `make gazelle` if you added or removed Go files.
5. Run `make test` to ensure unit tests pass.
6. Commit with a clear, descriptive message.
7. Open a pull request against `main`.

## Code Style

See [CLAUDE.md](CLAUDE.md) for code style conventions.

## Testing

See [doc/howto/TESTING.md](doc/howto/TESTING.md) for the full testing guide, including integration and end-to-end tests.

Key commands:

```bash
make test                # Unit tests
make integration-test    # Integration tests (Docker-based, auto-builds binaries)
make e2e-test            # End-to-end tests
```

All tests use table-driven style with `t.Run` subtests. Use `assert`/`require` from testify.

## Pull Request Guidelines

- Keep PRs focused on a single change.
- Include tests for new functionality.
- Ensure all existing tests pass (`bazel test //...`).
- Follow the existing code style and patterns (see [CLAUDE.md](CLAUDE.md) for detailed conventions).
- Fill out the PR template with a description, motivation, and test plan.

## Code Review

All submissions require review before merging. We use GitHub pull requests for this purpose. A maintainer will review your PR and may request changes.

## Reporting Issues

Use GitHub Issues to report bugs or request features. Please check existing issues before creating a new one.

## Shell Configuration (Optional)

See [doc/howto/SHELL.md](doc/howto/SHELL.md) for direnv setup and Make target auto-completion.

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

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## License

By contributing to SubmitQueue, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
