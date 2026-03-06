# Development

## Prerequisites

- **Docker** and **Docker Compose** (for integration and e2e tests)
- **direnv** (recommended — automatically loads `.envrc` so you can use `bazel` directly)

The project includes `./tool/bazel` (Bazelisk wrapper) and `.bazelversion`, so you don't need to install Bazel or Go separately.

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

## Optional Tools

```bash
# macOS
brew install protobuf grpcurl direnv

# Go protoc plugins (only if modifying .proto files)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install go.uber.org/yarpc/encoding/protobuf/protoc-gen-yarpc-go@latest
```

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
