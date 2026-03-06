# SubmitQueue

A distributed system for managing code submission workflows. SubmitQueue coordinates the lifecycle of code changes — from submission through validation to landing — using a clean, extensible architecture with pluggable backends.

## Quick Start

```bash
# Build everything
make build

# Run unit tests
make test

# Start full stack locally (Gateway + Orchestrator + MySQL via Docker Compose)
make local-start

# Test with grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.submitqueue.gateway.SubmitQueueGateway/Ping

# Stop services
make local-stop
```

See [example/README.md](example/README.md) for more examples including running individual services and clients.

## Architecture

See [CLAUDE.md](CLAUDE.md) for the full architecture guide, including project layout, controller patterns, entity conventions, extension system, and development workflows.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started, development workflow, code style, and testing.
