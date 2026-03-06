# SubmitQueue

SubmitQueue is a speculative merge queue that keeps the main branch green at scale. In large monorepo environments, concurrent changes can introduce conflicts and broken builds. SubmitQueue solves this by serializing and validating changes before they land on main, ensuring that every commit point passes defined validations.

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

## Developer Guide

See [CLAUDE.md](CLAUDE.md) for the full developer guide, including project layout, controller patterns, entity conventions, extension system, and development workflows.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started, development workflow, code style, and testing.
