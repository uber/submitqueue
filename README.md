# SubmitQueue

[![CI](https://github.com/uber/submitqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/uber/submitqueue/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/uber/submitqueue)](go.mod)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

SubmitQueue is a high-performance speculative merge queue that keeps your trunk consistently green at scale. Rather than validating changes one at a time, SubmitQueue speculatively rebases and validates multiple changes in parallel against predicted future states of HEAD. When validations pass, changes land automatically. When they fail, SubmitQueue isolates the offending change and retries the rest — all without human intervention.

Designed for large monorepos and fast-moving teams where concurrent changes can introduce subtle conflicts and destabilize builds.

## Quick Start

Requires Docker and Docker Compose. See [Development Setup](doc/howto/DEVELOPMENT.md) for full prerequisites.

```bash
# Build everything
make build

# Run unit tests
make test

# Start full stack locally (Gateway + Orchestrator + MySQL via Docker Compose)
make local-submitqueue-start

# Test with grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8081 uber.submitqueue.gateway.SubmitQueueGateway/Ping

# Stop services
make local-stop
```

See [example/README.md](example/README.md) for more examples including running individual services and clients.

## Documentation

| Document | Description |
|----------|-------------|
| [Development Setup](doc/howto/DEVELOPMENT.md) | Prerequisites, build, environment, IDE setup |
| [Contributing](CONTRIBUTING.md) | How to contribute, workflow, guidelines |
| [Testing Guide](doc/howto/TESTING.md) | Unit, integration, and E2E testing patterns |
| [Architecture Guide](CLAUDE.md) | Project layout, patterns, conventions |
| [Examples](example/README.md) | Running services, clients, API reference |
| [RFCs](doc/rfc/index.md) | Design documents and proposals |

## Project Status

SubmitQueue is under active development. We welcome contributions and feedback.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

## License

Licensed under the [Apache License 2.0](LICENSE).
