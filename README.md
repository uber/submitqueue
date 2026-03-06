# SubmitQueue

SubmitQueue is a change management system that guarantees an always-green master branch at scale. It addresses a critical challenge in large monorepo environments: as commit volume grows, concurrent changes can introduce conflicts and broken builds that are difficult to detect through traditional CI alone.

SubmitQueue solves this by serializing and validating changes before they land on master, ensuring that every commit point passes all build steps — compilation, unit tests, and integration tests. The system is designed for high throughput and low turnaround time, handling thousands of daily commits without becoming a bottleneck.

For more details, see the EuroSys '19 paper: [Keeping Master Green at Scale](https://dl.acm.org/doi/10.1145/3302424.3303970).

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
