# SubmitQueue

[![CI](https://github.com/uber/submitqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/uber/submitqueue/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/uber/submitqueue)](go.mod)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Slack](https://img.shields.io/badge/Slack-join%20the%20community-4A154B?logo=slack&logoColor=white)](https://join.slack.com/t/submitqueue/shared_invite/zt-46gkqj682-7zcQphxm2pYqkjDo9lbmYA)

SubmitQueue is a high-performance speculative merge queue that keeps your trunk consistently green at scale. Rather than validating changes one at a time, SubmitQueue speculatively rebases and validates multiple changes in parallel against predicted future states of HEAD. When validations pass, changes land automatically. When they fail, SubmitQueue isolates the offending change and retries the rest — all without human intervention.

Designed for large monorepos and fast-moving teams where concurrent changes can introduce subtle conflicts and destabilize builds.

## Repository layout

Cross-domain Go code (errors, metrics, consumer framework, HTTP helpers, shared entities, shared extension contracts) lives under [`platform/`](platform/README.md). Each product domain has its own tree (`submitqueue/`, `stovepipe/`, …) and grows into `gateway/`, `orchestrator/`, `entity/`, `extension/`, and domain-local `core/` — though a domain may start smaller (Stovepipe is currently a single Ping-only service with just `controller/`). See [CLAUDE.md](CLAUDE.md) for conventions and import paths.

## Quick Start

Land a change and watch it reach `landed`. Requires Docker and Docker Compose, and nothing else — no repository, no account, no token. See [Development Setup](doc/howto/DEVELOPMENT.md) for full prerequisites.

```bash
# Start the full stack (Gateway + Orchestrator + Runway + MySQL)
make local-submitqueue-start

# Compose publishes a random host port; the line above prints it, as does this
make local-submitqueue-ps
export GATEWAY_ADDR=localhost:<gateway port>

# Submit a change, and follow the receipt it returns
make land QUEUE=test-queue \
  URI='git://git.example.com/demo/refs%2Fheads%2Ffeature-a/1111111111111111111111111111111111111111'
make land-status QUEUE=test-queue SQID=test-queue/1

# Stop services
make local-stop
```

Every integration at the edges is faked — the change provider, CI, and the merge itself — so the run is free and finishes in seconds. The queue's own logic is real: validation, batching, conflict analysis, and speculation all run, and the request log records the full trail from `accepted` to `landed`. Nothing is pushed to any repository.

[Quickstart](doc/howto/QUICKSTART.md) explains the change URI, how to make a change fail on demand, and what this does and does not prove. From there, `make e2e-git-test` adds a real git merge (still no credentials), and [PROVIDER-E2E.md](doc/howto/PROVIDER-E2E.md) adds a live provider. See [service/README.md](service/README.md) for running individual services and clients.

## Documentation

| Document | Description |
|----------|-------------|
| [Quickstart](doc/howto/QUICKSTART.md) | Land a change locally with no credentials |
| [Development Setup](doc/howto/DEVELOPMENT.md) | Prerequisites, build, environment, IDE setup |
| [Contributing](CONTRIBUTING.md) | How to contribute, workflow, guidelines |
| [Testing Guide](doc/howto/TESTING.md) | Unit, integration, and E2E testing patterns |
| [Landing real changes](doc/howto/PROVIDER-E2E.md) | Running the pipeline against a live provider |
| [Architecture Guide](CLAUDE.md) | Project layout, patterns, conventions |
| [Examples](service/README.md) | Running services, clients, API reference |
| [RFCs](doc/rfc/index.md) | Design documents and proposals |

## Project Status

SubmitQueue is under active development. We welcome contributions and feedback.

## Community

Join us on Slack: [submitqueue.slack.com](https://join.slack.com/t/submitqueue/shared_invite/zt-46gkqj682-7zcQphxm2pYqkjDo9lbmYA) — questions, design discussions, and help getting started.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

## License

Licensed under the [Apache License 2.0](LICENSE).
