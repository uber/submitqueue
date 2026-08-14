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

Put traffic through the queue and watch it land. Requires Docker and Docker Compose, and nothing else — no repository, no account, no token. See [Development Setup](doc/howto/DEVELOPMENT.md) for full prerequisites.

```bash
# Start the full stack (Gateway + Orchestrator + Runway + MySQL)
make local-submitqueue-start

# Compose publishes a random host port; the line above prints it, as does this
make local-submitqueue-ps
export GATEWAY_ADDR=localhost:<gateway port>

# Create changes, enqueue each as it is created, and watch them settle
make demo-requests

# Stop services
make local-submitqueue-stop
```

`PROVIDER` decides where changes come from and what landing them does, and it is the only thing that changes between them:

| `PROVIDER` | A change is | Landing it | Needs |
|---|---|---|---|
| **`fake`** (default) | a URI, and nothing else | reports success without touching a repository | nothing |
| **`git`** | a branch in a bare repository on disk | a real fetch, cherry-pick and push | nothing |
| **`github`** | a real pull request | a real push to a real repository | a repository and a token |

The queue's own logic is real in all three: validation, batching, conflict analysis, speculation, and a request log recording the full trail from `accepted` to `landed`. `PROVIDER=git make local-submitqueue-start` is the first rung where a commit actually reaches a branch, and it still needs no credential.

[Quickstart](doc/howto/QUICKSTART.md) walks all three rungs — proving a change landed with `git log`, making one fail on demand, and what a live provider needs. See [service/README.md](service/README.md) for running individual services and clients.

## Documentation

| Document | Description |
|----------|-------------|
| [Quickstart](doc/howto/QUICKSTART.md) | Run the stack and land changes — fake, local git, or GitHub |
| [Development Setup](doc/howto/DEVELOPMENT.md) | Prerequisites, build, environment, IDE setup |
| [Contributing](CONTRIBUTING.md) | How to contribute, workflow, guidelines |
| [Testing Guide](doc/howto/TESTING.md) | Unit, integration, and E2E testing patterns |
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
