# SubmitQueue Services

Runnable wiring for the **SubmitQueue** domain's two services — the Gateway (entry point for land requests) and the Orchestrator (coordinates the pipeline) — wired with MySQL-backed extensions. The full Docker Compose workflow also starts Runway, which performs merge-conflict checks and merges.

## Starting

### Docker Compose (recommended)

```bash
make local-submitqueue-start   # builds binaries and starts Gateway, Orchestrator, Runway, and both databases
make local-submitqueue-ps      # verify services are running
make local-submitqueue-logs    # view logs
```

### Standalone

Each service requires the application and queue MySQL databases. Service-specific configuration is supplied through environment variables:

| Variable | Service | Required | Description | Default |
|----------|---------|----------|-------------|---------|
| `MYSQL_DSN` | both | yes | Application database DSN | — |
| `QUEUE_MYSQL_DSN` | both | yes | Queue database DSN | — |
| `QUEUE_CONFIG_PATH` | Gateway | yes | YAML registry of accepted queue names | — |
| `PROFILES_CONFIG_PATH` | Orchestrator | no | YAML extension profiles and per-queue routing | built-in example profiles |
| `HOSTNAME` | both | no | Base subscriber name; Gateway appends its PID | time-based service name |
| `QUEUE_LOG_LEVEL` | both | no | Message-queue logger level | `info` |
| `CONSUMER_GATE_DIR` | both | no | Directory holding per-consumer gate files | gating disabled when unset |
| `GITHUB_TOKEN` | Orchestrator | profile-dependent | Default credential for GitHub integrations selected by a profile | — |
| `GITHUB_BASE_URL` | Orchestrator | no | GitHub API base URL | `https://api.github.com` |
| `PORT` | both | no | gRPC listen address | `:8081` (Gateway), `:8082` (Orchestrator) |

```bash
export MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/submitqueue?parseTime=true'
export QUEUE_MYSQL_DSN='root:root@tcp(127.0.0.1:3307)/submitqueue?parseTime=true'

# Start gateway (default :8081)
export QUEUE_CONFIG_PATH=service/submitqueue/gateway/server/queues.yaml
go run ./service/submitqueue/gateway/server

# Start orchestrator with its built-in example profiles (default :8082)
go run ./service/submitqueue/orchestrator/server
```

## Stopping

Both services handle `SIGINT` (Ctrl+C) and `SIGTERM` gracefully:

1. The gRPC server stops accepting new connections and drains in-flight RPCs.
2. The Gateway stops its request-log consumer, and the Orchestrator stops its complete queue pipeline (30-second limit for each).
3. The process exits with a code reflecting the outcome (see below).

To stop Docker Compose services:

```bash
make local-stop
```

## Exit Codes

| Code | Meaning                                                                 |
|------|-------------------------------------------------------------------------|
| 0    | Clean shutdown, no errors.                                              |
| 1    | Startup failure or runtime error (details on stderr).                   |
| 143  | Stopped by signal (SIGINT or SIGTERM). This is 128 + SIGTERM per POSIX. |

When shutdown itself encounters errors (e.g. the gRPC server returns an error during graceful stop, or queue consumers time out), those override the signal exit code and the process exits with code 1. The actual errors are printed to stderr.
