# SubmitQueue Services

Runnable wiring for the **SubmitQueue** domain's two services — the Gateway (entry point for land requests) and the Orchestrator (coordinates the pipeline) — wired with MySQL-backed extensions and runnable via Docker Compose.

## Starting

### Docker Compose (recommended)

```bash
make local-submitqueue-start   # builds binaries and starts all services
make local-submitqueue-ps      # verify services are running
make local-submitqueue-logs    # view logs
```

### Standalone

Each service requires two MySQL databases (app and queue) and is configured via environment variables:

| Variable           | Required | Description                          | Default                              |
|--------------------|----------|--------------------------------------|--------------------------------------|
| `MYSQL_DSN`        | yes      | App database DSN                     | —                                    |
| `QUEUE_MYSQL_DSN`  | yes      | Queue database DSN                   | —                                    |
| `PORT`             | no       | gRPC listen address                  | `:8081` (gateway), `:8082` (orchestrator) |
| `HOSTNAME`         | no       | Subscriber name for queue consumers  | `orchestrator-<unix_ts>` (orchestrator only) |
| `GITHUB_TOKEN`     | no       | GitHub API token for merge checker   | — (orchestrator only)               |
| `GITHUB_GRAPHQL_URL` | no    | GitHub GraphQL endpoint              | `https://api.github.com/graphql` (orchestrator only) |

```bash
export MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/submitqueue?parseTime=true'
export QUEUE_MYSQL_DSN='root:root@tcp(127.0.0.1:3307)/submitqueue?parseTime=true'

# Start gateway (default :8081)
go run ./service/submitqueue/gateway/server

# Start orchestrator (default :8082)
go run ./service/submitqueue/orchestrator/server
```

## Stopping

Both services handle `SIGINT` (Ctrl+C) and `SIGTERM` gracefully:

1. The gRPC server stops accepting new connections and drains in-flight RPCs.
2. The orchestrator additionally stops its queue consumers (30s timeout).
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
