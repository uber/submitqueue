# Runway Service

Runnable wiring for the **Runway** domain — a single-service, consumer-only landing service. Runway has no gateway/orchestrator split: the domain *is* the service. It exposes a thin `Ping` RPC for health checks and does its real work as a queue consumer, draining the merge pipeline queues that the SubmitQueue orchestrator publishes to.

## What it does

Runway registers two consuming subscriptions against the shared MySQL-backed message queue:

- **merge-conflict-check** (`TopicKeyMergeConflictCheck`) — handled by `runway/controller/mergeconflictcheck`.
- **merge** (`TopicKeyMerge`) — handled by `runway/controller/merge`.

These topic keys and their wire contracts are owned by the queue's producer side and published under `api/runway/messagequeue/` (the external, cross-domain contract). The corresponding signal queues where Runway will publish results are not wired yet.

Because Runway only consumes queues and serves `Ping`, it needs a **queue** database but no application/storage database.

## Layout

```
runway/
├── server/
│   ├── main.go             # gRPC server (Ping) + primary consumer wiring
│   ├── Dockerfile
│   └── docker-compose.yml  # Runway service + queue MySQL
└── client/
    └── main.go             # Ping client (default :8086)
```

The Runway controllers themselves live under [`runway/controller/`](../../runway/controller); this directory only contains the runnable wiring and a Docker Compose stack for manual testing.

## Configuration

| Variable          | Required | Description                                   | Default                  |
|-------------------|----------|-----------------------------------------------|--------------------------|
| `QUEUE_MYSQL_DSN` | yes      | Queue database DSN                            | —                        |
| `PORT`            | no       | gRPC listen address                           | `:8086`                  |
| `HOSTNAME`        | no       | Subscriber name for the queue consumer        | `runway-<unix_ts>`       |

## Running

### Docker Compose (recommended)

```bash
make local-runway-start   # builds the Linux binary, starts runway + queue MySQL, applies the queue schema
make local-runway-stop    # tears the stack down
```

`local-runway-start` prints the ephemeral host ports for the gRPC server and the queue MySQL. Only the queue schema is applied — there is no Runway app schema.

### Bazel / Go

```bash
bazel build //service/runway/server:runway
bazel build //service/runway/client:runway

go run ./service/runway/server
```

## Testing the Ping RPC

```bash
go run ./service/runway/client -addr localhost:8086 -message "hello"
# or
make run-client-runway SERVER_ADDR=localhost:8086 MESSAGE=hello

# grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8086 uber.runway.Runway/Ping
```

## Shutdown

The server handles `SIGINT` / `SIGTERM` gracefully: it drains in-flight RPCs, then stops the queue consumer (30s timeout). It exits `0` on clean shutdown, `143` (128 + SIGTERM) when stopped by signal, and `1` on startup/runtime errors (details on stderr). Shutdown errors override the signal exit code.
</content>
