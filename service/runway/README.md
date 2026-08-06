# Runway Service

Runnable wiring for the **Runway** domain — a single-service, consumer-only landing service. Runway has no gateway/orchestrator split: the domain *is* the service. It exposes a thin `Ping` RPC for health checks and does its real work as a queue consumer, draining the merge pipeline queues that the SubmitQueue orchestrator publishes to.

## What it does

Runway registers two consuming subscriptions against the shared MySQL-backed message queue:

- **merge-conflict-check** (`TopicKeyMergeConflictCheck`) — handled by `runway/controller/mergeconflictcheck`.
- **merge** (`TopicKeyMerge`) — handled by `runway/controller/merge`.

Each controller applies the request's ordered steps via a `Merger` and publishes a `MergeResult` to the corresponding signal queue (`merge-conflict-check-signal` / `merge-signal`). A second, DLQ consumer drains the inbound topics' dead-letter queues (`merge-conflict-check_dlq`, `runway-merge_dlq`) and republishes a `FAILED` result to the matching signal queue, so a dead-lettered request still resolves the client's correlation id.

These topic keys and their wire contracts are owned by the queue's producer side and published under `api/runway/messagequeue/` (the external, cross-domain contract).

### Merger backend

The merge work is done by the [`merger`](../../runway/extension/merger) extension. By default the server wires the **noop** merger (always succeeds — for local dev and compose). Set `MERGE_CHECKOUT_PATH` to wire the real **git** merger, built from the `MERGE_*` / `GIT_*` environment (see Configuration); the git merger owns the checkout at that path and pushes to the configured remote/target.

Because Runway only consumes queues and serves `Ping`, it needs a **queue** database but no application/storage database.

## Layout

```
runway/
├── server/
│   ├── main.go             # gRPC server (Ping) + merge/DLQ consumer wiring
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
| `MERGE_CHECKOUT_PATH` | no   | Absolute path to the git checkout the merger owns. When unset, the noop merger is used. | — (noop) |
| `MERGE_REMOTE`    | no       | Git remote to fetch/push                       | `origin`                 |
| `MERGE_TARGET`    | no       | Destination branch on the remote               | `main`                   |
| `MERGE_DEFAULT_STRATEGY` | no | Strategy a `DEFAULT` step resolves to: `REBASE`, `SQUASH_REBASE`, `MERGE`, or `PROMOTE` | `REBASE` |
| `MERGE_COMMITTER_NAME` | no  | Committer name for service-created commits      | `SubmitQueue Runway`     |
| `MERGE_COMMITTER_EMAIL` | no | Committer email for service-created commits     | `runway@submitqueue.invalid` |
| `GIT_EXECUTABLE` / `GIT_EXEC_PATH` / `GIT_TEMPLATE_DIR` | when `MERGE_CHECKOUT_PATH` set | Absolute paths to the pinned git runtime | — |
| `MERGE_CHECK_STALENESS` | no | Verify each change's provider ref still points at the commit its URI names before applying | `true` |
| `MERGE_ALLOW_UNRELATED_HISTORIES` | no | Let a `MERGE` step integrate a change sharing no ancestry with the target (repository imports). Leave off unless the queue exists to perform imports. | `false` |
| `MERGE_FETCH_REFSPECS` | no | Comma-separated extra refspecs fetched each cycle. Only needed for a remote that refuses to serve an unadvertised commit by SHA. | — |

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

The server handles `SIGINT` / `SIGTERM` gracefully: it drains in-flight RPCs, then stops the queue consumers — primary and DLQ, 30s timeout each. It exits `0` on clean shutdown, `143` (128 + SIGTERM) when stopped by signal, and `1` on startup/runtime errors (details on stderr). Shutdown errors override the signal exit code.
</content>
