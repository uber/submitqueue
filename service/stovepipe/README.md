# Stovepipe Service

Runnable wiring for the **Stovepipe** domain — a single-service domain (the domain *is* the service). The server exposes four RPCs and runs the internal pipeline stages as queue consumers:

- **`Ping`** — health check.
- **`Ingest`** — resolves a queue's head commit, persists a `Request` (and its head URI) to storage, and publishes the request to the **process** stage.
- **`GetRequestHistoryByID`** — returns the retained request log for one request ID.
- **`GetRequestHistoryByURI`** — returns retained histories selected by an exact commit URI.
- **process consumer** (`TopicKeyProcess`) — reloads the persisted `Request` from storage and runs the process stage (`stovepipe/controller/process`).
- **build consumer** (`TopicKeyBuild`) — reloads the persisted `Request` and triggers the build-runner, then publishes to `buildsignal`.
- **buildsignal consumer** (`TopicKeyBuildSignal`) — polls/records the build's terminal status and releases the queue's in-flight slot, then publishes to `record`.
- **record consumer** (`TopicKeyRecord`) — writes the whole-repository validation fact and, for a green fact, advances the queue's last-green bookmark and promotion ref.
- **hook consumer** (`TopicKeyHook`) — receives `validation.repository.started` from process and `validation.repository.recorded` or `validation.repository.cancelled` from record. The current resolver invokes only `noop`, so these events have no external side effect. Its topic name is domain-qualified (`stovepipe-hook`) because the key is shared across domains.
- **DLQ consumers** — registered for process, build, buildsignal, record, and hook, with stage-specific reconciliation behavior.

The ingest → process → build → buildsignal → record flow stays inside one service and one store, so messages carry thin identifiers: request IDs on process, build, and record; a build ID on buildsignal. Consumers reload the full entity from storage, which keeps messages small and redelivery idempotent. The topic keys and internal wire contract are owned by the domain under `stovepipe/core/messagequeue/`.

Stovepipe therefore needs two MySQL databases: a **storage** database (the `queue`, `request`, `request_uri`, `request_log`, `build`, and `validation_fact` tables) and a **queue** database (messaging infrastructure).

## Wiring notes

`server/main.go` is the composition root and supplies the concrete extension implementations. Two are deliberately demo-only and must be replaced for any real deployment:

- **`inMemoryCounter`** — a process-local `counter.Counter` for sequence numbers; not durable. A real deployment uses a persistent implementation (e.g. `platform/extension/counter/mysql`).
- **`fakeSourceControlFactory`** — seeds each queue with a deterministic single-commit history so ingest resolves a stable head URI (and re-ingesting the same queue exercises the dedup path). A real deployment supplies a VCS-backed `sourcecontrol.Factory`, which is also where a queue's promotion ref is resolved. The fake has no ref to move, so a promotion locally shows up only in the record consumer's logs.

## Layout

```
stovepipe/
├── docker-compose.yml      # Stovepipe service + storage MySQL + queue MySQL
├── docker-compose.debug.yml # Debug variant with delve
├── server/
│   ├── main.go             # gRPC server (Ping, Ingest) + pipeline consumer wiring
│   └── Dockerfile
└── client/
    └── main.go             # Ping client (default :8083)
```

The Stovepipe controllers live under [`stovepipe/controller/`](../../stovepipe/controller): `ingest.go` contains the RPC controller, while `process/`, `build/`, `buildsignal/`, `record/`, and `dlq/` contain queue controllers. Its extensions live under [`stovepipe/extension/`](../../stovepipe/extension); this directory only contains runnable wiring and a Docker Compose stack for manual testing.

## Configuration

| Variable            | Required | Description                              | Default              |
|---------------------|----------|------------------------------------------|----------------------|
| `STORAGE_MYSQL_DSN` | yes      | Storage database DSN | — |
| `QUEUE_MYSQL_DSN`   | yes      | Queue database DSN                       | —                    |
| `QUEUE_LOG_LEVEL`   | no       | Message-queue logger level               | `info`               |
| `PORT`              | no       | gRPC listen address                      | `:8083`              |
| `HOSTNAME`          | no       | Subscriber name for the queue consumers  | `stovepipe-<unix_ts>` |

## Running

### Docker Compose (recommended)

```bash
make local-stovepipe-start   # builds the Linux binary, starts the service + both MySQL DBs, applies storage + queue schemas
make local-stovepipe-stop    # tears the stack down
make local-stovepipe-logs    # follow logs
```

The compose service key is **`stovepipe-service`**, so under the default project **`stovepipe`** the container is **`stovepipe-stovepipe-service-1`**. Inside the container the server listens on `:8080`, published on a random ephemeral host port.

### Breakpoint debugging (dlv debugger)

```bash
make local-stovepipe-debug-start
```

Attach with `.vscode/launch.json` (**Debug: attach (dlv in docker)**), then send a request using the gRPC port from the make output.

```bash
# Ingest example
grpcurl -plaintext -d '{"queue":"monorepo/main"}' localhost:PORT uber.submitqueue.stovepipe.Stovepipe/Ingest

# Retained history by request ID
grpcurl -plaintext -d '{"queue":"monorepo/main","request_id":"request/monorepo/main/1"}' localhost:PORT uber.submitqueue.stovepipe.Stovepipe/GetRequestHistoryByID

# Retained history by exact commit URI
grpcurl -plaintext -d '{"queue":"monorepo/main","uri":"git://monorepo/main/HEAD"}' localhost:PORT uber.submitqueue.stovepipe.Stovepipe/GetRequestHistoryByURI
```

History lookup is defined by retained `request_log` rows. A request with no retained rows is not discoverable through these RPCs, even if operational request data still exists.

### Bazel / Go

```bash
bazel build //service/stovepipe/server:stovepipe
bazel build //service/stovepipe/client:stovepipe

go run ./service/stovepipe/server
```

## Testing the Ping RPC

```bash
go run ./service/stovepipe/client -addr localhost:8083 -message "hello"
# or
make run-client-stovepipe SERVER_ADDR=localhost:8083 MESSAGE=hello

# grpcurl
grpcurl -plaintext -d '{"message": "hello"}' localhost:8083 uber.submitqueue.stovepipe.Stovepipe/Ping
```

## Shutdown

The server handles `SIGINT` / `SIGTERM` gracefully: it drains in-flight RPCs, then stops the primary pipeline consumer followed by the DLQ consumer (30-second limit for each). It exits `0` on clean shutdown, `143` (128 + SIGTERM) when stopped by signal, and `1` on startup/runtime errors (details on stderr). Shutdown errors override the signal exit code.
