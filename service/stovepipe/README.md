# Stovepipe Service

Runnable wiring for the **Stovepipe** domain — a single-service domain (the domain *is* the service). The server exposes two RPCs and runs one internal pipeline stage as a queue consumer:

- **`Ping`** — health check.
- **`Ingest`** — resolves a queue's head commit, persists a `Request` (and its head URI) to storage, and publishes the request to the **process** stage.
- **process consumer** (`TopicKeyProcess`) — reloads the persisted `Request` from storage and runs the process stage (`stovepipe/controller/process`).

The ingest → process hop stays inside one service and one store, so only the request **ID** travels on the queue; the consumer reloads from storage (the source of truth), which keeps messages small and redelivery idempotent. The process topic key and its internal wire contract are owned by the domain under `stovepipe/core/messagequeue/`.

Stovepipe therefore needs two MySQL databases: a **storage** database (the `request` and `request_uri` tables) and a **queue** database (messaging infrastructure).

## Wiring notes

`server/main.go` is the composition root and supplies the concrete extension implementations. Two are deliberately demo-only and must be replaced for any real deployment:

- **`inMemoryCounter`** — a process-local `counter.Counter` for sequence numbers; not durable. A real deployment uses a persistent implementation (e.g. `platform/extension/counter/mysql`).
- **`fakeSourceControlFactory`** — seeds each queue with a deterministic single-commit history so ingest resolves a stable head URI (and re-ingesting the same queue exercises the dedup path). A real deployment supplies a VCS-backed `sourcecontrol.Factory`.
- **`newBuildRunnerFactory`** — builds the `buildrunner.Factory` from the environment. Each queue gets its own fake runner, bound to that queue's `Config` and sharing the profile the `BUILD_RUNNER_*` knobs describe (failure rate, build duration, and its spread). A real deployment keeps this shape and swaps the constructed runner for a Buildkite or GitHub Actions one (see [`stovepipe/extension/buildrunner`](../../stovepipe/extension/buildrunner)); a deployment that gives queues *different* backends routes on `Config.QueueName` in `For`, as the SubmitQueue orchestrator does in its `profiles.go`.

## Layout

```
stovepipe/
├── docker-compose.yml      # Stovepipe service + storage MySQL + queue MySQL
├── server/
│   ├── main.go             # gRPC server (Ping, Ingest) + process-stage consumer wiring
│   └── Dockerfile
└── client/
    └── main.go             # Ping client (default :8083)
```

The Stovepipe controllers live under [`stovepipe/controller/`](../../stovepipe/controller) and its extensions under [`stovepipe/extension/`](../../stovepipe/extension); this directory only contains the runnable wiring and a Docker Compose stack for manual testing.

## Configuration

| Variable            | Required | Description                              | Default              |
|---------------------|----------|------------------------------------------|----------------------|
| `STORAGE_MYSQL_DSN` | yes      | Storage database DSN (`request`, `request_uri`) | —             |
| `QUEUE_MYSQL_DSN`   | yes      | Queue database DSN                       | —                    |
| `PORT`              | no       | gRPC listen address                      | `:8083`              |
| `HOSTNAME`          | no       | Subscriber name for the process consumer | `stovepipe-<unix_ts>` |

### Build runner

These knobs configure the fake runner every queue shares. A malformed value fails startup rather than silently falling back to the default.

| Variable                                 | Required | Description                                                  | Default |
|------------------------------------------|----------|--------------------------------------------------------------|---------|
| `BUILD_RUNNER_FAILURE_PERCENT`           | no       | Share of builds (0-100) the runner reports as failed         | `0`     |
| `BUILD_RUNNER_DURATION_MS`               | no       | How long each build reports running before turning terminal  | `0`     |
| `BUILD_RUNNER_DURATION_JITTER_PERCENT`   | no       | Spread (0-100) applied to that duration per build            | `0`     |

The defaults reproduce the original behavior: a fake runner that succeeds immediately unless a request's head URI carries a `buildrunner-fake=<token>` marker. Markers keep working under a configured rate — they pin the outcome for the build that carries them.

Durations are configured as a typical value plus a spread, so the bounds stay easy to state: `BUILD_RUNNER_DURATION_MS=60000` with `BUILD_RUNNER_DURATION_JITTER_PERCENT=25` means every build takes between 45s and 75s, drawn uniformly. A jitter of `0` pins every build to exactly the configured duration; `100` is the widest setting, spanning "terminal immediately" to twice the duration.

Under Compose these are set through `SQ_`-prefixed variables, so a stack whose builds are slow and flaky starts with:

```bash
SQ_BUILD_RUNNER_FAILURE_PERCENT=20 \
SQ_BUILD_RUNNER_DURATION_MS=5000 \
SQ_BUILD_RUNNER_DURATION_JITTER_PERCENT=40 \
make local-stovepipe-start
```

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
```

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

The server handles `SIGINT` / `SIGTERM` gracefully: it drains in-flight RPCs, then stops the process consumer (30s timeout). It exits `0` on clean shutdown, `143` (128 + SIGTERM) when stopped by signal, and `1` on startup/runtime errors (details on stderr). Shutdown errors override the signal exit code.
