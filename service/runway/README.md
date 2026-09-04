# Runway Service

Runnable wiring for the **Runway** domain — a single-service, consumer-only landing service. Runway has no gateway/orchestrator split: the domain *is* the service. It exposes a thin `Ping` RPC for health checks and does its real work as a queue consumer, draining the land pipeline queues that the SubmitQueue orchestrator publishes to.

## What it does

Runway registers two consuming subscriptions against the shared MySQL-backed message queue:

- **land-conflict-check** (`TopicKeyLandConflictCheck`) — handled by `runway/controller/landconflictcheck`.
- **land** (`TopicKeyLand`) — handled by `runway/controller/land`.

Each controller applies the request's ordered steps via a `Lander` and publishes a `LandResult` to the corresponding signal queue (`land-conflict-check-signal` / `land-signal`). A second, DLQ consumer drains the inbound topics' dead-letter queues (`land-conflict-check_dlq`, `runway-land_dlq`) and republishes a `FAILED` result to the matching signal queue, so a dead-lettered request still resolves the client's correlation id.

These topic keys and their wire contracts are owned by the queue's producer side and published under `api/runway/messagequeue/` (the external, cross-domain contract).

### Lander backend

The land work is done by the [`lander`](../../runway/extension/lander) extension, resolved **per queue** — so one Runway can serve several repositories by giving each queue its own land target. By default every queue gets the **noop** lander (always succeeds — for local dev and compose). Point `LAND_CONFIG_PATH` at a land configuration file to wire real **git** land targets, or set `LAND_CHECKOUT_PATH` to configure a single one from the environment (see Configuration).

Two queues naming the same checkout resolve to the *same* lander instance, which is what serializes them against each other: a git lander locks the working tree it owns, and two instances over one tree would reset it out from under each other mid-land. Naming one checkout for two *different* targets is rejected at startup.

### Land configuration file

`LAND_CONFIG_PATH` names a YAML file with a `defaults` block and per-queue overrides. A queue's `lander` block replaces the default wholesale rather than merging field by field.

```yaml
defaults:
  lander: {type: noop}            # noop | git
queues:
  - name: demo-queue
    lander:
      type: git
      remoteUrl: https://github.com/uber/sq-sandbox.git
      target: main
      checkoutPath: /var/runway/checkouts/sq-sandbox
      defaultStrategy: SQUASH_REBASE
      updateHeadBranch: true
      tokenEnv: GITHUB_TOKEN
```

The file holds **no secret**: `tokenEnv` names the environment variable carrying the credential, so the file stays committable and rotating the token needs no edit. Omitting `remoteUrl` means the checkout is provisioned by something else and is used as it stands.

### Checkout provisioning

A git land target's checkout is created at startup if it is not already there: the repository is initialised, the remote configured, the credential written, and the target branch checked out. It is idempotent, so restarting against a persisted volume costs nothing and a rotated token takes effect.

The credential never goes into the remote URL. The lander folds git's stderr into the errors it returns, so a URL-embedded token would be reprinted into logs and dead-letter payloads by any failed fetch. Instead it is written as an HTTP `Authorization` header into a `0600` config fragment inside `.git/`, which the repository config includes — keeping it out of the command line too.

For SSH, leave `tokenEnv` unset and use an `ssh://` remote: the lander already passes `SSH_AUTH_SOCK` and `GIT_SSH_COMMAND` through to git, so an agent or a mounted deploy key works with no further configuration.

Because Runway only consumes queues and serves `Ping`, it needs a **queue** database but no application/storage database.

## Layout

```
runway/
├── server/
│   ├── main.go             # gRPC server (Ping) + land/DLQ consumer wiring
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
| `LAND_CONFIG_PATH` | no | Path to the per-queue land configuration file (see above). Takes precedence over the `LAND_*` variables below, which configure a single target. | — |
| `LAND_CHECKOUT_PATH` | no   | Absolute path to the git checkout the lander owns. When unset (and no config file), the noop lander is used. | — (noop) |
| `LAND_REMOTE`    | no       | Git remote to fetch/push                       | `origin`                 |
| `LAND_TARGET`    | no       | Destination branch on the remote               | `main`                   |
| `LAND_DEFAULT_STRATEGY` | no | Strategy a `DEFAULT` step resolves to: `REBASE`, `SQUASH_REBASE`, `MERGE`, or `PROMOTE` | `REBASE` |
| `LAND_COMMITTER_NAME` | no  | Committer name for service-created commits      | `SubmitQueue Runway`     |
| `LAND_COMMITTER_EMAIL` | no | Committer email for service-created commits     | `runway@submitqueue.invalid` |
| `GIT_EXECUTABLE` / `GIT_EXEC_PATH` / `GIT_TEMPLATE_DIR` | no | Absolute paths pinning the git runtime. Each is derived from the installed git when unset — the executable from `PATH`, the exec path from `git --exec-path`, the templates from the matching install prefix. | derived |
| `LAND_CHECK_STALENESS` | no | Verify each change's provider ref still points at the commit its URI names before applying | `true` |
| `LAND_UPDATE_HEAD_BRANCH` | no | Before pushing the target, move each change's head branch to the commit it landed as, so the provider marks the change merged rather than closed. Only affects `REBASE`/`SQUASH_REBASE`. | `false` |
| `LAND_ALLOW_UNRELATED_HISTORIES` | no | Let a `MERGE` step integrate a change sharing no ancestry with the target (repository imports). Leave off unless the queue exists to perform imports. | `false` |
| `LAND_FETCH_REFSPECS` | no | Comma-separated extra refspecs fetched each cycle. Only needed for a remote that refuses to serve an unadvertised commit by SHA. | — |

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
