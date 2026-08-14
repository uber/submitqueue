# Landing a change with no credentials

The shortest path from a clone to watching the queue actually do its job: start the stack, submit a change, watch it reach `landed`. It needs Docker and nothing else — no repository, no account, no token — because every integration at the edges is faked.

That is also its limit, and worth being clear about before you read anything into a green result. Faking the edges is what makes the run free; it also means `landed` here proves the queue's own logic ran, not that a commit reached a branch anywhere.

| | Command | What is real | What you need |
|---|---|---|---|
| **Tier 1, here** | `make local-submitqueue-start` | the pipeline: validation, batching, conflict analysis, speculation, the request log | Docker |
| Tier 2 | `make e2e-git-test` | the above, plus a real git merge into a bare repository | Docker |
| Tier 3 | [PROVIDER-E2E.md](PROVIDER-E2E.md) | the above, plus a provider: change metadata, its CI, changes marked merged | a repository and a token |

The tiers are the ones [PROVIDER-E2E.md](PROVIDER-E2E.md) names. This guide is the manual form of tier 1; `make e2e-test` is the same ground covered automatically, and is what CI runs.

## Start the stack

```bash
make local-submitqueue-start
```

This builds the Linux binaries, brings up Gateway, Orchestrator, Runway and two MySQL databases, and applies their schemas. The first run spends most of its time in the Bazel build; later ones start in seconds.

Compose publishes each service on a **random** host port so several stacks can run side by side, which means there is no fixed address to hard-code. The start-up output ends with the ports; `make local-submitqueue-ps` prints them again at any time:

```
📡 Service Endpoints:
  Gateway gRPC:      localhost:55343
```

Export it, because every command below needs it:

```bash
export GATEWAY_ADDR=localhost:55343
```

Leaving it unset does not fall back to anything useful — the client's default is `localhost:8081`, which is the `go run` port, not the compose one.

## Land a change

```bash
make land QUEUE=test-queue \
  URI='git://git.example.com/demo/refs%2Fheads%2Ffeature-a/1111111111111111111111111111111111111111'
```

```
Change 1: git://git.example.com/demo/refs%2Fheads%2Ffeature-a/1111111111111111111111111111111111111111
Landed request submitted.
  sqid: test-queue/1
```

The `sqid` is the receipt. Numbering is per queue and starts from a counter that lives in the application database, which the stack does not carry across a restart (see Clean up), so on a freshly started stack the first request really is `test-queue/1` and the commands below can be copied as they are.

### Why the URI looks like that

A change URI is `git://{remote}/{repo}/{ref}/{commit_sha}`, and the fake change provider echoes back whatever it is handed — so the remote, repository and branch above need not exist. Two parts are still checked before anything is echoed, and both reject a request outright:

- the **commit SHA must be 40 lowercase hex characters**, so a placeholder like `abc123` fails;
- the **ref must be fully qualified and percent-encoded** — `refs%2Fheads%2Ffeature-a`, not `feature-a`. Encoding is what keeps a branch name containing slashes inside a single path segment.

Quote the URI in your shell. It contains a `%`, and in the failure case below a `?` as well.

## Follow it

```bash
make land-status QUEUE=test-queue SQID=test-queue/1
```

```
sqid:   test-queue/1
queue:  test-queue
status: landed
```

A request settles in a few seconds here, since the fake build runner passes instantly. Run the command while it is still moving and you will catch it mid-pipeline — `speculating` is the usual one to see. The full trail a successful request records is:

```
accepted → started → validating → validated → batched → speculating → speculated → landing → landed
```

with `building` and `built` recorded alongside as events rather than statuses.

To watch a whole queue rather than one request:

```bash
make land-list QUEUE=test-queue     # a table of recent requests
make land-watch QUEUE=test-queue    # follow them until they settle
```

Both draw the table `make demo-pr` uses, but they do not carry the same information. `list` is a one-shot read of the queue's receipts and does not fetch histories, so its `STAGE` column is always `…`; `watch` follows the history API and fills the trail in as each request moves. Use `land-list` to see what is in the queue and `land-watch` to see where it is going.

## Make one fail

The fakes take instructions through the change URI itself, so a failure needs no configuration change and no restart. Append `?sq-fake=build-fail`:

```bash
make land QUEUE=test-queue \
  URI='git://git.example.com/demo/refs%2Fheads%2Ffeature-b/2222222222222222222222222222222222222222?sq-fake=build-fail'
make land-status QUEUE=test-queue SQID=test-queue/2
```

```
status: error
```

The request walks the same path as far as `speculating`, records `building`, and then goes terminal at `error` instead of landing. Other tokens follow the same `sq-fake=<token>` convention and are documented on the fake they drive — `provider-error` on the change provider, `unmergeable` and `mergecheck-error` on the merge checker, `trigger-error` and `build-error` on the build runner.

Submit a second change immediately behind the failing one and you can watch the property that makes a queue worth having:

```bash
make land QUEUE=test-queue \
  URI='git://git.example.com/demo/refs%2Fheads%2Fbad/4444444444444444444444444444444444444444?sq-fake=build-fail'
make land QUEUE=test-queue \
  URI='git://git.example.com/demo/refs%2Fheads%2Fgood/5555555555555555555555555555555555555555'
make land-watch QUEUE=test-queue
```

Both are in flight together, and `test-queue` serializes them — its conflict analyzer is the conservative `all`, so the second is batched behind the first and speculates on it succeeding. When the first fails that guess is contradicted, and the second re-plans and lands anyway. One bad change goes to `error`; the good one behind it still reaches `landed` without a human touching it.

## What this does not test

Everything that touches the outside world, which is exactly what the fakes replaced. No commit reaches a branch — Runway falls back to the **noop merger** whenever no `MERGE_CONFIG_PATH` is configured, and the local stack configures none. No CI runs. No change is marked merged anywhere.

Add the merge back with `make e2e-git-test`, which points Runway at a bare repository on a shared volume and asserts against the repository itself — still with no credential. Add the provider on top of that by following [PROVIDER-E2E.md](PROVIDER-E2E.md), which is the first tier that needs a token.

## Clean up

```bash
make local-stop                   # stop the services
make local-submitqueue-clean      # also remove volumes and images
```

`make local-stop` reports that data volumes are preserved, which is true of the volumes themselves but not of the data you can reach: both MySQL services mount **anonymous** volumes, so stopping detaches them and the next start creates a new, empty pair. Expect a restarted stack to have forgotten every request — and expect each cycle to leave a few hundred megabytes of orphaned volume behind, which is what eventually fills Docker up. `make local-submitqueue-clean` removes them as it goes.

## Troubleshooting

**MySQL exits immediately, and the stack fails with `dependency failed to start`.** Check `docker logs submitqueue-mysql-queue-1`. Two causes look similar:

- `No space left on device` — Docker is full. Every stop/start cycle orphans the two anonymous MySQL volumes (see Clean up), and integration and E2E runs add their own at a few hundred megabytes apiece; `docker system df` shows the total. `docker volume prune` reclaims every detached volume on the machine, so check `docker volume ls -f dangling=true` first if anything else of yours might be in there.
- `--initialize specified but the data directory has files in it` — a previous run died partway through initializing, leaving a half-written data directory that the next start cannot use. Remove that stack's volumes with `docker compose -f service/submitqueue/docker-compose.yml -p submitqueue down -v` and start again.

**A land is rejected before it returns an sqid.** The URI failed validation. Re-read the two rules above: 40 hex characters of SHA, and a percent-encoded `refs/…` ref.

**`grpcurl` reports `target server does not expose service`.** The server registers reflection, but its descriptor references `api/base/change/proto/change.proto` while the generated code registers that file as `change.proto`, so reflection cannot resolve the gateway's descriptor. Use the client CLI, which is what every command here does.

**Everything reports `error` immediately.** Check the queue exists in [`queues.yaml`](../../service/submitqueue/gateway/server/queues.yaml). `test-queue` is there by default.
