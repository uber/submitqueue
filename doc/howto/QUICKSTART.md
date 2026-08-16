# Quickstart

Start the stack, put traffic through it, and watch changes land — beginning with nothing installed but Docker.

The stack always runs the same way. What changes is where the changes come from and what landing them does, chosen with `PROVIDER`:

| `PROVIDER` | A change is | Landing it | Needs |
|---|---|---|---|
| **`fake`** (default) | a URI, and nothing else | reports success without touching a repository | nothing |
| **`git`** | a branch in a bare repository on disk | a real fetch, cherry-pick and push | nothing |
| **`github`** | a real pull request | a real push to a real repository | a repository and a token |

They are a ladder, not alternatives: the same commands work on each rung, so you can start with the one that needs nothing and only pay for what you want to see next. Each is a directory of configuration under [`service/submitqueue/demo/provider/`](../../service/submitqueue/demo/provider) — the difference between rungs is two YAML files, not a code path.

## Start the stack

```bash
make local-submitqueue-start
```

This builds the Linux binaries, brings up Gateway, Orchestrator, Runway and two MySQL databases, and applies their schemas. The first run spends most of its time in the Bazel build; later ones start in seconds.

Compose publishes each service on a **random** host port so several stacks can run side by side, which means there is no fixed address to hard-code. The start-up output ends with the ports, and `make local-submitqueue-ps` prints them again at any time:

```
✅ Stack is running against provider 'fake'.

Gateway gRPC port: 58537
```

You do not have to note it down. Every command below finds the running stack's port for itself, which matters because Compose picks a fresh one on every start — a number copied from an earlier run is the most common reason a demo command cannot connect. Set `GATEWAY_ADDR=host:port` only to reach a gateway this Makefile did not start.

## Put traffic through it

```bash
make demo-requests
```

`demo-requests` creates changes, enqueues each the moment it exists, and watches them all until they settle:

```
Creating 3 change(s) across 8 folder(s) via fake changes (no repository) — independent, 5 at a time, each enqueued as soon as it is created

  REQUEST       CHANGES             ELAPSED  STAGE
  ────────────  ──────────────────  ───────  ─────────────────────────────────────────────
  demo-queue/1  demo/0814-135021/1      13s  accepted → started → validating → validated →
                                             batching → batched → speculating → speculated →
                                             landing → landed
```

The overlap is the point. A queue holding one request at a time never batches, never analyzes a conflict, and never speculates — so nothing is awaited until every change is in, and the queue is already working on the first while the last is still being created.

Each row shows the states its request passed through, not just the one it is in, read from the gateway's history API rather than sampled — a transition between two polls is not missed. `ELAPSED` runs from the moment the gateway accepted the request and stops when it settles, so a finished row keeps the time it took rather than counting on. The command exits non-zero if any request settles anywhere other than `landed`, so it works in a script.

```bash
make demo-requests COUNT=8              # more traffic
make demo-requests FOLDERS=1            # every change in one folder: all of them conflict
make demo-requests FOLDERS=50           # a folder each: none of them conflict
make demo-requests FILES=8              # wider changes, more files each
make demo-requests CONCURRENCY=1        # create them one at a time
make demo-requests STACKED=true         # one stack, enqueued as a single request
make demo-requests LAND=false           # create only, print the command to enqueue them
```

Independent changes are created **five at a time** by default (`CONCURRENCY`), because creating them serially is most of what a large run spends its time on and it delays the overlap the demo exists to show. A stack ignores the setting: each of its changes is based on the branch before it, so the next cannot be cut until the previous head exists.

A change touches several files rather than one, each committed separately, so it arrives as a multi-file, multi-commit change — closer to a real one, and enough to exercise replaying a range of commits. `FILES` sets the floor (default 3); the actual count varies a little above it, derived from the run tag so replaying a tag reproduces the same run.

Every change writes all of its files into one folder under `demo/`, and `FOLDERS` decides how many folders there are to land in — by default a number between five and ten, picked per run. That is what makes a run interesting rather than uniform, because `demo-queue` uses the `pathoverlap` analyzer keyed on the directory: two changes landing in the same folder are batched in order and the second speculates on the first, while changes in different folders go out beside each other. A run prints the number it picked, and repeating a run tag reproduces the same collisions.

Set it deliberately when you want a run to show one thing. `FOLDERS=1` puts every change in the same place, so the queue serializes the lot and each change speculates on the one before it. A number well above `COUNT` keeps them all apart, so they go out together.

How much speculation that turns into is capped by the queue's **build budget** — how many builds it may have occupying CI at once, counted across every in-flight batch rather than per batch. It defaults to 4 and is set per queue in the provider's `profiles.yaml`:

```yaml
defaults:
  speculator: {buildBudget: 4}

queues:
  - name: demo-queue
    speculator: {buildBudget: 12}
```

It is the other half of `FOLDERS`. Folders decide how many dependencies there are to speculate *about*; the budget decides how many of the possible outcomes the queue may hedge at once. `FOLDERS=1 buildBudget: 1` explores one path at a time and lands the slowest; raising the budget lets the queue build the "it fails" branch alongside the "it succeeds" one, which is what makes a failure cost nothing. A trail like `speculating [building ×8, built ×8]` below is a queue that kept finding paths worth funding.

Changing it needs a restart, since the file is read at startup — `make local-submitqueue-stop && make local-submitqueue-start`.

You can watch the queue reach that conclusion:

```bash
docker exec submitqueue-mysql-app-1 mysql -uroot -proot submitqueue \
  -N -e "select batch_id, dependents from batch_dependent where dependents != '[]'"
```

Every dependency listed is between changes sharing a folder; a batch that shares its folder with nothing in flight has none.

Landing one change by hand instead:

```bash
make land QUEUE=demo-queue \
  URI='git://demo.example.com/demo/refs%2Fheads%2Fmy-change/1111111111111111111111111111111111111111'
make land-status QUEUE=demo-queue SQID=demo-queue/1
```

A change URI is `git://{remote}/{repo}/{ref}/{commit_sha}`. Two parts are checked before anything else happens: the **commit SHA must be 40 lowercase hex characters**, and the **ref must be fully qualified and percent-encoded** — `refs%2Fheads%2Fmy-change`, not `my-change`. Encoding is what keeps a branch name containing slashes inside a single path segment.

## Watching the queue

The queue is readable without creating any traffic:

```bash
make land-list                       # a table of recent requests
make land-list SINCE=24h LIMIT=200   # a wider window
make land-watch                      # follow them until they settle
```

Both draw the same table `make demo-requests` does — the demo tool and the CLI share it — but against whatever the queue already holds, so watching a queue no longer means adding to it. They do not carry the same information, though: `list` is a one-shot read of the queue's receipts and does not fetch a history per request, so its `STAGE` column shows where each request is and not how it got there, while `watch` follows the history API and fills the whole trail in as each one moves.

That trail carries more than positions. A request records events while it sits at one — a build starting or finishing, a passed path waiting on a dependency — and those are shown against the status they happened under, with repeats counted:

```
accepted → started → validating → validated → batching → batched →
speculating [building ×8, built ×8, waiting] → speculated → landing → landed
```

Eight builds means the batch explored eight paths before one of them landed it — not eight at the same time, since the build budget above caps how many may hold CI at once and a finished build frees its slot for the next. `waiting` means a path passed and then sat on a dependency that had not resolved. A request that sailed through reads `speculating [building, built]` instead — the same position, a very different amount of work behind it.

`land-watch` fixes its set when it starts and exits non-zero if any request in that set finishes anywhere other than `landed`, which makes it usable from a script. A request accepted after the watch begins is not picked up: a watch that grew as the queue did would never finish.

Watching more requests than the window holds takes over the screen while it runs, the way `top` does, so the table can be scrolled rather than trimmed:

| Key | |
|---|---|
| `↑` `↓` or `k` `j` | one row |
| `PgUp` `PgDn` or `Space` | one screen |
| `g` `G` | first row, last row |
| `q` | stop watching |

The view follows the end of the table by default, so new rows and new stages appear without touching it. Scrolling up holds your place; scrolling back to the bottom starts following again. The screen you had is restored on exit and the finished table is printed into it whole, so nothing is lost with the view — and when output is redirected, none of this happens at all and the run stays a plain log.

A listing of a busy queue is mostly `speculating` rows, since that is where a request spends most of its active life — waiting on the build its batch was admitted for.

Under the hood these are `client list` and `client watch`, which take a queue and reach any gateway:

```bash
bazel run //service/submitqueue/gateway/client:gateway -- \
  -addr sq.example.com:443 -tls list -queue my-queue -since 1h
```

`-addr` is passed to the dialler untouched, so `dns:///host:port` and `unix:///path.sock` work as well as a plain `host:port`. Transport security is a separate flag rather than part of the address, because gRPC keeps target resolution and credentials apart — there is no `grpcs://` to write.

### Authentication

The gateway admits every caller. It is a sandbox stack, and nothing in it checks a credential.

The client can still present one, for a gateway reached through something that does — a proxy, a mesh sidecar, an ingress that terminates auth ahead of the service. It reads `SQ_TOKEN` by default and sends it as `Authorization: Bearer …`; `-token-env` names a different variable, and an unset one sends nothing rather than failing, which is how it stays usable against a stack that wants no credential.

```bash
SQ_TOKEN=$(cat ~/.sq-token) bazel run //service/submitqueue/gateway/client:gateway -- \
  -addr sq.example.com:443 -tls list -queue my-queue
```

### Service logs

```bash
make local-submitqueue-logs                  # every service
docker compose -p submitqueue logs -f runway-service   # one of them
```

The message queue logs a line per message published, fetched, leased and acked, which at debug level buries everything else a service says. It is levelled separately from the rest of the service, at info by default. To follow the queue itself — chasing a message that never arrived, or a partition that never got leased — turn it back up:

```bash
QUEUE_LOG_LEVEL=debug make local-submitqueue-start
```

`QUEUE_LOG_LEVEL` takes any zap level name. It can only raise the queue's level above the one the service logger was built with, never lower it, so it cannot be used to make a quiet service verbose.

## What this proves, and what it does not

The queue's own logic is real in every mode: validation, batching, conflict analysis, speculation, the request log, and the whole trail from `accepted` to `landed`.

In `fake` mode, everything at the edges is not. There is no repository, so a change is a URI that points at nothing; the build runner passes instantly; and Runway uses the **noop merger**, which reports success without touching anything. `landed` here means the pipeline ran to completion — not that a commit exists anywhere.

For that, take the next rung.

## Land into a real repository

```bash
make local-submitqueue-stop
PROVIDER=git make local-submitqueue-start
```

Still no credential. `PROVIDER=git` provisions a bare repository at `/tmp/sq-sandbox/sandbox.git`, points Runway at it by path, and prints it at startup:

```
✅ Stack is running against provider 'git'.

Gateway gRPC port: 55295
Merge target:      /tmp/sq-sandbox/sandbox.git
```

Then the same command as before, unchanged:

```bash
make demo-requests
```

`demo-requests` creates changes for whichever provider the running stack was started with, so there is nothing to repeat and nothing to keep in sync. The two must agree — a fake change points at no repository, so a stack running the git merger rejects every one of them as a commit it cannot find — and rather than leaving that to memory, a run with no `PROVIDER` of its own asks the stack which one it has. Passing one that disagrees still works, and says so before it starts.

Now `demo-requests` pushes real branches with real commits, and landing them is a real cherry-pick and push. Look at the repository itself:

```bash
git -C /tmp/sq-sandbox/sandbox.git log --oneline main
```

```
b517508 squash: demo-queue/5 (sandbox@refs/heads/demo/0814-134848/2)
25c86c5 squash: demo-queue/4 (sandbox@refs/heads/demo/0814-134848/1)
9f72dcf squash: demo-queue/3 (sandbox@refs/heads/demo/0814-134848/3)
b5d86d6 seed the sandbox
```

The commits are there, and they are not the ones that were pushed: `SQUASH_REBASE` replays each change onto the target rather than merging it, which is why the queue can keep the trunk linear.

One property worth seeing, because it is the thing a submit queue exists for. A stack lands as a single push, so no reader ever observes it half-applied:

```bash
git -C /tmp/sq-sandbox/sandbox.git reflog show refs/heads/main | wc -l
make demo-requests COUNT=3 STACKED=true
git -C /tmp/sq-sandbox/sandbox.git reflog show refs/heads/main | wc -l
```

Three changes, one more ref update than before.

The sandbox survives `make local-submitqueue-stop`, so what landed is still there to look at. `make local-submitqueue-clean` removes it.

## Land against GitHub

The last rung is the only one that needs credentials, and the only one where a change is a pull request that a person can review.

### What you need

A **scratch repository** you are willing to have commits pushed to and branches force-moved on. Do not point this at anything you care about — the merger pushes to the target branch and rewrites the head branch of every change it lands.

A **token** for it, scoped to that one repository.

For a **fine-grained** token, grant these repository permissions. Each is here because a specific component needs it, so you can drop the last two if you are not using those pieces:

| Permission | Access | Needed by |
|---|---|---|
| Metadata | Read | mandatory on every fine-grained token; GitHub adds it for you |
| Contents | Read and write | the git merger — clone, fetch, push to the target branch, and force-move each landed change's head branch |
| Pull requests | Read | the change provider reads pull request metadata, and `land -pr` reads the head commit |
| Pull requests | Read **and write** | only for `make demo-requests`, which opens pull requests |
| Actions | Read and write | only if you switch the build runner to GitHub Actions — dispatch a run, poll it, cancel it |

A **classic** PAT needs `repo`, plus `workflow` if you use the GitHub Actions build runner.

Two things people get caught by. Fine-grained tokens must have the repository explicitly selected under "Repository access" — org-owned repositories also need the org to have approved fine-grained tokens at all. And **Contents: Read and write is the one that cannot be reduced**: landing *is* pushing, so a read-only token fails at the last step, after everything else has appeared to work.

### Configure and run

Everything provider-specific is one directory: [`service/submitqueue/demo/provider/github/`](../../service/submitqueue/demo/provider/github). Edit the three marked lines in `merge.yaml`:

```yaml
remoteUrl: https://github.com/<you>/<your-scratch-repo>.git
target: main
checkoutPath: /var/runway/checkouts/<your-scratch-repo>
```

Neither file holds a secret — `tokenEnv: GITHUB_TOKEN` names the variable, and the value comes from your environment.

```bash
export GITHUB_TOKEN=ghp_...
PROVIDER=github make local-submitqueue-start
```

The token is required rather than defaulted: a stack that silently falls back to the fake integrations reports changes as landed without having gone near the provider, which is a much worse way to find out.

Then the same command as the other two rungs:

```bash
make demo-requests
```

It opens real pull requests, enqueues each as it is created, and watches them land — having picked up from the running stack that this one is GitHub. A three-change run against a scratch repo:

```
  REQUEST       CHANGES                                           ELAPSED  STAGE
  ────────────  ────────────────────────────────────────────────  ───────  ──────────────────────────────
  demo-queue/1  https://github.com/behinddwalls/sq-demo/pull/522      21s  accepted → … → landed
  demo-queue/2  https://github.com/behinddwalls/sq-demo/pull/523      22s  accepted → … → landed
  demo-queue/3  https://github.com/behinddwalls/sq-demo/pull/524      25s  accepted → … → landed
```

All three show **Merged** on GitHub and their commits are on `main`.

Worth understanding *why* they show merged, because nothing called an API to close them. A provider marks a change merged once its head commit is reachable from the target branch. `SQUASH_REBASE` rewrites the commits, so a pull request's original head is nowhere in `main` — and `updateHeadBranch` therefore moves its branch to the commit it landed as. GitHub draws its own conclusion from that.

`make demo-requests STACKED=true` submits a chain instead, each pull request targeting the previous one's branch. All of them land as one push to `main`, and all of them show as merged.

**Your CI does not run these builds.** Even here the build runner is fake, so a land takes seconds and costs no Actions minutes — GitHub supplies the change metadata and takes the push, and nothing else. That is worth knowing before reading `landed` as "CI passed on the combination", because it did not run. See [Using real CI](#using-real-ci) below.

### Land an existing pull request

For a pull request you opened yourself rather than one the demo created:

```bash
make land PR=https://github.com/<you>/<repo>/pull/1
```

`land` resolves the pull request's head commit and prints the change URI it built, so there is no 40-character SHA to copy. It returns an `sqid` to follow with `make land-status`.

A stack is a chain of pull requests where each targets the previous one's branch, submitted in order:

```bash
make land PRS="https://github.com/<you>/<repo>/pull/1 \
             https://github.com/<you>/<repo>/pull/2 \
             https://github.com/<you>/<repo>/pull/3"
```

The order of `PRS` is the stack order.

### Using real CI

The demo keeps the build runner fake so a land finishes in seconds. Switching to real GitHub Actions takes three things.

**1. The workflow must be dispatchable.** The runner triggers builds with `POST /actions/workflows/{id}/dispatches`, which only works if the workflow declares `workflow_dispatch`. A typical scratch-repo `ci.yml` triggered on `pull_request` alone cannot be dispatched at all — GitHub rejects it. Add the trigger and the inputs the runner sends:

```yaml
on:
  pull_request:
  merge_group:
  workflow_dispatch:
    inputs:
      sq_head_uris:
        description: "JSON array of change URIs in the batch under test"
        required: false
      sq_base_uris:
        description: "JSON array of in-flight change URIs this batch speculates on top of"
        required: false
      sq_queue:
        description: "SubmitQueue queue name"
        required: false
      sq_metadata:
        description: "Caller-supplied build metadata, as JSON"
        required: false
```

**2. Point the runner at it.** Replace the `buildRunner` line for the queue in `profiles.yaml`:

```yaml
buildRunner:
  type: githubactions
  owner: behinddwalls
  repo: sq-demo
  workflow: ci.yml        # file name or numeric workflow id
  ref: main               # the branch the workflow definition is read from
```

**3. Grant Actions: Read and write** on the token (see the permissions table above).

One caveat worth understanding before you rely on the result. A workflow that only checks out the pull request tests *that change alone* — which is not what a submit queue is for. The point of speculation is to test the **combination**: `sq_base_uris` are the in-flight changes assumed to land first, and `sq_head_uris` is the batch under test on top of them. Until the workflow actually applies both, a green run says nothing about whether the batch lands cleanly, and the queue is only exercising its trigger-and-poll loop.

## Make a change fail

Back on the `fake` rung, where a change is a URI and nothing has to exist for one to name it. The fakes take instructions through the change URI itself, so a failure needs no configuration change and no restart. Append `?sq-fake=build-fail`:

```bash
make land QUEUE=demo-queue \
  URI='git://demo.example.com/demo/refs%2Fheads%2Fbad/2222222222222222222222222222222222222222?sq-fake=build-fail'
```

That request walks the same path as far as `speculating`, records `building`, and then goes terminal at `error` instead of landing. Other tokens follow the same `sq-fake=<token>` convention and are documented on the fake they drive — `provider-error` on the change provider, `unmergeable` and `mergecheck-error` on the merge checker, `trigger-error` and `build-error` on the build runner.

A hand-written URI like the one above belongs to the `fake` rung alone. On `git` it names a commit the merger cannot fetch, and on `github` the change provider tries to resolve it as a pull request — both fail, but for reasons that have nothing to do with the marker.

Submit a good change into the **same folder** as a failing one and you can watch what makes a queue worth having: the two are batched in order, and the second speculates on the first landing. When the first fails, that guess is contradicted, the second re-plans, and it lands anyway.

## Clean up

```bash
make local-submitqueue-stop    # stop the services; PROVIDER=git's repository stays
make local-submitqueue-clean    # also delete the sandbox repository
```

A GitHub scratch repository keeps whatever landed; reset it with `git push --force` from a known-good commit.

Both MySQL services mount **anonymous** volumes, so a stop/start cycle orphans a pair and comes back to an empty database. Expect a restarted stack to have forgotten every request, and expect each cycle to leave a few hundred megabytes of orphaned volume behind — which is what eventually fills Docker up.

## Troubleshooting

**MySQL exits immediately, and the stack fails with `dependency failed to start`.** Check `docker logs submitqueue-mysql-queue-1`. Two causes look similar:

- `No space left on device` — Docker is full, usually of the orphaned volumes above; `docker system df` shows the total. `docker volume prune` reclaims every detached volume on the machine, so check `docker volume ls -f dangling=true` first if anything else of yours might be in there.
- `--initialize specified but the data directory has files in it` — a previous run died partway through initializing. Remove that stack's volumes with `make local-submitqueue-clean` and start again.

**A land is rejected before it returns an sqid.** The URI failed validation: 40 hex characters of SHA, and a percent-encoded `refs/…` ref.

**Everything reports `error` immediately.** Check the queue name exists in [`queues.yaml`](../../service/submitqueue/gateway/server/queues.yaml) and is configured in the provider directory you are running. A queue with no entry in `merge.yaml` gets the noop merger by design, so it will appear to land without pushing anything.

**`PROVIDER=git` lands report `error`.** Read Runway's log and look for the git command that failed. A change whose branch was never pushed to the sandbox, or a `demo-requests` run whose `PROVIDER` did not match the stack's, both surface here as a failed fetch or cherry-pick.

**`PROVIDER=github`: the push is rejected on the first try.** Branch protection on `main` — required status checks, or a linear-history or no-force-push rule — applies to the merger like anyone else. Either relax it on the scratch repo or add the token's identity to the bypass list.

**`PROVIDER=github`: the change lands but the pull request stays open.** Two causes, distinguishable in Runway's logs. If the change came from a **fork**, this is expected and permanent: the head branch lives in the contributor's repository, which this stack has no business writing to, and the log says `no head branch on this remote for change`. Otherwise it is **protection on the head branch** blocking the force update, logged as `could not move change head branch`. The land itself succeeded either way — the failure is reported and deliberately not retried, because the push already happened and cannot be undone.

**A change is rejected as stale.** Its head moved after it was submitted, so the commit named is no longer the one under review. Re-submit it. This also happens if you re-land a change that already landed, since landing moved its branch.

**`grpcurl` reports `target server does not expose service`.** The server registers reflection, but its descriptor references `api/base/change/proto/change.proto` while the generated code registers that file as `change.proto`, so reflection cannot resolve the gateway's descriptor. Use the client CLI, which is what every command here does.

## Adding another provider

Nothing above is GitHub-specific except the contents of that configuration directory and the URI parser behind it. Adding GitLab or another provider is a new directory plus a handful of new files beside the existing ones — the complete list is in [`service/submitqueue/demo/provider/README.md`](../../service/submitqueue/demo/provider/README.md).
