# Landing real changes against a provider

How to run the whole pipeline against a live repository and watch a change actually land. This is the manual tier: it needs a scratch repository and a token, which is why it is not automated in CI.

Two tiers below it run with no credentials at all and cover most of what can break:

| | Command | Covers | Secrets |
|---|---|---|---|
| Tier 1 | `make e2e-test` | pipeline choreography, on the noop merger | none |
| Tier 2 | `make e2e-git-test` | real git: provisioning, cherry-pick, atomic push, head-branch updates | none |
| Tier 3 | this document | the change provider: reading metadata, its CI, changes marked merged | a token |

Run tier 2 first. If the merge machinery is broken, it will say so in under a minute and without a repository to clean up afterwards.

## What you need

A **scratch repository** you are willing to have commits pushed to and branches force-moved on. Do not point this at anything you care about — the merger pushes to the target branch and rewrites the head branch of every change it lands.

A **token** for it, scoped to that one repository.

For a **fine-grained** token, grant these repository permissions. Each is here because a specific component needs it, so you can drop the last two if you are not using those pieces:

| Permission | Access | Needed by |
|---|---|---|
| Metadata | Read | mandatory on every fine-grained token; GitHub adds it for you |
| Contents | Read and write | the git merger — clone, fetch, push to the target branch, and force-move each landed change's head branch |
| Pull requests | Read | the change provider reads pull request metadata, and `land -pr` reads the head commit |
| Pull requests | Read **and write** | only for `make demo-pr`, which opens pull requests |
| Actions | Read and write | only if you switch the build runner to GitHub Actions — dispatch a run, poll it, cancel it |

A **classic** PAT needs `repo`, plus `workflow` if you use the GitHub Actions build runner.

Two things people get caught by. Fine-grained tokens must have the repository explicitly selected under "Repository access" — org-owned repositories also need the org to have approved fine-grained tokens at all. And **Contents: Read and write is the one that cannot be reduced**: landing *is* pushing, so a read-only token fails at the last step, after everything else has appeared to work.

## Configure

Everything provider-specific is one directory: [`service/submitqueue/demo/provider/github/`](../../service/submitqueue/demo/provider/github). Edit the three marked lines in `merge.yaml`:

```yaml
remoteUrl: https://github.com/<you>/<your-scratch-repo>.git
target: main
checkoutPath: /var/runway/checkouts/<your-scratch-repo>
```

Neither file holds a secret — `tokenEnv: GITHUB_TOKEN` names the variable, and the value comes from your environment.

## Run

```bash
export GITHUB_TOKEN=ghp_...
make local-provider-start PROVIDER=github
```

The stack refuses to start without the token rather than falling back to the fake integrations. That is deliberate: a stack that silently runs on fakes reports changes as landed without having gone near the provider, which is a much worse way to find out.

`local-provider-start` prints the gateway's port. Export it so the commands below are shorter:

```bash
export GATEWAY_ADDR=localhost:<port>
```

## Land a single change

Open a pull request against `main` in the scratch repo, then:

```bash
make land PR=https://github.com/<you>/<repo>/pull/1
```

`land` resolves the pull request's head commit and prints the change URI it built, so there is no 40-character SHA to copy. It returns an `sqid`; follow it with:

```bash
make land-status SQID=<sqid> GATEWAY_ADDR=$GATEWAY_ADDR
```

The status walks `accepted → started → validated → batched → landed`. When it reaches `landed`, on GitHub the pull request shows **Merged** and its commit is on `main`.

Worth understanding *why* it shows merged, because nothing called an API to close it. A provider marks a change merged once its head commit is reachable from the target branch. `SQUASH_REBASE` rewrites the commits, so the pull request's original head is nowhere in `main` — and `updateHeadBranch` therefore moves the pull request's branch to the commit it landed as. GitHub draws its own conclusion from that.

## Land a stack

Open a chain of pull requests where each targets the previous one's branch, then submit them in order:

```bash
make land PRS="https://github.com/<you>/<repo>/pull/1 \
             https://github.com/<you>/<repo>/pull/2 \
             https://github.com/<you>/<repo>/pull/3"
```

The order of `PRS` is the stack order. All three land as **one push** to `main` — there is no window where a reader sees the stack half-applied — and all three show as merged. Tier 2 asserts the single-push property mechanically, by counting ref updates in the target's reflog.

## Simulating traffic

Opening pull requests by hand gets old fast. `demo-pr` creates them, enqueues them, and shows you where each one is:

```bash
make demo-pr                      # 3 independent PRs, each enqueued as it is created
make demo-pr COUNT=8              # more traffic
make demo-pr FILES=8              # wider changes, more files per PR
make demo-pr CONCURRENCY=1        # create them one at a time
make demo-pr STACKED=true         # one stack, enqueued as a single request
make demo-pr LAND=false           # create only, print the land command
```

Each pull request is enqueued the moment it exists, so the queue is already working on the first while the last is still being opened. That overlap is the point: a queue holding one request at a time never batches, never analyzes a conflict against another batch, and never speculates. Nothing is awaited until every request is in.

Independent pull requests are created **five at a time** by default (`CONCURRENCY`). Opening one is several round trips — a branch, a commit per file, the pull request itself — so creating them serially was most of what a large run spent its time on, and it delayed the overlap the demo exists to show. A stack ignores the setting: each of its changes is based on the branch before it, so the next cannot be cut until the previous head exists. Lower it if the provider starts refusing bursts.

The table is there from the start — one row per land request, drawn before the first pull request exists and filled in as the run proceeds. Whatever is happening right now is a single line underneath it, so creating and enqueuing does not scroll the table away:

```
  REQUEST        CHANGES  ELAPSED  STAGE
  ─────────────  ───────  ───────  ────────────────────────────────────────────────
  demo-queue/12  #31          34s  accepted → started → validating → validated →
                                   batched → speculating → speculated → landing →
                                   landed
  demo-queue/13  #32          31s  accepted → started → validating → validated
  demo-queue/14  #33          28s  accepted → started

  ▸ 1 of 3 settled
```

Each row shows the states its request passed through, not just the one it is in. That comes from the gateway's history API rather than from sampling the current status, so a transition between two polls is not missed. `CHANGES` links to the pull request: on a terminal `#31` is clickable, and in a redirected run it is written out as a full URL instead. `ELAPSED` runs from the moment the gateway accepted the request and stops when it settles, so a finished row keeps the time it took rather than counting on.

The trail is as detailed as what the pipeline reports, which is the full walk: `accepted`, `started`, `validating`, `validated`, `batched`, `speculating`, `speculated`, `landing`, and then a terminal `landed`, `error` or `cancelled`. `building` and `built` are recorded alongside as events rather than statuses. A long pause on `speculating` is the batch waiting on its build, not a stuck request.

`STACKED=true` is the exception to the overlap: one request carries the whole chain, so it can only go in once every pull request in it exists. That is the atomic-stack path — the whole set reaches `main` in a single push, and the table shows it as the single row it is.

It talks to GitHub over the REST API with the same `GITHUB_TOKEN`, so it needs no clone and no git binary. Each run tags its branches with a timestamp so repeated runs do not collide, and every file a change writes is at a path no other change uses, so independent changes do not conflict by accident.

A change touches several files rather than one, each committed separately, so it arrives as a multi-file, multi-commit pull request — closer to a real change, and enough to exercise replaying a range of commits. `FILES` sets the floor (default 3); the actual count varies a little above it, derived from the run tag so replaying a tag reproduces the same run. Paths are sharded into two levels of hex buckets under `demo/` (`demo/c2/91/<tag>-<change>-<file>.txt`), which keeps the tree from degenerating into one enormous directory as runs accumulate.

The command exits non-zero if any request settles anywhere other than `landed`, so it works in a script. Piped to a file it prints a fresh table whenever a request moves — and not when only the clock did — instead of redrawing in place.

## Watching it work

The queue itself is readable without creating any traffic:

```bash
make land-list                       # a table of recent requests
make land-list SINCE=24h LIMIT=200   # a wider window
make land-watch                      # follow them until they settle
```

Both draw the same table `make demo-pr` does — the demo tool and the CLI share it — but against whatever the queue already holds, so watching a queue no longer means adding to it. `land-watch` fixes its set when it starts and exits non-zero if any request in that set finishes anywhere other than `landed`, which makes it usable from a script. A request accepted after the watch begins is not picked up: a watch that grew as the queue did would never finish.

Under the hood these are `client list` and `client watch`, which take a queue and reach any gateway:

```bash
bazel run //service/submitqueue/gateway/client:gateway -- \
  -addr sq.example.com:443 -tls list -queue my-queue -since 1h
```

`-addr` is passed to the dialler untouched, so `dns:///host:port` and `unix:///path.sock` work as well as a plain `host:port`. Transport security is a separate flag rather than part of the address, because gRPC keeps target resolution and credentials apart — there is no `grpcs://` to write.

A listing of a busy queue is mostly `speculating` rows, since that is where a request spends most of its active life — waiting on the build its batch was admitted for.

### Authentication

The gateway admits every caller. It is a sandbox stack, and nothing in it checks a credential.

The client can still present one, for a gateway reached through something that does — a proxy, a mesh sidecar, an ingress that terminates auth ahead of the service. It reads `SQ_TOKEN` by default and sends it as `Authorization: Bearer …`; `-token-env` names a different variable, and an unset one sends nothing rather than failing, which is how it stays usable against a stack that wants no credential.

```bash
SQ_TOKEN=$(cat ~/.sq-token) bazel run //service/submitqueue/gateway/client:gateway -- \
  -addr sq.example.com:443 -tls list -queue my-queue
```

### Service logs

```bash
docker compose -p submitqueue-provider logs -f runway-service
```

Runway logs each merge and each head-branch move:

```
moved change head branch to its landed commit  {"change": "you/repo#1", "branch": "refs/heads/feature-a", ...}
```

The message queue logs a line per message published, fetched, leased and acked, which at debug level buries everything else a service says. It is levelled separately from the rest of the service, at info by default. To follow the queue itself — chasing a message that never arrived, or a partition that never got leased — turn it back up for the services you care about:

```bash
QUEUE_LOG_LEVEL=debug make local-submitqueue-start
```

`QUEUE_LOG_LEVEL` takes any zap level name. It can only raise the queue's level above the one the service logger was built with, never lower it, so it cannot be used to make a quiet service verbose.

## When it does not work

**The push is rejected on the first try.** Branch protection on `main` — required status checks, or a linear-history or no-force-push rule — applies to the merger like anyone else. Either relax it on the scratch repo or add the token's identity to the bypass list.

**The change lands but the pull request stays open.** Two causes, distinguishable in Runway's logs.

If the change came from a **fork**, this is expected and permanent: the head branch lives in the contributor's repository, which this stack has no business writing to. The log says `no head branch on this remote for change`. The change is on `main`; only the pull request's status is wrong.

Otherwise it is **protection on the head branch** blocking the force update. The log says `could not move change head branch`. Note the land itself succeeded — the failure is reported and deliberately not retried, because the push already happened and cannot be undone.

**A change is rejected as stale.** Its head moved after it was submitted, so the commit named is no longer the one under review. Re-submit it. This also happens if you re-land a change that already landed, since landing moved its branch.

**Everything reports `error` immediately.** Check the queue name exists in [`queues.yaml`](../../service/submitqueue/gateway/server/queues.yaml) and matches the one in `profiles.yaml` and `merge.yaml`. A queue with no entry in `merge.yaml` gets the noop merger by design, so it will appear to land without pushing anything.

## Using real CI

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

## Clean up

```bash
make local-provider-stop
```

The scratch repository keeps whatever landed; reset it with `git push --force` from a known-good commit.

## Another provider

Nothing above is GitHub-specific except the contents of the config directory and the URI parser behind it. Adding GitLab or another provider is a new directory here plus a handful of new files beside the existing ones — the complete list is in [`service/submitqueue/demo/provider/README.md`](../../service/submitqueue/demo/provider/README.md).
