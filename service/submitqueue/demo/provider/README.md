# Provider configurations

Each directory here is one **provider** — a code-hosting system SubmitQueue lands changes on — described entirely as configuration. A directory holds two files:

| File | Selects |
|---|---|
| `profiles.yaml` | the change provider, build runner, and conflict analyzer each queue resolves to (read by the orchestrator) |
| `merge.yaml` | the merge target each queue lands on (read by Runway) |

Neither holds a secret. Each integration names the *environment variable* carrying its credential, so these files stay committable and rotating a token needs no edit.

Pick one with `make local-provider-start PROVIDER=<name>`, which bind-mounts that directory into the orchestrator and Runway. Because the choice is a mount rather than a build input, switching providers needs no rebuild.

| Directory | What it demonstrates |
|---|---|
| [`github/`](github) | a live provider: GitHub change metadata, a real repository, pull requests marked merged |
| [`local/`](local) | a plain git remote with no provider at all — used by the hermetic git E2E (`make e2e-git-test`) |

`local/` is worth reading first. It is proof that the merge machinery has no provider in it: the same Runway code path lands changes against a bare repository addressed by path, with no credential and no API.

## Adding a provider

Everything provider-specific is reached through an existing seam, so a new provider is new code beside the old, not a change to it. The complete list:

| Touchpoint | Why |
|---|---|
| `platform/base/change/{provider}/change_id.go` | parse `{provider}://…` change URIs |
| `runway/extension/merger/git/changeref.go` | one `case` in `resolveChange`, mapping the URI to the ref the provider publishes a change's head under — GitHub `refs/pull/{n}/head`, GitLab `refs/merge-requests/{iid}/head` |
| `submitqueue/extension/changeprovider/{provider}/` and one case in `changeprovider/routing` | fetch change metadata |
| `submitqueue/extension/buildrunner/{provider}/` | only if the provider's CI is not already covered by the Buildkite or GitHub Actions runners |
| `service/submitqueue/gateway/client/main.go` | one case in `resolvePullRequest`, so `land -pr <url>` accepts the provider's change URLs |
| this directory | a `{provider}/` with the two files above |

What is **not** on that list is the point of it: the merger's apply and push paths, the head-branch update, the orchestrator pipeline, the wire contract, the compose overlay, and the hermetic git E2E are all provider-independent and need no change.

Two of those deserve explanation.

**The merger stays provider-neutral** because `resolveChange` reduces every URI to the same three things — the commit to apply, the ref it lives under, and a label — before any git command runs. The apply paths never learn which provider a change came from.

**Marking a change merged needs no provider API.** A provider decides whether a change merged while it processes the push to the target branch, comparing the change's recorded head against what that push makes reachable. `MERGE` and `PROMOTE` satisfy that by construction; the rewriting strategies do not, so `updateHeadBranch` moves the change's head branch to the commit it landed as — as its own push, immediately before the target is pushed. The ordering is the mechanism: a head moved *after* the target has been pushed, or in the same atomic push, is recorded too late, and the provider marks the change closed rather than merged even though its head is demonstrably on the target. That works by matching a SHA against the remote's branch tips — no change number, no API call — so it behaves identically for a GitHub pull request and a GitLab merge request. The one case it cannot serve is a change proposed from a fork, whose head branch lives in another repository: such a change lands and stays open.

See [doc/howto/PROVIDER-E2E.md](../../../../doc/howto/PROVIDER-E2E.md) for running a real land end to end.
