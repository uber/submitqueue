# git merger

A `merger.Merger` backed by the `git` CLI operating on a local checkout. It applies a merge request's ordered steps onto a target branch, honoring each step's strategy, and — for a committing merge — pushes the result. It is constructed by the wiring layer (see [`service/runway`](../../../../service/runway)) with the checkout path, remote, target branch, pinned git runtime, committer identity, and the default strategy; the request itself carries only a queue name and the changes, so the merger reads change URIs straight from the payload (no store, no resolver).

## Model

A request is an ordered list of steps; each step names a change (a set of provider URIs, each ending in a full head commit SHA) and a strategy. Steps are applied in order on top of the target tip — earlier steps are the in-flight base, the last step is the candidate. Each step yields one `StepResult`; the revisions a step produces on the target are its outputs, in application order.

A URI pins a change to one head commit, but a change is routinely several commits. The full set is recovered locally rather than from the wire: the commits to replay are the range from the change's merge base with the target up to its head. Applying the head commit alone would apply only that commit's diff against its own parent — conflicting against context its predecessors would have established, or silently dropping them when they touch different files.

## Change providers

Every URI is reduced to three things: the commit to apply, the ref the provider publishes that commit under, and a short label for synthesized commit messages. Adding a provider is one case in that mapping, not a change to any apply path.

| Scheme | Commit | Canonical ref | Label |
|---|---|---|---|
| `github://` | head commit SHA | `refs/pull/{n}/head` | `org/repo#n` |
| `git://` | the URI's commit SHA | the URI's own ref | `repo@ref` |

An unrecognized scheme is a terminal invalid request.

## Object availability

The default fetch refspec is `+refs/heads/*`, which does not cover a provider's change refs — a pull request head never also pushed as a branch, the normal case for a fork, is simply absent locally. Every referenced commit is therefore fetched and verified before any step is applied, so a request naming an unreachable commit fails without having touched the checkout.

Commits are requested by SHA, which relies on the server serving a want for an object that is reachable but not advertised (`uploadpack.allowReachableSHA1InWant`, which github.com enables); the provider's canonical ref is the fallback. Neither fetch is shallow — the apply paths need ancestry, not just the commit. A remote that supports neither can supply explicit refspecs via configuration.

A commit that cannot be fetched from a reachable remote is a terminal invalid request, not a conflict. If the remote itself is unreachable the error stays retryable.

The same line is drawn inside an apply: a non-zero exit from git is not by itself a conflict. The index is consulted for conflicted entries before the operation is aborted, so a missing object, an unreadable repository or a killed process stays a retryable infrastructure error instead of permanently failing a change that never actually collided.

## Staleness

Fetching by SHA guarantees the merger applies exactly the commit a URI names — not that the commit is still the change's head. A force-push leaves the superseded commit fetchable for some time on most hosts, so a successful fetch says nothing about freshness. When enabled, each change's canonical ref is read (one ref advertisement, no object transfer) and a mismatch is terminal. A ref that no longer exists yields no verdict.

## Strategies

| Strategy | What it does | Outputs |
|---|---|---|
| `REBASE` | Cherry-picks every commit each change introduces onto the tip, in order. A commit already present on the target is skipped (no output), as is one that was empty to begin with. | one revision per newly-created commit |
| `SQUASH_REBASE` | Applies each change like `REBASE`, then collapses the commits it produced into a single commit (squash unit = the change, not the step). | one revision per change, or none for a change already present |
| `MERGE` | Creates a `--no-ff` merge commit per change, keeping the change's original commits reachable through second-parent history. A commit already contained in the tip is skipped. | the merge-commit revision(s) |
| `DEFAULT` | Resolved to the instance's configured default strategy before any step runs. | per the resolved strategy |

`PROMOTE` is defined by the wire contract but not yet applied here — a step naming it is rejected as an invalid request.

## Importing an unrelated history

A repository migration arrives as an ordinary change in the target repo whose branch carries the source repo's entire history. Living in the target repo is what makes its commits fetchable; it says nothing about ancestry, and the two graphs still share no common ancestor, so git refuses the merge by default.

`MERGE` is the only strategy that can serve this, because it is the only one that leaves the imported commits reachable under their original hashes — the picking strategies would rewrite every one of them, and have no range to compute in the first place, so they reject such a change outright.

The refusal is lifted by a per-instance option rather than always: it is a real safeguard, and without it a merge of the wrong object fails loudly instead of quietly producing a nonsense result. A queue that exists to perform imports turns it on. A refusal that surfaces without the option is reported as an invalid request, not as a conflict — nothing collided.

Redelivery is safe: once imported, the source head is contained in the target, so the change is skipped rather than merged twice.

## Committing, dry-run, atomicity, contention

`Merge` commits and reports outputs; `CheckMergeability` runs the identical apply but never pushes, then resets the checkout to discard the local commits and reports empty outputs. A multi-step check commits its intermediate steps locally so it sees the same conflict surface a real merge would.

For a committing merge nothing reaches the remote until the final push. A step that fails to apply aborts its in-progress git operation and returns without pushing. If the push fails because the remote tip moved between reset and push, the whole reset/apply/push cycle is retried up to a bounded number of attempts; detection re-fetches the tip and compares it to the SHA the cycle was based on.

## Failure classification

A merge conflict surfaces as `merger.ErrConflict`. An unusable request surfaces as `merger.ErrInvalidRequest`: an unsupported strategy or URI scheme, a malformed URI, a commit a reachable remote cannot supply, a change whose head has moved on, or a change sharing no history with the target under a picking strategy. Both are terminal — the controller publishes a `FAILED` result rather than retrying. Everything else (network/auth/push faults, and an unreachable remote) is returned as a plain error for the consumer to retry.

The distinction between the last two matters operationally: a commit that is missing while the remote answers is a property of the request, whereas a remote that will not answer is a property of the moment.

## Runtime and identity

Every git invocation uses the pinned runtime (explicit executable, exec-path, and template dir) and a scrubbed environment: no ambient configuration, no system or global git config, no interactive prompts. Because that leaves no ambient identity, the committer name and email are injected per-invocation, which the commit-creating strategies (`REBASE`, `SQUASH_REBASE`, `MERGE`) require.

Scrubbing denies git ambient *configuration* — anything that could change what a merge produces. It deliberately does not deny it the means to reach the remote, so the agent socket, `PATH`, ssh-command, TLS and proxy variables are inherited when set. Without them an SSH remote cannot authenticate and git cannot even exec `ssh`; none of them can influence merge semantics. A deployment needing more can name additional variables on the runtime.

See [Object availability](#object-availability) for how referenced commits are obtained.
