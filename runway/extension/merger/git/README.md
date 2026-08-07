# git merger

A `merger.Merger` backed by the `git` CLI operating on a local checkout. It applies a merge request's ordered steps onto a target branch, honoring each step's strategy, and — for a committing merge — pushes the result. It is constructed by the wiring layer (see [`service/runway`](../../../../service/runway)) with the checkout path, remote, target branch, pinned git runtime, committer identity, and the default strategy; the request itself carries only a queue name and the changes, so the merger reads change URIs straight from the payload (no store, no resolver).

## Model

A request is an ordered list of steps; each step names a change (a set of provider URIs, each ending in a full head commit SHA) and a strategy. Steps are applied in order on top of the target tip — earlier steps are the in-flight base, the last step is the candidate. Each step yields one `StepResult`; the revisions a step produces on the target are its outputs, in application order.

The URI is the unit of application. A step's change may carry several URIs — a stack — and the step's strategy applies to each of them, the same way to each, in the order given. So a step is never a mix of strategies, and its outputs are the concatenation of what each URI produced: one revision per created commit under `REBASE`, one per URI under `SQUASH_REBASE` and `MERGE`. `PROMOTE` is the exception that admits only one URI, since advancing a ref to an exact revision cannot repeat.

A URI pins a change to one head commit, but a change is routinely several commits. The full set is recovered locally rather than from the wire: the commits to replay are the range from the change's merge base with the target up to its head. Applying the head commit alone would apply only that commit's diff against its own parent — conflicting against context its predecessors would have established, or silently dropping them when they touch different files.

## Change providers

Every URI is reduced to three things: the commit to apply, the ref the provider publishes that commit under, and a short label for synthesized commit messages. Adding a provider is one case in that mapping, not a change to any apply path.

| Scheme | Commit | Canonical ref | Label |
|---|---|---|---|
| `github://` | head commit SHA | `refs/pull/{n}/head` | `org/repo#n` |
| `git://` | the URI's commit SHA | the URI's own ref | `repo@ref` |

An unrecognized scheme is a terminal invalid request.

## What a request must agree on

The supported providers are a property of this merger, not of the queue or of the wire contract: the URI scheme selects the parser, and a scheme with no case is a terminal invalid request. Nothing upstream filters on it, so an unsupported provider is first refused here.

Beyond the scheme, every change in one request must come from the same provider. There is no sense in one merge being addressed through two of them, and the check runs before any git command, so an incoherent request costs nothing and leaves the checkout untouched.

Whether a change actually belongs to the repository this merger serves is not checked here — the merger is constrained to its checkout and remote by configuration, and a change it cannot fetch is refused on those grounds.

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
| `PROMOTE` | Fast-forwards the target to an already-existing commit — no content transform, no new revision. Must be the entire request (one step, one change, one URI). | the exact named revision |
| `DEFAULT` | Resolved to the instance's configured default strategy before any step runs. | per the resolved strategy |

`PROMOTE` is exclusive because a pre-existing commit cannot descend from commits an earlier transforming step produced, and it advances the ref to an exact SHA rather than to the locally-built HEAD. Mixing it with any other step is rejected as an invalid request.

## Authorship

Git records an author and a committer separately, and the merger keeps them apart: the committer is always the merger's configured identity, because it is what applied the change, while the author is the person who wrote it.

`REBASE` gets this for free — cherry-pick carries each commit's author across, so every landed commit keeps whoever wrote it. The strategies that mint a fresh commit do not: the squash commit and the `--no-ff` merge commit are new objects, and without attribution both would be credited to the service. Each is therefore authored as the author recorded on the commit its change's URI pins. For a change spanning several commits by different people, that is the head commit's author — the one identity the request actually names.

The author is read out of the local object store, so this costs no network and needs nothing on the wire: every referenced commit is already fetched and verified before a step is applied. A commit that records no usable author (either half missing) falls back to the committer identity rather than failing the merge.

The author travels to git through `GIT_AUTHOR_NAME`/`GIT_AUTHOR_EMAIL` rather than `git commit --author`, because `git merge` has no `--author` flag and because the environment keeps the name and address as separate values — a single `Name <address>` string has to be parsed back apart, which a display name containing an angle bracket breaks.

## Importing an unrelated history

A repository migration arrives as an ordinary change in the target repo whose branch carries the source repo's entire history. Living in the target repo is what makes its commits fetchable; it says nothing about ancestry, and the two graphs still share no common ancestor, so git refuses the merge by default.

`MERGE` is the only strategy that can serve this, because it is the only one that leaves the imported commits reachable under their original hashes — the picking strategies would rewrite every one of them, and have no range to compute in the first place, so they reject such a change outright.

The refusal is lifted by a per-instance option rather than always: it is a real safeguard, and without it a merge of the wrong object fails loudly instead of quietly producing a nonsense result. A queue that exists to perform imports turns it on. A refusal that surfaces without the option is reported as an invalid request, not as a conflict — nothing collided.

Redelivery is safe: once imported, the source head is contained in the target, so the change is skipped rather than merged twice.

## Committing, dry-run, atomicity, contention

`Merge` commits and reports outputs; `CheckMergeability` runs the identical apply but never pushes, then resets the checkout to discard the local commits and reports empty outputs. A multi-step check commits its intermediate steps locally so it sees the same conflict surface a real merge would.

For a committing merge nothing reaches the remote until every step has applied cleanly (a `PROMOTE` is itself a single atomic fast-forward ref update). A step that fails to apply aborts its in-progress git operation and returns without pushing. With head-branch updates enabled a merge writes two things rather than one — the head branches first, then the target — and only the target's push is the point of no return. If the push fails because the remote tip moved between reset and push, the whole reset/apply/push cycle is retried up to a bounded number of attempts; detection re-fetches the tip and compares it to the SHA the cycle was based on.

## Head branches

A provider decides whether a change merged while it processes the push to the target branch, comparing the change's recorded head against what that push makes reachable. `MERGE` and `PROMOTE` satisfy that on their own — the first keeps the change's head reachable through second-parent history, the second fast-forwards the target to it. The picking strategies do not: `REBASE` and `SQUASH_REBASE` produce new commits, so the change's original head appears nowhere in the target's history and the change is recorded as closed after it has, in every meaningful sense, landed.

Enabling head-branch updates closes that gap. Before the target is pushed, each change's head branch is moved to the commit that change became — its last replayed commit under `REBASE`, its single squashed commit under `SQUASH_REBASE`. The provider records that new head, and when the target push arrives moments later it finds exactly that commit reachable, so it marks the change merged. Nothing here knows what a pull request is: the branch is found by matching the change's pinned head SHA against the remote's branch tips, so the same mechanism serves a GitHub pull request, a GitLab merge request, or a bare branch.

**The ordering is the mechanism.** Moving the head branch *after* the target has been pushed leaves the provider comparing against the pre-merge head at the only moment it looks, and it records the change closed rather than merged — even though the branch ends up on a commit that is demonstrably in the target. Doing both in a single atomic push behaves the same way, since the provider still evaluates the target update against the head it had recorded beforehand. Only a separate, earlier push works.

Three cases are declined rather than guessed at. A change whose head matches **no branch** on this remote is normally one proposed from a fork, whose branch lives in another repository and is not this merger's to move — such a change lands normally. A head matching **several branches** is ambiguous, and the URI does not say which one the change was proposed from, so rewriting a guess risks clobbering an unrelated branch. The **target branch itself** is never a candidate, so a change whose head coincides with the target tip cannot make the merger rewrite the branch it just landed on.

Each push carries a lease against the SHA the change's URI pinned, so an author who pushes in the window between reading the remote's branches and updating them fails the lease instead of losing their work. A failure to move a branch fails the merge, before the target is pushed: landing a change while knowing its head could not be moved produces exactly the half-merged state the option exists to prevent. The three declined cases above are not failures and do not stop the merge.

A branch a failed attempt already moved is remembered for the next one. Once moved, it no longer sits at the SHA the URI pinned, so a retry could not find it by matching tips and would strand it on a commit that never landed; the attempt's resolved branch and the value the next lease must name are carried forward instead.

Off by default — moving a branch the merger was not asked to move is a surprise unless a deployment opted in.

## Failure classification

A merge conflict surfaces as `merger.ErrConflict`. An unusable request surfaces as `merger.ErrInvalidRequest`: an unsupported strategy or URI scheme, a malformed URI, an invalid `PROMOTE` composition, a commit a reachable remote cannot supply, a change whose head has moved on, or a change sharing no history with the target under a picking strategy. Both are terminal — the controller publishes a `FAILED` result rather than retrying. Everything else (network/auth/push faults, and an unreachable remote) is returned as a plain error for the consumer to retry.

The distinction between the last two matters operationally: a commit that is missing while the remote answers is a property of the request, whereas a remote that will not answer is a property of the moment.

## Runtime and identity

Every git invocation uses the pinned runtime (explicit executable, exec-path, and template dir) and a scrubbed environment: no ambient configuration, no system or global git config, no interactive prompts. Because that leaves no ambient identity, the committer name and email are injected per-invocation, which the commit-creating strategies (`REBASE`, `SQUASH_REBASE`, `MERGE`) require.

Scrubbing denies git ambient *configuration* — anything that could change what a merge produces. It deliberately does not deny it the means to reach the remote, so the agent socket, `PATH`, ssh-command, TLS and proxy variables are inherited when set. Without them an SSH remote cannot authenticate and git cannot even exec `ssh`; none of them can influence merge semantics. A deployment needing more can name additional variables on the runtime.

See [Object availability](#object-availability) for how referenced commits are obtained.
