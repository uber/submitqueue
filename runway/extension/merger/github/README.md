# github merger

A `merger.Merger` that lands `github://` changes through the GitHub REST API instead of a local checkout. GitHub performs the merge itself, so branch rules, the pull request's merged state, merge commit attribution and the rebasing of a stack's remaining pull requests are all GitHub's. It is constructed by the wiring layer (see [`service/runway`](../../../../service/runway)) with an HTTP client, the repository it serves (host, owner, repo), the trunk branch and the default strategy.

## Model

A request is an ordered list of steps, and a step's change is an ordered list of URIs — a stack, bottom first. This merger maps that directly onto GitHub's [stacked pull requests](https://docs.github.com/en/pull-requests/get-started/about-stacked-prs): **one step is one GitHub stack**, and a single pull request is the one-element case.

A step is landed with one call to the [asynchronous merge API](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request-asynchronously) on the step's top pull request. For a stacked pull request GitHub merges it together with every unmerged pull request below it, as one operation that either lands all of them or none, ordered bottom-up in the resulting history. Pull requests above the step's top stay open and are rebased onto the trunk by GitHub. The merge runs with `direct_merge`: Runway is the queue, so handing the pull requests to GitHub's own merge queue would let a second queue reorder what this one decided.

Each step's strategy maps onto a GitHub merge method — `REBASE` to `rebase`, `SQUASH_REBASE` to `squash`, `MERGE` to `merge` — and `DEFAULT` resolves to the configured default first. `PROMOTE` advances a ref to an existing revision, which no pull request merge expresses, so it is an invalid request here; the git merger serves it.

Each URI's output is the merge commit GitHub records for its pull request: the squash commit, the merge commit, or, for a rebase, the last commit the rebase created for that pull request. That is one output per URI, where the git merger reports one per created commit under `REBASE`. It is read from the pull request's `merged` issue event, because API version 2026-03-10 no longer reports `merge_commit_sha` on a merged pull request. GitHub reports a stack merge settled a moment before every pull request in it shows its merge, so the merger re-reads until each is recorded.

## Live tests

Two suites run against a real repository whenever `SQ_GITHUB_TOKEN` and `SQ_GITHUB_TEST_REPO=owner/repo` are set, and skip otherwise (see [`test/testutil/githubtestrepo`](../../../../test/testutil/githubtestrepo)). Both open, stack and merge throwaway pull requests and check what GitHub recorded:

- [`test/integration/runway/extension/merger/github`](../../../../test/integration/runway/extension/merger/github) drives this merger on its own (`make integration-test-runway-merger`).
- `TestGitHubLandE2E` in [`test/e2e/submitqueue`](../../../../test/e2e/submitqueue) lands pull requests through the whole stack — gateway, orchestrator with the GitHub change provider, and Runway with this merger (`make e2e-test`).

CI runs both in its usual e2e and merger extension jobs, with the token from the `SQ_TEST_REPO_TOKEN` repository secret.

## What a step must be

The URIs of a step must be something GitHub will land as one stack onto the target, and anything else is refused as an invalid request:

- Every URI names a pull request in the configured repository on the configured host, at the head commit the pull request still has. A head that moved is stale; a closed, unmerged pull request or a draft cannot be landed.
- Pull requests that are already merged must be a prefix of the list, since a stack merges bottom-up; they are skipped.
- One remaining pull request must be based on the target. A stacked pull request listed on its own is based on the one below it, and merging it would land that one too.
- Several remaining pull requests must be the unmerged bottom of one GitHub stack based on the target, in the stack's order and with no gap. Separate pull requests that each target the trunk, a stack listed out of order or with a pull request missing, and pull requests from two stacks are all refused. The merger never creates or edits a stack on the author's behalf.

These checks run at the mergeability check, so SubmitQueue rejects such a request at validation rather than after batching it. `Merge` runs them again, before submitting anything, because a stack can be edited between validation and landing.

## Mergeability check

`CheckMergeability` writes nothing. After the checks above it reads GitHub's `mergeable` verdict for every remaining pull request, re-reading while GitHub reports it as still being computed; a pull request that is not mergeable, or whose `mergeable_state` is `dirty`, is a conflict.

GitHub computes that verdict for each pull request against its own base, so a check sees conflicts within a stack and against the trunk, but not between the steps of one request. Those surface when the merge is attempted.

## Atomicity

Each step is one GitHub merge, so each step lands atomically. Steps land in request order, and `Merge` stops at the first that fails: ordering cannot be guaranteed past a failure, so later steps are not attempted, and the steps before it stay landed. The `FAILED` result lists a `StepResult` per landed step with its outputs, then one for the failed step with its reason.

## Idempotency and redelivery

A redelivered request converges instead of merging twice. Already-merged pull requests are skipped and report their recorded merge commit, so a step that fully landed before a crash is reported without another call. An already-merged pull request is trusted as landed on the target; no further check is made, since one would cost a call per pull request on every redelivery. A submission GitHub already has in flight answers with that request's id (HTTP 409); it is adopted and polled only when it merges the same head with the same method, and is an invalid request otherwise.

## Failure classification

- `merger.ErrInvalidRequest` (terminal): a malformed or foreign URI, an unsupported strategy, a stale head, a closed or draft pull request, a step that is not a stack based on the target, or GitHub refusing the merge as asked (HTTP 400/422, e.g. required checks not satisfied).
- `merger.ErrConflict` (terminal): a pull request that is not mergeable at the check, or a merge GitHub reports as failed. GitHub does not separate a conflict from other merge failures there; its message is the reason.
- `ErrMergePending` (retryable, via this package's `Classifier`): GitHub had not settled a merge, or a pull request's mergeability, within the poll budget. The work is still in flight on GitHub's side.
- Anything else is a plain error. HTTP rejections keep their status code (`platform/http.StatusError`) so `platform/errs/http` can retry a 5xx or 429 and dead-letter a 403 or 404. A 404 is not treated as terminal here: GitHub answers 404 for a repository the token cannot see, and that is a deployment fault, not a property of the request.

## Auth and hosts

The merger adds no credential and no base URL. Its HTTP client's transport roots relative paths at the API (for example `platform/http.NewClient("https://api.github.com")`) and authenticates them, so the wiring decides the scheme — a static token, an App installation token source, or anything an internal deployment injects. The token needs write access to the repository, and the right to bypass branch rules only when `BypassRules` is set.

The stacks and asynchronous merge APIs exist on github.com. GitHub Enterprise Server has not shipped stacked pull requests yet; the host, repository and API base URL are configuration, so an Enterprise Server instance needs no code change once it does.
