# github validator

A `validator.Validator` that rejects a request whose GitHub pull requests the repository would not let merge yet. It asks GitHub rather than restating policy: the rules come from the repository's branch protection or rulesets, so a repository changes what SubmitQueue enforces by changing its own settings. It is constructed by the wiring layer with an HTTP client and the GitHub host the client talks to; the example orchestrator selects it per queue with `validator: {type: github}` in its extension profiles (see [`service/submitqueue/orchestrator/server`](../../../../service/submitqueue/orchestrator/server)).

## Rules

For each `github://` URI in the request, bottom of the stack first, the validator reads the pull request from GitHub's GraphQL API and rejects at the first rule it breaks:

| Rule | Rejection reason |
|---|---|
| The URI names the configured host | `host` |
| The pull request's head is still the commit the URI names | `stale` |
| GitHub's review decision is not `CHANGES_REQUESTED` or `REVIEW_REQUIRED` | `not_approved` |
| Every required check on the head commit passed; success, neutral and skipped count as passing | `check_failed`, `check_pending` |
| GitHub's merge state is not `BLOCKED` | `blocked` |

A check is required when GitHub reports it as required for that pull request, which covers both branch protection and rulesets. One check name can report more than once, for example from two workflows; every report of it must pass, so a failing one names the check rather than surfacing only as `blocked`.

The `blocked` rule comes last because it explains nothing. It catches what the rules above cannot see: a required check that has not reported yet, a required conversation resolution, required signatures, or a required check past the first 100 the validator reads.

Whatever GitHub has not decided passes. A merge state GitHub is still computing (`UNKNOWN`) or a repository with no review rule rejects nothing. GitHub enforces the same rules again when the pull request is merged, so a miss here fails later at the merge rather than landing.

## What it leaves to other stages

- **Open, draft, conflicting, stack shape, target branch**: the merger's mergeability check rejects these before batching, and runs again before merging.
- **The pull request exists and its head has not moved**: the change provider fails the request before validators run. The validator still compares heads, because its own verdict must be about the commit the URI names.

Each pull request is judged by GitHub's verdict for that pull request, which applies the rules of its own base branch. In a stack, that base is the branch below it rather than the trunk.

## Composition

URIs that are not GitHub change IDs are skipped, so the validator composes with validators for other change providers (see [`composite`](../composite)).

## Failures

A rejection is an error naming the pull request and the rule it breaks, wrapping `validator.ErrRejected`; the validate stage terminates the request with it as the reason. A failure to read GitHub — a transport error, an HTTP status, a GraphQL error such as a pull request that does not exist — does not wrap it, so the validate stage hands it to the consumer's classifiers like any other stage failure: an HTTP status stays in the chain as `platform/http.StatusError`, so `platform/errs/http` retries a 5xx or 429 and dead-letters a 403 or 404.

## Auth and hosts

The validator adds no credential and no base URL. Its HTTP client's transport roots relative paths at the API (for example `platform/http.NewClient("https://api.github.com")`) and authenticates them. Reading a public repository needs only an authenticated request; a private one needs read access to pull requests, checks and commit statuses. GitHub Enterprise Server works by pointing the client at its API and setting the host.

## Metrics

Under the `github_validator` scope, tagged with the `queue`: `validate.start` and the `validate.finish` latency histogram, whose `result` tag is `error` only for a failure to read GitHub, and `validate.rejections` tagged with the rejection `reason`.
