# GitHub Actions client

Shared HTTP client and GitHub Actions-specific facts for every domain's GitHub Actions-backed `BuildRunner`. There is no `BuildRunner` interface here by design — each domain (`submitqueue`, `stovepipe`, ...) defines its own `BuildRunner` and its own `BuildStatus`, and adapts this package's `RunStatus` to it. Mirrors [`platform/extension/buildrunner/buildkite`](../buildkite/README.md)'s split; see that package's README and [`doc/rfc/stovepipe/steps/build.md`](../../../../doc/rfc/stovepipe/steps/build.md#alternatives-considered-for-sharing-the-contract) for the shared rationale.

## What lives here

- `Client`: `DispatchWorkflow` / `GetRun` / `CancelRun` against the GitHub Actions REST API, bound to a single repository and workflow. Construct one with `NewClient(httpClient, owner, repo, workflowID)`, where `httpClient` already has the GitHub API root (via `platform/http.BaseURLTransport`, typically `https://api.github.com`) and a token with `actions:read`/`actions:write` configured.
- `RunStatus` / `ParseRunStatus`: GitHub's own run status/conclusion vocabulary (`queued`/`in_progress`/`completed` crossed with a conclusion), collapsed into the five states every domain's `BuildStatus` already distinguishes. Each domain still does its own trivial `RunStatus` → its own `BuildStatus` switch — this package does not know either domain's entity types.
- `EncodeRunID` / `ParseRunID`: the run-id-as-build-id convention, mirroring Buildkite's build-number encoding.
- `RunMetadata`: builds the caller-facing metadata map (run id, attempt, status, conclusion, title, URL, branch, created-at) from a `WorkflowRun`.

## Who consumes it

- `submitqueue/extension/buildrunner/githubactions` — batch-identity `BuildRunner`, resolves changes via `changeset.Resolver` before dispatching.
- `stovepipe/extension/buildrunner/githubactions` — URI-identity `BuildRunner`, dispatches directly from `headURI`/`baseURI`.

Both wrap a `*Client` built at the wiring layer; this package never constructs one from raw config (no credentials, no repo/workflow) itself.

## How the same `Client` stays safe to share across two different checkout strategies

As with the Buildkite client, `Client.DispatchWorkflow` never inspects `DispatchWorkflowRequest.Inputs` or picks a strategy — the split happens entirely outside this package:

1. **Each domain's own adapter shapes a different `Inputs` payload.** SubmitQueue's runner sends `sq_base_uris`/`sq_head_uris` as JSON-encoded arrays of resolved change URIs; stovepipe's sends `stovepipe_head_uri`/`stovepipe_base_uri` as single plain strings. Both just call `Client.DispatchWorkflow` with their own `Inputs` map — this package treats it as an opaque `map[string]string`.
2. **Each domain's `Client` targets a different repository/workflow**, bound once at wiring time via `NewClient`'s `owner`/`repo`/`workflowID` arguments. Each workflow file (owned by the target repository, outside this package) is written against its domain's input contract — one applies patches into composite commits, the other checks out `stovepipe_head_uri` and diffs against `stovepipe_base_uri`.

So the checkout strategy is a static, wiring-time binding — one `Client` instance ↔ one repo/workflow ↔ one workflow definition ↔ one input contract ↔ one domain's adapter — never a runtime decision made anywhere in Go code.
