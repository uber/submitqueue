# Buildkite client

Shared HTTP client and Buildkite-specific facts for every domain's Buildkite-backed `BuildRunner`. There is no `BuildRunner` interface here by design — each domain (`submitqueue`, `stovepipe`, ...) defines its own `BuildRunner` and its own `BuildStatus`, and adapts this package's `State` to it. See [`doc/rfc/stovepipe/steps/build.md`](../../../../doc/rfc/stovepipe/steps/build.md#alternatives-considered-for-sharing-the-contract) for why the contract stays per-domain while the backend is shared.

## What lives here

- `Client`: `CreateBuild` / `GetBuild` / `CancelBuild` against the Buildkite REST API, plus `EncodeBuildNumber` / `ParseBuildNumber` for the build-number-as-id convention. Construct one with `NewClient(httpClient)`, where `httpClient` already has the pipeline's base URL (via `platform/http.BaseURLTransport`) and auth configured.
- `State` / `ParseState`: Buildkite's own build-state vocabulary (`creating`, `scheduled`, `running`, `blocked`, `passed`, `failed`, `canceling`, `canceled`, `skipped`, `not_run`), collapsed into the five states every domain's `BuildStatus` already distinguishes. Each domain still does its own trivial `State` → its own `BuildStatus` switch — this package does not know either domain's entity types.
- `DecodeMetadataEnv`: recovers a JSON-encoded `map[string]string` from a build's echoed env vars, given the env key the caller used at trigger time.

## Who consumes it

- `submitqueue/extension/buildrunner/buildkite` — batch-identity `BuildRunner`, resolves changes via `changeset.Resolver` before triggering.
- `stovepipe/extension/buildrunner/buildkite` — URI-identity `BuildRunner`, triggers directly from `headURI`/`baseURI`.

Both wrap a `*Client` built at the wiring layer; this package never constructs one from raw config (no credentials, no pipeline slug) itself.

## How the same `Client` stays safe to share across two different checkout strategies

SubmitQueue's build materializes state that doesn't exist yet — the runner resolves a batch DAG into composite base/head commits by applying patches. Stovepipe's build checks out a commit that already exists on trunk and, for an incremental build, diffs it against a baseline. These are different problems at the CI-pipeline level (see [build.md's "Why separate contracts"](../../../../doc/rfc/stovepipe/steps/build.md#why-separate-contracts)), yet both go through the same `Client.CreateBuild` call — the `Client` never inspects `CreateBuildRequest.Env` or picks a strategy, so there is nothing here that needs to "know" which pattern applies.

The split happens entirely outside this package, at two layers below it:

1. **Each domain's own adapter shapes a different `Env` payload.** SubmitQueue's runner sends `SQ_BASE_URIS`/`SQ_HEAD_URIS` as JSON-encoded arrays of resolved change URIs; stovepipe's sends `STOVEPIPE_HEAD_URI`/`STOVEPIPE_BASE_URI` as single plain strings. Both just call `Client.CreateBuild` with their own `Env` map — this package treats it as an opaque `map[string]string`.
2. **Each domain's `Client` targets a different Buildkite pipeline**, bound once at wiring time via the `httpClient`'s `BaseURLTransport.BaseURL` (e.g. `.../pipelines/submitqueue-go-code` vs `.../pipelines/stovepipe-go-code`). Each pipeline has its own Buildkite pipeline script (owned by Buildkite config, outside this repo) written against its domain's env-var contract — one applies patches into composite commits, the other checks out `STOVEPIPE_HEAD_URI` and diffs against `STOVEPIPE_BASE_URI`.

So the checkout strategy is a static, wiring-time binding — one `Client` instance ↔ one pipeline ↔ one pipeline script ↔ one env-var contract ↔ one domain's adapter — never a runtime decision made anywhere in Go code.
