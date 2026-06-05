# Build Runner

Pluggable abstraction for triggering builds against an external Build Runner, querying their status, and cancelling them.

See [`doc/rfc/submitqueue/build-runner.md`](../../../doc/rfc/submitqueue/build-runner.md) for the contract and design rationale. See `build_runner.go` for the interface itself.

## Adding a new backend

1. Create `extension/buildrunner/{backend}/` with a `BuildRunner` implementation bound to its runner configuration at construction.
2. Map the `base` and `head` change slices onto the backend's build primitives (apply `base`, apply `head`, validate the result).
3. Map the runner's lifecycle states down to the `BuildStatus` values: `Accepted` (accepted for execution), `Running` (executing), and the terminal `Succeeded` / `Failed` / `Cancelled`.
4. Implement internal reconnect / retry so transient failures surface as plain errors without blocking the caller.

## Backends

- `fake`: local-development backend; every build succeeds unless a head change URI carries a failure marker (see `fake` package doc).
- `githubactions`: proof-of-architecture backend that dispatches a GitHub
  Actions workflow. See [`githubactions/README.md`](githubactions/README.md)
  for the workflow inputs and example orchestrator environment variables.
- `buildkite`: Buildkite-backed backend.
