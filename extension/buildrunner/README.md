# Build Runner

Pluggable abstraction for triggering builds against an external Build Runner, querying their status, and cancelling them.

See [`doc/rfc/build-runner.md`](../../doc/rfc/build-runner.md) for the contract and design rationale. See `build_runner.go` for the interface itself.

## Adding a new backend

1. Create `extension/buildrunner/{backend}/` with a `Factory` whose `New` returns a `BuildRunner` bound to one queue's job configuration. The runner verbs carry no queue selector — that selection lives in the `Config` passed to the factory.
2. Map the `base` and `head` change slices onto the backend's build primitives (apply `base`, apply `head`, validate the result).
3. Map the runner's lifecycle states down to the `BuildStatus` values: `Accepted` (accepted for execution), `Running` (executing), and the terminal `Succeeded` / `Failed` / `Cancelled`.
4. Implement internal reconnect / retry so transient failures surface as plain errors without blocking the caller.
