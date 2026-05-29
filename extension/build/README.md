# Build Manager

Pluggable abstraction for triggering builds against an external provider
(BuildKite, Jenkins, an internal job runner, etc.), querying their status,
and cancelling them.

## Interface

`BuildManager` exposes `Trigger`, `Status`, `Cancel`, and `Close`.
Implementations are long-lived singletons bound to their provider
configuration at construction; the interface itself stays vendor-agnostic.

### Lifecycle and state

Implementations are long-lived singletons. They must make every method safe
for concurrent use and should not serialize calls beyond what the provider's
rate limits require. They may hold transient local state (connection pools,
caches), but anything that must survive a restart belongs in `Storage`.

### Async vs sync contract

- `Trigger` returns promptly; provider-side work is asynchronous. MAY return
  a terminal status when the input maps to an already-finished build;
  otherwise returns `BuildStatusAccepted`.
- `Status` MAY be synchronous and lengthy — a provider round trip is typical.
- `Cancel` returns once the request reaches the provider; it does not wait
  for the build to stop. No-op on already-terminal builds.
- `Close` is idempotent; subsequent calls on other methods return errors.

### Transient errors

Implementations recover from transient connectivity failures (network blips,
provider 5xx) internally — reconnect, retry-with-backoff, etc. During the
recovery window methods return plain errors and never block the caller
indefinitely.

### Errors

Methods return plain errors. Per the `core/errs` convention, the calling
controller decides classification (user vs infra, retryable vs not); the
implementation should wrap with `errs.NewRetryableError` only when it has
specific knowledge that a failure is transient and safely retryable. Domain
sentinels (e.g. a "build not found" error) will be introduced alongside the
first implementation that needs them.

## Adding a new backend

1. Create `extension/build/{backend}/` with a `BuildManager` implementation
   bound to its provider configuration at construction.
2. Map each `entity.BuildChange` (with its `ChangeAction`) onto the
   backend's build primitives.
3. Map the provider's lifecycle states down to the `BuildStatus` values:
   `Accepted` (accepted/queued, not started), `Running` (executing), and the
   terminal `Succeeded` / `Failed` / `Cancelled`. Provider-initiated
   cancellations (timeout, resource limits) map to `Failed`; only
   caller-initiated cancellations map to `Cancelled`.
4. Implement internal reconnect / retry so transient failures surface as
   plain errors without blocking the caller.
