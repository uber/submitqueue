# BuildRunner

Vendor-agnostic interface through which Stovepipe triggers and polls builds against an external build system. Shaped the same as SubmitQueue's own `buildrunner` extension — same `Trigger`/`Status`/`Cancel` verbs, same async contract, same id model — but a separate interface rather than a shared one: Stovepipe validates a single commit against a baseline (or from scratch), not a stack of dependency batches, so `Trigger` takes URI identity instead of batch identity. See [doc/rfc/stovepipe/steps/build.md](../../../doc/rfc/stovepipe/steps/build.md#why-separate-contracts) for the full rationale, and its ["Carries over vs. new"](../../../doc/rfc/stovepipe/steps/build.md#carries-over-vs-new) section for what is shaped the same as SubmitQueue's versus what is Stovepipe-specific.

Per the repository's extension rules, this package holds the `BuildRunner` interface, its `Config`, and the `Factory` *interface* only — concrete `Factory` implementations and the per-queue routing that picks a backend for a `Config.QueueName` live in the wiring layer.

## Behavior

- **Trigger** starts a new build against `headURI` (optionally relative to an incremental `baseURI`) and returns the runner-minted build id. There is no caller-supplied dedup input — every call starts a fresh build, and downstream idempotency absorbs any duplicate from a redelivery. Trigger must return promptly; the build itself runs asynchronously.
- **Status** polls the current status and any provider metadata for a build id `Trigger` returned. Unlike `Trigger`, it may round-trip to the backend and block.
- **Cancel** requests cancellation for a build id, returning once the request reaches the runner rather than once the build actually stops. No controller calls it today — it exists for contract parity with SubmitQueue and for future use.

## Errors

Implementations return plain, unclassified errors — the calling controller decides user-vs-infra and retryable-vs-not, per `platform/errs`. There is no package error sentinel yet; a domain sentinel (e.g. for "unknown build") is deferred until a concrete need for it lands.

## Implementations

- **fake** — a stateless backend that succeeds by default and honors failure-injection markers embedded in `headURI`, for examples and tests.

To add a backend, create `buildrunner/{backend}/`, implement the `BuildRunner` interface, and return it from a `New(...)` constructor.
