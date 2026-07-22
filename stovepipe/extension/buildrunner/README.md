# BuildRunner

Vendor-agnostic interface through which Stovepipe triggers and polls builds against an external build system.

- **Trigger** starts a new build against `headURI`, optionally relative to an incremental `baseURI`, and returns the runner-minted build id. There is no caller-supplied dedup input — every call starts a fresh build; downstream idempotency absorbs any duplicate from a redelivery. Trigger must return promptly; the build itself runs asynchronously.
- **Status** polls the current status and any provider metadata for a build id `Trigger` returned. Unlike `Trigger`, it may round-trip to the backend and block.
- **Cancel** requests cancellation for a build id, returning once the request reaches the runner rather than once the build actually stops. Unused today; kept for contract parity with SubmitQueue.

Implementations return plain, unclassified errors — the calling controller decides retryable-vs-not and user-vs-infra, per `platform/errs`.

See [doc/rfc/stovepipe/steps/build.md](../../../doc/rfc/stovepipe/steps/build.md#why-separate-contracts) for why this is a separate contract from SubmitQueue's own `buildrunner` rather than a shared one. To add a backend, create `buildrunner/{backend}/`, implement `BuildRunner`, and return it from a `New(...)` constructor.
