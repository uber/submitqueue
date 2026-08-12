# BuildRunner

Vendor-agnostic interface through which Stovepipe triggers and polls builds against an external build system.

- **Trigger** starts a new build against `headURI`, optionally relative to an incremental `baseURI`, and returns the runner-minted build id. There is no caller-supplied dedup input — every call starts a fresh build; downstream idempotency absorbs any duplicate from a redelivery. Trigger must return promptly; the build itself runs asynchronously.
- **Status** polls the current status and any provider metadata for a build id `Trigger` returned. Unlike `Trigger`, it may round-trip to the backend and block.
- **Cancel** requests cancellation for a build id, returning once the request reaches the runner rather than once the build actually stops. Unused today; kept for contract parity with SubmitQueue.

Implementations return plain, unclassified errors — the calling controller decides retryable-vs-not and user-vs-infra, per `platform/errs`.

Real backends (`buildkite`, `githubactions`) are thin adapters over a shared platform client (`platform/{backend}`) — see that package's README for the HTTP client and vendor-specific details.

`fake` is the stub for local stacks and tests. By default every build succeeds immediately; its `Params` set a failure rate and a build duration — optionally spread by a percentage so durations vary within bounds the configuration states outright — for every build, and a `buildrunner-fake=<token>` marker in a request's head URI pins the outcome or the running window for one build. It keeps no per-build state — the outcome and the instant the build turns terminal are encoded in the build id — so `Status` can be answered by any instance in any process. Never production.

`sampler` is a composite rather than a backend: it wraps two other `BuildRunner`s and sends a configured percentage of builds to the second one, which is how a new backend is rolled out gradually or compared against the incumbent on live traffic. The sample is drawn per `Trigger`, so the percentage is a rate over many builds. Because `Status` and `Cancel` have to reach whichever runner minted a build's opaque id, the sampler tags each id with the runner behind it and strips the tag before delegating; untagged ids route to the baseline. That tagging is what keeps it stateless across redeliveries and replicas, and it lets samplers nest.

See [doc/rfc/stovepipe/steps/build.md](../../../doc/rfc/stovepipe/steps/build.md#why-separate-contracts) for why this is a separate contract from SubmitQueue's own `buildrunner` rather than a shared one. To add a backend, create `buildrunner/{backend}/`, implement `BuildRunner`, and return it from a `New(...)` constructor. Which backend serves which queue — and whether a queue gets a sampler at all — is decided in the wiring layer ([`service/stovepipe/server`](../../../service/stovepipe/server)), not here.
