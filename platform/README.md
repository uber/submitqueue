# Platform

Cross-domain packages shared by SubmitQueue, Stovepipe, and other services in this repository. Nothing under `platform/` may depend on domain folders (`submitqueue/`, `stovepipe/`).

## Packages

- **errs/** — Error classification (`errs` types, classifiers, MySQL/generic helpers).
- **metrics/** — Tally helpers with error-aware tagging via `platform/errs`.
- **consumer/** — Queue consumer framework (`consumer.Controller`, registry, DLQ wiring).
- **http/** — Small HTTP client helpers (e.g. base-URL `RoundTripper`). Go import path: `github.com/uber/submitqueue/platform/http`; package name is `http`. Callers that also import `net/http` should import this package with an alias (for example `phttp "github.com/uber/submitqueue/platform/http"`) and use `phttp.NewClient`.
- **git/** — Git command environment (`exec`) and local bare-repo plumbing (`repo`) shared by change providers and mergers.
- **hook/** — Consumer-side hook dispatch: turn queued hook events into `hook.Hook` calls, and reconcile events that dead-letter.
- **lifecycle/** — `Component` start/stop interface and ordered `Group` composition for runnable subsystems.
- **pipeline/** — Typed engine that assembles a service's queue topology into one lifecycle component.
- **publish/** — Topic-key publish plumbing and the message-ID convention that controls deduplication.
- **conduit/** — Phabricator Conduit client: one generic `Call` that encodes JSON parameters with the API token, and decodes the result envelope into a typed `*Error` or the caller's result. Typed methods stay with each caller.
- **buildkite/**, **githubactions/** — Vendor CI clients over `platform/http`: the REST calls and provider-specific vocabulary (state strings, id encoding) shared by every domain's `BuildRunner` backend. They deliberately define no interface, so they are plumbing rather than extensions — each domain keeps its own `BuildRunner` contract under `{domain}/extension/buildrunner` and adapts these clients to it.
- **base/** — Shared domain entities (`change`, `messagequeue`, and related subpackages). Root package `base` is documentation-only.
- **extension/** — Shared extension interfaces and implementations reused across domains (`counter`, `messagequeue`, and backends such as `mysql`). A package belongs here only if it defines a behavioral interface with its `Config` and `Factory` interface; a vendor client with no interface belongs directly under `platform/`.

Domain-scoped infrastructure and extensions stay under each domain (for example `submitqueue/core/`, `submitqueue/extension/`).

## Calling a vendor API

GitHub and Phabricator callers split the work across the same three layers:

- **Protocol**: `githubapi/` and `conduit/` hold what every caller of the API would otherwise repeat, which is request encoding and the response envelope with its errors. They have no endpoint methods.
- **Endpoints**: each extension keeps the calls it makes, with its own typed requests and results, and takes the authenticated connection as a dependency. That dependency is the smallest type that can carry the API's auth: an `*http.Client` for GitHub, whose token rides in a transport, and a `*conduit.Client` for Phabricator, whose token has to be inside the request body.
- **Wiring**: the service's server reads the credentials, builds each connection once with a timeout, and hands it to the extensions that use it, as `githubClient` and `conduitClient` do in the orchestrator's `profiles.go`.
