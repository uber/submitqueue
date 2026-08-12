# Platform

Cross-domain packages shared by SubmitQueue, Stovepipe, and other services in this repository. Nothing under `platform/` may depend on domain folders (`submitqueue/`, `stovepipe/`).

## Packages

- **errs/** — Error classification (`errs` types, classifiers, MySQL/generic helpers).
- **metrics/** — Tally helpers with error-aware tagging via `platform/errs`.
- **consumer/** — Queue consumer framework (`consumer.Controller`, registry, DLQ wiring).
- **http/** — Small HTTP client helpers (e.g. base-URL `RoundTripper`). Go import path: `github.com/uber/submitqueue/platform/http`; package name is `http`. Callers that also import `net/http` should import this package with an alias (for example `phttp "github.com/uber/submitqueue/platform/http"`) and use `phttp.NewClient`.
- **buildkite/**, **githubactions/** — Vendor CI clients over `platform/http`: the REST calls and provider-specific vocabulary (state strings, id encoding) shared by every domain's `BuildRunner` backend. They deliberately define no interface, so they are plumbing rather than extensions — each domain keeps its own `BuildRunner` contract under `{domain}/extension/buildrunner` and adapts these clients to it.
- **base/** — Shared domain entities (`change`, `messagequeue`, and related subpackages). Root package `base` is documentation-only.
- **extension/** — Shared extension interfaces and implementations reused across domains (`counter`, `messagequeue`, and backends such as `mysql`). A package belongs here only if it defines a behavioral interface with its `Config` and `Factory` interface; a vendor client with no interface belongs directly under `platform/`.

Domain-scoped infrastructure and extensions stay under each domain (for example `submitqueue/core/`, `submitqueue/extension/`).
