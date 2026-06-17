# Platform

Cross-domain packages shared by SubmitQueue, Stovepipe, and other services in this repository. Nothing under `platform/` may depend on domain folders (`submitqueue/`, `stovepipe/`).

## Packages

- **errs/** — Error classification (`errs` types, classifiers, MySQL/generic helpers).
- **metrics/** — Tally helpers with error-aware tagging via `platform/errs`.
- **consumer/** — Queue consumer framework (`consumer.Controller`, registry, DLQ wiring).
- **http/** — Small HTTP client helpers (e.g. base-URL `RoundTripper`). Go import path: `github.com/uber/submitqueue/platform/http`; package name is `http`. Callers that also import `net/http` should import this package with an alias (for example `phttp "github.com/uber/submitqueue/platform/http"`) and use `phttp.NewClient`.
- **base/** — Shared domain entities (`change`, `messagequeue`, and related subpackages). Root package `base` is documentation-only.
- **extension/** — Shared extension interfaces and implementations reused across domains (`counter`, `messagequeue`, and backends such as `mysql`).

Domain-scoped infrastructure and extensions stay under each domain (for example `submitqueue/core/`, `submitqueue/extension/`).
