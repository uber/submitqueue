# Stovepipe

Stovepipe is a post-merge validation service. Its layout:

- `controller/` — business logic (transport-agnostic). Exposes the `Ping` and `Ingest` RPCs, and consumes the internal pipeline stages (`process`, `build`, `buildsignal`, `record`) plus a DLQ reconciler.

The wire contract lives under `api/stovepipe/` (`proto/` for the `.proto` source, `protopb/` for the committed generated stubs). The internal queue contract and topic keys live under `stovepipe/core/messagequeue/`. Storage, source-control, and build-runner extensions live under `stovepipe/extension/`.
