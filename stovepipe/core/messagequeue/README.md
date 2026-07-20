# Stovepipe internal message-queue contract

Wire payloads for the queues internal to the Stovepipe pipeline. It is **internal** — used only within the Stovepipe domain — so it lives under `stovepipe/core` rather than `api/` (Bazel visibility keeps it domain-scoped).

Payloads are defined in proto3 (`proto/`, generated into `protopb/`) and serialized as **protobuf JSON** (protojson), so the MySQL-backed queue keeps storing self-describing JSON. The contract package adds only generic glue — `Marshal`/`Unmarshal` and the `TopicKeys` reflection lookup — and owns the `TopicKey` constants for the stages it carries. Each payload declares the topic key(s) that carry it via the `topic_keys` proto option (defined in `api/base/messagequeue`); a contract test round-trips every payload and asserts each topic key is bound to exactly one message.

## Stages

- **process** (`TopicKeyProcess`, `ProcessRequest`) — ingest publishes the minted request id here once it accepts a new head; the process controller reloads the `Request` from storage and decides the build strategy. Only the id travels: producer and consumer share the store, so messages stay small and redelivery is idempotent.
- **build** (`TopicKeyBuild`, `BuildRequest`) — process/analyze publishes the request id here once its build scope (`BuildStrategy`/`BaseURI`) is decided; the build controller reloads the `Request`, triggers the build, and persists the resulting `Build`. Partitioned by request id.
- **buildsignal** (`TopicKeyBuildSignal`, `BuildSignal`) — build publishes the build id here after triggering; buildsignal re-publishes to itself between polls until the build reaches a terminal status. Partitioned by build id, so each build's poll loop is an independent partition. See [doc/rfc/stovepipe/steps/build.md](../../../doc/rfc/stovepipe/steps/build.md) and [buildsignal.md](../../../doc/rfc/stovepipe/steps/buildsignal.md).

See [doc/rfc/messagequeue-contract.md](../../../doc/rfc/messagequeue-contract.md) for the contract conventions and `api/runway/messagequeue` for the external reference example.
