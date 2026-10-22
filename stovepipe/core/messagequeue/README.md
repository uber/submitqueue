# Stovepipe internal message-queue contract

Wire payloads for the queues internal to the Stovepipe pipeline. It is **internal** — used only within the Stovepipe domain — so it lives under `stovepipe/core` rather than `api/` (Bazel visibility keeps it domain-scoped).

Payloads are defined in proto3 (`proto/`, generated into `protopb/`) and serialized as **protobuf JSON** (protojson), so the MySQL-backed queue keeps storing self-describing JSON. The contract package adds only generic glue — `Marshal`/`Unmarshal` and the `TopicKeys` reflection lookup — and owns the `TopicKey` constants for the stages it carries. Each payload declares the topic key(s) that carry it via the `topic_keys` proto option (defined in `api/base/messagequeue`); a contract test round-trips every payload and asserts each topic key is bound to exactly one message.

## Stages

- **process** (`TopicKeyProcess`, `ProcessRequest`) — ingest publishes the minted request id here once it accepts a new head; the process controller reloads the `Request` from storage and decides the build strategy. Only the id travels: producer and consumer share the store, so messages stay small and redelivery is idempotent.
- **build** (`TopicKeyBuild`, `BuildRequest`) — process publishes the request id here once its build scope (`BuildStrategy`/`BaseURI`) is decided; the build controller reloads the `Request`, triggers the build, and persists the resulting `Build`. Partitioned by request id.
- **buildsignal** (`TopicKeyBuildSignal`, `BuildSignal`) — build publishes the build id here after triggering; buildsignal holds the delivery between polls until the build reaches a terminal status. Partitioned by build id, so each build's poll loop is an independent partition.
- **buildanalysis** (`TopicKeyBuildAnalysis`, `BuildAnalysis`) — identifies a persisted build for issue detection, partitioned by build id. Build publishes a start notice and buildsignal publishes a completion notice with a distinct message id. See [build analysis](../../controller/buildanalysis/README.md) for polling and persistence behavior.
- **record** (`TopicKeyRecord`, `Record`) — buildsignal publishes the request id after persisting the terminal outcome; record reloads the request and records its validation fact. Partitioned by request id.

## Repair stage contract

**fix** (`TopicKeyFix`, [`FixRequest`](proto/fix.proto)) identifies a persisted observation and issue opening using queue, detecting build, task, unit, and first-seen build. Partition by detecting build ID. The `consumer.Controller` selects the first eligible failed unit by creating a Fix keyed by the detecting BuildID, persists a submitting marker, and calls `RepairExecutor.Submit` once. Later deliveries reconcile the selected submission and its request history.

Persist observations and their current opening before publishing a request. Replay publication after duplicate observation writes so a crash between persistence and handoff can recover. Redelivery can complete preparation, but a durable submitting marker suppresses any further outbound call. Execution monitoring and verified recovery are follow-up work.
