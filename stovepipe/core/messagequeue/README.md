# Stovepipe internal message-queue contract

Wire payloads for the queues internal to the Stovepipe pipeline. It is **internal** — used only within the Stovepipe domain — so it lives under `stovepipe/core` rather than `api/` (Bazel visibility keeps it domain-scoped).

Payloads are defined in proto3 (`proto/`, generated into `protopb/`) and serialized as **protobuf JSON** (protojson), so the MySQL-backed queue keeps storing self-describing JSON. The contract package adds only generic glue — `Marshal`/`Unmarshal` and the `TopicKeys` reflection lookup — and owns the `TopicKey` constants for the stages it carries. Each payload declares the topic key(s) that carry it via the `topic_keys` proto option (defined in `api/base/messagequeue`); a contract test round-trips every payload and asserts each topic key is bound to exactly one message.

## Stages

- **process** (`TopicKeyProcess`, `ProcessRequest`) — ingest publishes the minted request id here once it accepts a new head; the process controller reloads the `Request` from storage and decides the build strategy. Only the id travels: producer and consumer share the store, so messages stay small and redelivery is idempotent.
- **build** (`TopicKeyBuild`, `BuildRequest`) — process publishes the request id here once its build scope (`BuildStrategy`/`BaseURI`) is decided; the build controller reloads the `Request`, triggers the build, and persists the resulting `Build`. Partitioned by request id.
- **buildsignal** (`TopicKeyBuildSignal`, `BuildSignal`) — build publishes the build id here after triggering; buildsignal holds the delivery between polls until the build reaches a terminal status. Partitioned by build id, so each build's poll loop is an independent partition.
- **buildanalysis** (`TopicKeyBuildAnalysis`, `BuildAnalysis`) — identifies a persisted build for issue detection, partitioned by build id. Build publishes a start notice and buildsignal publishes a completion notice with a distinct message id. See [build analysis](../../controller/buildanalysis/README.md) for polling and persistence behavior.
- **record** (`TopicKeyRecord`, `Record`) — buildsignal publishes the request id after persisting the terminal outcome; record reloads the request and records its validation fact. Partitioned by request id.

## Issue tracking handoff

`IssueTrack` binds to `TopicKeyIssueTrack` and identifies a persisted build and its queue. Notifications are partitioned by queue so both running and final issue-state updates share one serialized path, independently of build-partitioned analysis polling. The message carries identifiers rather than a failure collection; per-build observations are read through `BuildIssueStore.List`.

Publish only after detection and observation persistence succeed. With `finalize` unset, the notification permits opening newly detected issues, not resolution by absence. With `finalize` set, it requests final reconciliation after a successful terminal analysis. Publish that notification even when no failures were detected; terminal Build status alone does not replace the handoff. Publication retries must also follow duplicate observation writes so a crash between persistence and publication does not strand work.

Per-build observations accumulate historical evidence; finalization does not turn them into a replacement snapshot or override the detector's absence semantics. The reconciler must establish the authoritative final failure set and applicable validation scope before resolving issues, reject older requests, and ignore running notifications after finalization. Notification identity must distinguish new observations from finalization while remaining stable on publication retries. Business behavior follows the payload, not a parsed message ID.

Only the handoff contract is defined. No publisher, consumer, topic registration, reconciliation progress, or writes to open issues and lifecycle history are enabled.
