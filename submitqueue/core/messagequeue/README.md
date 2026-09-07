# SubmitQueue internal message-queue contract

Wire payloads for the queues internal to the SubmitQueue pipeline (gateway and orchestrator). It is **internal** — used only within the SubmitQueue domain — so it lives under `submitqueue/core` rather than `api/` (Bazel visibility keeps it domain-scoped).

Payloads are defined in proto3 (`proto/`, generated into `protopb/`) and serialized as **protobuf JSON** (protojson), so the MySQL-backed queue keeps storing self-describing JSON. The contract package adds protojson glue (`Marshal`/`Unmarshal`), `TopicKeys`, the pipeline `TopicKey` constants, and helpers that map generated payloads to `submitqueue/entity` types. `MarshalID`/`UnmarshalID` take the topic key so the bound message is used; unmarshalling through a different type would drop fields added later (`DiscardUnknown`). Each payload declares the topic key that carries it via the `topic_keys` proto option (defined in `api/base/messagequeue`); a contract test round-trips every payload and asserts each topic key is bound to exactly one message.

Shared field types `Change` and `Strategy` come from `api/base/change` and `api/base/mergestrategy`.

## Stages

Each topic key has its own message, even when the first version is only an id and a queue, so a stage can grow fields without touching others.

- **start** (`TopicKeyStart`, `Start`) — gateway publishes the minted request id and land inputs; start persists a `Request`. Full payload: this seam crosses services.
- **cancel** (`TopicKeyCancel`, `Cancel`) — gateway publishes the request id to cancel; cancel reloads the `Request`. Full payload across the gateway/orchestrator seam.
- **validate** (`TopicKeyValidate`, `Validate`) — start publishes the request id; validate reloads the `Request`.
- **batch** (`TopicKeyBatch`, `Batch`) — landconflictsignal publishes the request id; batch reloads the `Request`.
- **dependency-analysis** (`TopicKeyDependencyAnalysis`, `DependencyAnalysis`) — batch publishes the batch id; partitioned by queue.
- **speculate** (`TopicKeySpeculate`, `Speculate`) — dependency-analysis (and later stages) publish the batch id.
- **build** (`TopicKeyBuild`, `Build` in `submitqueuebuild.proto`) — speculate publishes a **batch** id; build reloads the `Batch`. The proto filename is not `build.proto` so it does not collide with Stovepipe's `stovepipe/core/messagequeue/proto/build.proto` in the protobuf filename registry.
- **buildsignal** (`TopicKeyBuildSignal`, `BuildSignal` in `submitqueuebuildsignal.proto`) — build publishes a **build** id; buildsignal polls and may hold the delivery. Same filename-registry reason as build.
- **submitqueue-land** (`TopicKeyLand`, `Merge` in `submitqueuemerge.proto`) — speculate publishes a batch id; land reloads the `Batch` before handing work to Runway. The proto filename is not `merge.proto`/`land.proto` so it does not collide with Runway's `api/runway/messagequeue/proto/merge.proto` in the protobuf filename registry.
- **conclude** (`TopicKeyConclude`, `Conclude`) — speculate/landsignal publish a batch id. A failed batch's reason travels in message metadata (`MetadataKeyFailureReason`), not the payload.
- **log** (`TopicKeyLog`, `Log`) — orchestrator publishes a full request-log entry; the gateway materializes it. `type`, `status`, and `event` are open strings matching the domain vocabularies.

In-boundary stages (validate through conclude, except start/cancel/log) put only an id on the queue because producer and consumer share storage.
