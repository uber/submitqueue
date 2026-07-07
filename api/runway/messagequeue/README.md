# Runway message queue contract

The published, language-neutral contract for the merge queues Runway owns. A client — in any language — publishes a merge request and consumes the result without access to Runway's Go types or storage. See [the message queue contract RFC](../../../doc/rfc/messagequeue-contract.md) for the design.

Payloads are defined as proto3 messages in [`proto/merge.proto`](proto/merge.proto) and generated into [`protopb/`](protopb); the proto is the authority and a non-Go client compiles against it directly. On the wire, payloads are serialized as protobuf JSON (`protojson`), so the queue keeps storing self-describing JSON. The message types are generated, so the Go helpers in this package are just generic `protojson` glue — `Marshal(m)` and `Unmarshal[T](b, m)` — for Go callers; field names stay snake_case (`UseProtoNames`) and enums serialize as their UPPER_SNAKE value name.

The shared field types `Change` and `MergeStrategy` come from `api/base/change` and `api/base/mergestrategy`, imported by the contract.

## Merge strategy

Each `MergeStep` carries a `Strategy` (from `api/base/mergestrategy`) naming how that step is integrated into the merge target. `REBASE`, `SQUASH_REBASE`, and `MERGE` *transform* the change onto the branch tip and produce new revisions. `PROMOTE` is different: it integrates the exact revision **as-is**, advancing the merge target to an already-existing commit with no content transform and no new revision. Each backend realizes `PROMOTE` natively — git fast-forward, Mercurial bookmark advance, Subversion/Perforce copy — so for `PROMOTE` a step's `StepOutput.id` is the same revision the request named rather than a freshly created one. It is the strategy a post-merge verifier (e.g. Stovepipe) uses to advance a verified branch like `verified/main` to an already-landed, already-verified commit. `DEFAULT` lets the server pick per queue configuration.

## Topic keys

The binding between a topic key and its payload lives in each message's `topic_keys` option (defined in `api/base/messagequeue`); `TopicKeys` reads it back by reflection. A topic key is a stable logical name, not a concrete wire topic — each implementer maps the key to whatever topic name its broker/queue requires. Our Go wiring maps it via `consumer.TopicRegistry`.

| Message | Direction | Topic keys |
|---|---|---|
| `MergeRequest` | client → Runway | `merge-conflict-check`, `merge` |
| `MergeResult` | Runway → client | `merge-conflict-check-signal`, `merge-signal` |

One message serves a queue pair because a merge-conflict check is a dry run of a merge: Runway applies the same ordered steps onto the same merge target, and the topic key the request arrives on decides whether it commits the result and reports the produced revisions. A request on `merge-conflict-check` is a dry run; a request on `merge` commits.

## Result shape

`MergeResult.outcome` is an `Outcome` enum (`OUTCOME_UNSPECIFIED`/`SUCCEEDED`/`FAILED`): `SUCCEEDED` means mergeable (check) or merged (commit), `FAILED` a conflict or a failed apply; `reason` carries the explanation when `FAILED`. Per-step detail is in `steps` (request order): each `StepResult.outputs` is the list of `StepOutput`s the step produced on the merge target, **in application order** (the order they were created). A committing merge populates `outputs`; a dry-run check, an already-present change, or a failed step leaves them empty. `StepOutput.id` is the VCS-neutral revision identifier (git SHA, Mercurial hash, Subversion revision, Perforce changelist, …), with room to grow (author, timestamp, …).

## Evolution

Contract changes are additive-only: add new fields; never remove, rename, repurpose, or retype an existing field, and never reuse a field number. protojson ignores unknown fields on read and omits zero-valued fields on write, so a new optional field is backward-compatible in both directions.
