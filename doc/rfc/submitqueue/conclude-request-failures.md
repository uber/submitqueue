# Conclude Request Failures

Broaden the existing `conclude` stage so controllers and DLQs can send it structured request failures. Do not add a separate `request-failure` topic.

## Current versus proposed

Current `conclude` consumes a `BatchID`, loads the terminal batch, updates each member request, and publishes its terminal request log.

The proposal keeps that path and adds `RequestFailureID` as a second tagged input:

| | Current | Proposed |
|---|---|---|
| Input | `BatchID` | `BatchID` or `RequestFailureID` |
| Source | Terminal batch | Terminal batch, business rejection, or DLQ |
| Context | Batch state and ID | Structured stage, code, message, and metadata |
| Failure ownership | Controllers and DLQs may terminalize directly | They delegate terminalization and logging to `conclude` |

The finalization algorithm is not new. The change is a broader input contract and durable failure context.

```
terminal batch ────────────────────────────────> conclude { batch_id }

known business rejection ─> RequestFailure ────> conclude { failure_id }

permanent controller error ─> DLQ ─> RequestFailure ─> conclude { failure_id }
```

Both business-decision controllers and DLQs publish to `conclude`. A controller publishes only after reaching a known terminal decision. Execution and infrastructure errors continue to return as errors while recovery remains possible; the DLQ publishes only after they become permanent.

## Contracts

`ConcludeMessage` is an internal protobuf with a tagged `oneof`:

```proto
message ConcludeMessage {
  oneof target {
    string batch_id = 1;
    string request_failure_id = 2;
  }
}
```

This avoids mixing bare `BatchID` and `RequestFailureID` payloads on one topic.

`RequestFailure` is an immutable orchestrator entity containing a stable ID, request ID, stage, kind (`business_rejection` or `controller_failure`), stable code, message, metadata, and creation time. Producers create it idempotently; the queue carries only its ID.

## Controller change

Today a controller such as `mergeconflictsignal` terminalizes a business rejection directly:

```go
if result.Outcome != runwaypb.Outcome_SUCCEEDED {
    return c.failRequest(ctx, request, result.Reason)
}
```

Under this proposal it persists the decision and delegates finalization:

```go
if result.Outcome != runwaypb.Outcome_SUCCEEDED {
    failure := entity.NewRequestFailure(
        request.ID,
        "mergeconflictsignal",
        entity.FailureKindBusinessRejection,
        "mergeability_check_failed",
        result.Reason,
        metadata,
    )
    if err := failureStore.Create(ctx, failure); err != nil {
        return fmt.Errorf("create request failure: %w", err)
    }
    return publishConcludeFailure(ctx, failure.ID, request.Queue)
}
```

The controller no longer changes `Request.State` or publishes a terminal request log.

## DLQ change

Today a request-scoped DLQ changes the request to `RequestStateError` and publishes a log using `dlq.last_error`.

Under this proposal it creates a generic durable failure and publishes it to `conclude`:

```go
failure := entity.NewRequestFailure(
    requestID,
    originalTopic,
    entity.FailureKindControllerFailure,
    "controller_failure",
    delivery.Metadata()["dlq.last_error"],
    nil,
)
if err := failureStore.Create(ctx, failure); err != nil {
    return err
}
return publishConcludeFailure(ctx, failure.ID, partitionKey)
```

A previously persisted business failure wins over a later generic DLQ failure for the same logical transition. A batch-scoped DLQ still marks the batch `BatchStateFailed`, then publishes one failure conclusion per member to preserve the original error context.

## Broadened conclude

`conclude` dispatches on the tagged reference:

```go
switch target := msg.Target.(type) {
case *ConcludeMessage_BatchId:
    return c.concludeBatch(ctx, target.BatchId)
case *ConcludeMessage_RequestFailureId:
    return c.concludeFailure(ctx, target.RequestFailureId)
default:
    return errors.New("conclude message has no target")
}
```

- `concludeBatch` retains the current batch fan-out behavior.
- `concludeFailure` loads the failure and request, conditionally changes the request to `RequestStateError`, and publishes `RequestStatusError` with the structured context.
- Both paths use the same request reconciliation and log-repair helper.
- Gateway remains the sole writer of the request-log database.

## Replay and rollout

- Failure creation and conclusion publication use stable identities.
- If state update succeeds but log publication fails, redelivery republishes the log.
- Version conflicts return errors so redelivery reloads current state.
- A different terminal request outcome is left unchanged.
- `conclude_dlq` replays a referenced failure instead of creating another one.

To preserve in-flight messages, first deploy a decoder that accepts both legacy bare `BatchID` payloads and `ConcludeMessage`. Then migrate batch producers, add failure producers, and remove legacy decoding after the topic retention window.
