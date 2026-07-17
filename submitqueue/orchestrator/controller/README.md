# Controller Correctness

SubmitQueue controllers are built around eventual consistency. Controllers advance a workflow through durable state checkpoints, and every component must tolerate retries before the next checkpoint is recorded.

The core model is:

> Load durable state, reconcile it toward this controller's checkpoint, then replay the checkpoint's fanout until it is accepted.

Optimistic locking protects checkpoints from concurrent writers. Failures and races are expected to be uncommon, so the system may leave harmless partial or orphaned data from attempts that never reached a checkpoint. That data can be cleaned up separately if it becomes a problem.

## Checkpoint pattern

Each controller owns a small set of state transitions. It must classify the latest state before writing:

```text
Process(message):
    entity = load latest durable state

    if state is before my checkpoint:
        perform retry-safe preparation
        record checkpoint with optimistic locking
        if the version changed:
            return ErrVersionMismatch

    if state is at my checkpoint:
        replay complete fanout using stable message identities
        return success

    if state is beyond or supersedes my checkpoint:
        return success

    return invalid-state error
```

The important states are:

| State relative to this controller | Behavior |
|---|---|
| Before checkpoint | Perform retry-safe work and record the checkpoint. |
| At checkpoint | Skip the state transition and replay the complete fanout. |
| Beyond checkpoint | A downstream controller already consumed the handoff. Acknowledge without regressing state. |
| Superseded | Cancellation, failure, or another outcome made this work unnecessary. |
| Invalid | Return an error rather than inventing a transition. |

## Retry and redelivery

Prefer one reconciliation pass per delivery. The consumer framework is the retry loop:

```text
controller returns error
    -> error processor classifies it
    -> consumer nacks retryable errors
    -> redelivery re-enters Process and reloads durable state
```

Controllers should not classify ordinary backend failures merely because replay would be convenient. Return the raw wrapped error and let the configured classifiers decide whether it is transient. A permanent publish or storage failure must eventually reach the DLQ rather than retry forever.

## Persist before publishing

For a state transition followed by queue fanout:

```text
persist checkpoint
publish complete fanout
ack delivery
```

The checkpoint proves that the state transition happened. It does not prove that every output was published.

If a process fails after recording the checkpoint, redelivery observes the checkpoint, skips the transition, and republishes the complete fanout. Every replayed output must use the same topic, partition key, logical message ID, and payload.

## Optimistic locking

Optimistic locking answers whether an entity changed since it was read. It does not decide whether a lifecycle transition is valid.

A controller must write only from states it owns. For example, score may transition `Created` to `Scored`; it must not load `Speculating` and write it back to `Scored`.

Version arithmetic follows the [storage optimistic-locking contract](../../extension/storage/README.md): compute the new version in the controller and update the in-memory entity only after the write succeeds.

## Example: score

| Batch state | Behavior |
|---|---|
| `Created` | Compute the score and conditionally record `Scored`. |
| `Scored` | Preserve the durable score and replay logs plus `speculate`. |
| `Speculating`, `Merging` | Acknowledge without regressing the batch. |
| `Cancelling`, terminal | The transition was superseded or another controller owns recovery. |

If publishing fails after the batch reaches `Scored`, the controller returns an error. Redelivery reloads `Scored`, skips rescoring, and republishes the fanout with stable message identities.

## External effects

An external effect whose outcome was not recorded cannot be made safe by queue deduplication alone:

```text
provider accepts operation
controller fails before recording the result
```

Such effects require a provider-supported idempotency key, a stable operation identity that can be queried, or an explicit acceptance that duplicate or orphaned work is harmless.

## Review checklist

For each controller, make these answers clear:

1. What durable checkpoint does it own?
2. Is all work before that checkpoint safe to retry?
3. How does each possible durable state classify relative to the checkpoint?
4. Can the complete fanout be reconstructed and replayed with stable message identities?
5. Which controller or DLQ path owns superseded and terminal recovery?

See the [consumer error contract](../../../platform/consumer/README.md), the [orchestrator workflow](../../../doc/rfc/submitqueue/workflow.md), and the [SQL queue RFC](../../../doc/rfc/sql-queue-rfc.md).
