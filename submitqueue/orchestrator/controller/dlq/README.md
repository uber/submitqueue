# DLQ Reconciliation Controllers

This package contains the controllers that drain each primary pipeline topic's `{topic}_dlq` companion and reconcile the affected request or batch into a terminal failed state. They are wired alongside the primary controllers in `service/submitqueue/orchestrator/server/main.go`.

## Design principles

**The DLQ is the final destination.** A message in `{topic}_dlq` has already failed the primary controller's retry budget (`Retry.MaxAttempts` for a retryable error) or surfaced as non-retryable on the first attempt. There is no further dead-letter beyond this point — every DLQ subscription is configured with `DLQ.Enabled = false`, so a DLQ controller's outcome is either "reconciled" or "the message keeps coming back". A genuinely unprocessable DLQ message (e.g. a malformed payload that no controller can ever decode) must be removed by an operator from the queue manually.

**The implementation is deliberately the simplest and most reliable thing that can work.** Each controller does the same three steps: decode the payload to recover the affected `RequestID` or `BatchID`, fetch the entity, transition it to its terminal failed state with the same optimistic-locking CAS the primary pipeline uses. No business logic, no branching on the original error, no per-stage cleanup. Adding behaviour here trades off the one property the DLQ has to provide — convergence.

**Reconcile only; do not attempt to recover.** A DLQ controller never tries to re-run the failed stage, retry the original action against an external dependency, or repair a partially-completed transition. The original controller already failed; the DLQ controller's only job is to make sure the request and batch state stop saying "in progress" and start saying "failed", so the gateway reports an honest answer and downstream tooling can move on. Recovery, when it is appropriate, is a separate concern handled by an operator or a future reconciliation job — not by this code path.

## Convergence guarantee

DLQ consumers are wired with `errs.AlwaysRetryableProcessor` and a very high `Retry.MaxAttempts` (currently 1000). Together with `DLQ.Enabled = false` on the DLQ subscription itself, this means any non-nil error returned from a DLQ controller — including a plain unclassified infra error — is forced retryable and redelivered rather than silently dropped. The combination is "always retryable + bounded-but-effectively-infinite attempts" and is the property the package relies on for convergence.

The recognised error condition is handled explicitly in `dlq.go`:

- `errs.ErrNotFound` -> logged at warn and treated as success. The request or batch never persisted; there is nothing to reconcile.

Everything else, including `errs.ErrVersionMismatch` on the CAS, is returned plain and, after the always-retryable processor wrap, redelivered until it either succeeds or hits the attempt cap. There is no point in pre-classifying retryability at this layer when the processor forces every non-nil error retryable anyway.

## Request log entries are published to Gateway

When a DLQ controller transitions a request to `RequestStateError`, it publishes the terminal request log to the `log` topic. Gateway consumes that topic and remains the sole writer of the request log and public projections. A publish failure leaves the DLQ delivery unacknowledged so redelivery retries the same idempotent log event.

## Controller mapping

Two controller shapes cover the eleven primary pipeline topics:

| Controller | Topics | Decoded ID | Terminal state |
|---|---|---|---|
| `NewDLQRequestController` | `start`, `validate`, `batch`, `cancel`, `log` | `RequestID` | `RequestStateError` |
| `NewDLQBatchController` | `score`, `speculate`, `build`, `merge`, `conclude` | `BatchID` | `BatchStateFailed` + fan-out to member requests as `RequestStateError` |

`buildsignal` carries a `Build` payload and has its own small dedicated controller. The split exists because the DLQ message payload shape mirrors the primary topic's payload shape (the queue framework preserves bytes verbatim under the `_dlq` topic name), so the decoder is what changes per topic — not the reconciliation step. The package-level `RequestIDDecoder` interface plus `DecodeLandRequestID` / `DecodeCancelRequestID` / `DecodeRequestID` cover the three payload shapes used by request-scoped topics.

## Idempotency and concurrent activity

Reconciliation is safe to run more than once for the same message:

- A request already in `RequestStateError` skips the state-transition CAS but republishes its terminal log so redelivery repairs an earlier publish failure. Requests in a different terminal state are left unchanged.
- A batch already in `BatchStateFailed` still fans out to member requests, because a previous attempt may have transitioned the batch but crashed before completing the fan-out. Batches in a different terminal state are left unchanged.
- Per-request fan-out is itself idempotent via `failRequest`.
- A request in `RequestStateCancelling` is reconciled to `RequestStateError`, not left in place: DLQ means the pipeline failed to converge, so we cannot confirm the cancel completed cleanly. Writing Error is the honest signal.

## See also

- `core/errs/README.md` — the error-processing framework, including `AlwaysRetryableProcessor` and the choice of processor for primary vs. DLQ consumers.
- `core/consumer/README.md` — how the consumer applies the processor to controller errors and decides ack/nack/reject.
- `doc/rfc/submitqueue/workflow.md` — the per-stage primary pipeline that the DLQ companions mirror.
