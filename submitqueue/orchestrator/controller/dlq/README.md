# DLQ Reconciliation Controllers

This package contains the controllers that drain each primary pipeline topic's `{topic}_dlq` companion and reconcile the affected request or batch. Most stages fail unfinished work; conclude preserves the authoritative terminal batch outcome and completes its request fan-out. They are wired alongside the primary controllers in `service/submitqueue/orchestrator/server/main.go`.

## Design principles

**The DLQ is the final destination.** A message in `{topic}_dlq` has already failed the primary controller's retry budget (`Retry.MaxAttempts` for a retryable error) or surfaced as non-retryable on the first attempt. There is no further dead-letter beyond this point — every DLQ subscription is configured with `DLQ.Enabled = false`, so a DLQ controller's outcome is either "reconciled" or "the message keeps coming back". A genuinely unprocessable DLQ message (e.g. a malformed payload that no controller can ever decode) must be removed by an operator from the queue manually.

**The implementation is deliberately the simplest and most reliable thing that can work.** Most controllers follow three steps: decode the payload to recover the affected `RequestID` or `BatchID`, fetch the entity, transition it to its terminal failed state with the same optimistic-locking CAS the primary pipeline uses. Conclude instead requires a terminal batch and reuses primary conclusion’s outcome mapping; a failed conclusion is not evidence of a failed merge. Adding behaviour here trades off the one property the DLQ has to provide — convergence.

**Reconcile only; do not attempt to recover.** A DLQ controller never tries to re-run the failed stage, retry the original action against an external dependency, or repeat an external action. Conclude does repair terminal membership and partial request/log fan-out; this is completion of the stored outcome, not recovery of the merge. The original controller already failed; reconciliation makes the request and batch state reflect a justified terminal outcome, so the gateway reports an honest answer and downstream tooling can move on. Recovery, when it is appropriate, is a separate concern handled by an operator or a future reconciliation job — not by this code path.

## Convergence guarantee

DLQ consumers are wired with `errs.AlwaysRetryableProcessor`, unlimited attempts (`Retry.MaxAttempts = 0`), and `DLQ.Enabled = false` on the DLQ subscription itself. Any non-nil error returned from a DLQ controller — including a plain unclassified infra error — is therefore forced retryable and redelivered rather than silently dropped. This is the convergence guarantee the package relies on.

The recognised error condition is handled explicitly in `dlq.go`:

- `storage.ErrNotFound` is generally logged and treated as success. Conclude is stricter: a missing batch or member leaves a known conclusion obligation incomplete, so it returns an error and retains the delivery.

Everything else — including `storage.ErrVersionMismatch` on the CAS — is returned without controller-level classification and redelivered until reconciliation succeeds. `ErrVersionMismatch` is already retryable at its declaration, while the always-retryable processor makes every other non-nil reconciliation error retryable too.

## Request log entries are published to Gateway

When a DLQ controller transitions a request to `RequestStateError`, it publishes the terminal request log to the `log` topic. Gateway consumes that topic and remains the sole writer of the request log and public projections. A publish failure leaves the DLQ delivery unacknowledged so redelivery retries the same idempotent log event.

## Controller mapping

The request and batch controllers cover most primary topics; conclude has its own final reconciler:

| Controller | Topics | Decoded ID | Terminal state |
|---|---|---|---|
| `NewDLQRequestController` | `start`, `validate`, `batch`, `cancel` | `RequestID` | `RequestStateError` |
| `NewDLQBatchController` | `dependencyanalysis`, `build`, `land` | `BatchID` | `BatchStateFailed` + fan-out to member requests as `RequestStateError` |
| `NewDLQConcludeController` | `conclude` | `BatchID` | Preserve Succeeded → Landed, Failed → Error, Cancelled → Cancelled |

`buildsignal` carries a `Build` payload and has its own small dedicated controller. The split exists because the DLQ message payload shape mirrors the primary topic's payload shape (the queue framework preserves bytes verbatim under the `_dlq` topic name), so decoders use the original topic key, not the final binding. The package-level `RequestIDDecoder` interface plus `DecodeLandRequestID` / `DecodeCancelRequestID` / `DecodeRequestID` cover the three payload shapes used by request-scoped topics.

## Conclude final policy

Conclude repairs terminal queue membership before fan-out and never changes the batch state. Every non-terminal state, including Landing, returns an error, as do missing records and invalid message identities. The same final delivery retries until member writes and log publication complete; it does not republish a fresh conclude message.

Failed batches use the original message’s `failure_reason` metadata, not a later DLQ infrastructure error. An unavailable historical reason remains empty. Newly landed or cancelled members carry no LastError. Already-matching terminal members replay their persisted winning diagnostics; a different terminal winner is logged and preserved without overwriting it. Queue-wide dependent reconsideration is outside conclusion’s fan-out contract.

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
