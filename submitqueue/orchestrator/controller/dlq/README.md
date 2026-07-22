# DLQ Reconciliation Controllers

This package contains the controllers that drain each primary pipeline topic's `{topic}_dlq` companion and reconcile affected request and batch entities into terminal states. They are wired alongside the primary controllers in `service/submitqueue/orchestrator/server/main.go`.

## Design principles

**The DLQ is the final destination.** A message in `{topic}_dlq` has already exhausted the primary controller's retry policy or surfaced as non-retryable. DLQ subscriptions disable their own DLQ, so a controller either reconciles the message or keeps retrying it. An unprocessable message must be removed by an operator.

**Reconcile state rather than rerun business work.** DLQ controllers decode the original payload, resolve the affected identity, and converge state and public request logs. They do not repeat external actions such as builds or merges.

**Repair partial materialization.** Request reconciliation republishes the terminal log when the state was already written. Failed batches repeat request fanout, and the dedicated conclude DLQ repairs fanout according to the batch's existing terminal outcome.

## Convergence guarantee

DLQ consumers use `errs.AlwaysRetryableProcessor`, a high retry limit, and `DLQ.Enabled = false`. Any non-nil controller error is therefore redelivered rather than silently dropped.

`storage.ErrNotFound` is logged and treated as success because there is no entity to reconcile. `storage.ErrVersionMismatch` retains its declaration-level retryable classification through reconciliation, while other DLQ errors are made retryable by `AlwaysRetryableProcessor`.

## Request log entries

When a DLQ terminates a request as `RequestStateError`, it publishes the matching log to the `log` topic. Gateway remains the sole writer of request logs and public projections. The log preserves `dlq.last_error` in `LastError` and records the original topic, failure count, and failure time as metadata when available.

## Controller mapping

| Controller | Topics | Decoded identity | Reconciliation |
|---|---|---|---|
| `NewDLQRequestController` | `start`, `validate`, `batch`, `cancel` | Request ID from the original payload | Request to `Error` |
| `NewDLQMergeConflictSignalController` | `mergeconflictsignal` | Request ID from Runway `MergeResult` | Request to `Error` |
| `NewDLQBatchController` | `score`, `speculate`, `build`, `merge` | `BatchID` | Batch to `Failed`, members to `Error` |
| `NewDLQBuildSignalController` | `buildsignal` | `BuildID`, then `BatchID` | Batch to `Failed`, members to `Error` |
| `NewDLQMergeSignalController` | `mergesignal` | Batch ID from Runway `MergeResult` | Batch to `Failed`, members to `Error` |
| `NewDLQConcludeController` | `conclude` | `BatchID` | Preserve terminal batch outcome and repair member fanout |

## Idempotency and concurrent activity

- A request already in the target terminal state skips the CAS and republishes its terminal log.
- A request in a different terminal state is left unchanged.
- A batch already in `BatchStateFailed` repeats member fanout.
- A request in `RequestStateCancelling` is reconciled to `RequestStateError` when its pipeline message reaches a normal DLQ.
- A conclude DLQ maps `Succeeded`, `Failed`, and `Cancelled` batches to `Landed`, `Error`, and `Cancelled` request outcomes respectively.

## See also

- `platform/errs/README.md` describes error processing and `AlwaysRetryableProcessor`.
- `doc/rfc/submitqueue/workflow.md` describes the primary pipeline mirrored by these DLQ companions.
