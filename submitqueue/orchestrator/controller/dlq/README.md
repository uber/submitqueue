# DLQ Reconciliation Controllers

This package reconciles dead-lettered SubmitQueue work. The [orchestrator stage table](../../pipeline.go) defines the primary/DLQ bindings; the host constructs them through the shared pipeline framework.

## Design principles

**Terminal reconciliation, not workflow recovery.** Idempotent primary controllers own normal processing under their primary retry policy. By default, a DLQ handler must not resume a partially completed normal workflow or repeat its business action. Repairing state, membership, and status/log outputs needed to express a justified terminal outcome is reconciliation.

**The implementation is deliberately the simplest and most reliable thing that can work.** The default is to decode the affected identity, load its state, and reconcile unfinished work to `RequestStateError` or `BatchStateFailed`, preserving existing terminal winners and valid downstream ownership. An optimistic-locking CAS prevents lost concurrent writes; it does not prove that the failed stage still owns the outcome.

Do not bind a normal orchestration controller to a DLQ, create a missing request in Started, or publish validate/build/merge work to resume the dead-lettered request. Idempotency, a missing row, or possession of the full original payload does not authorize that recovery. Likewise, final retries must not trigger or poll a failed build, rerun a merge, invoke a fixer, or automatically retry a dropped hook side effect.

## Final retry policy

Dead letters have exhausted the primary retry budget or returned a non-retryable error. Final consumers use `errs.AlwaysRetryableProcessor`, unlimited attempts (`Retry.MaxAttempts = 0`), and `DLQ.Enabled = false`. Every non-nil error returned by a DLQ controller, including a version mismatch or publish failure, is redelivered until reconciliation succeeds or an operator intervenes. Unlimited retries protect reconciliation and permitted outputs, not another business retry budget.

Disposition remains handler-specific: the generic request/batch reconcilers acknowledge missing entities; buildsignal also acknowledges a missing build or empty batch ID. Decode errors returned by these handlers remain retryable and may need operator removal. Payload/tenant mismatches are acknowledged, while invalid envelope tenant metadata is rejected before controller error processing. The shared hook handler reports and acknowledges even undecodable events. Final retry policy therefore does not promise retention of every poison message or recreation of every missing entity.

Start reports the failed submission through an Error request log with version 0, because there is no saved Request version. No normal work is published. Reconciliation validates the request identity and queue; existing rows retain authoritative-identity, batch-ownership, terminal-winner, and conditional-write guards.

## Terminal outputs and ownership

- An already-Error request skips the state CAS but republishes its terminal status log. A different terminal request outcome is left unchanged. This repairs status delivery, not the original diagnostic content or timestamp.
- An already-Failed batch repairs terminal membership and repeats Error fan-out to its member requests. Succeeded and Cancelled batches are left unchanged by the generic batch reconciler; it does not rerun Conclude.
- The generic request reconciler leaves a request owned by a live batch to that batch's completion path. A Creating batch counts as owner only when the request is Batched; an association alone can outlive an unsuccessful claim.
- Without such an owner, a Cancelling request is reconciled to Error: dead-lettering cannot confirm that cancellation completed.

Request logs are published to `log`; the gateway alone materializes them into its request log and public projections. A publish failure returns an error so redelivery can retry terminal status delivery. Replaying an existing event into gateway-owned read models is projection repair, not restarting SubmitQueue orchestration, and is not an orchestrator pipeline stage.

## Bindings and named limits

The [stage table](../../pipeline.go) is the source of truth rather than a second topology table here:

- `start` uses dedicated request reconciliation; `cancel`, `validate`, and `batch` use generic request reconciliation. Their decoders read LandRequest, CancelRequest, or the topic's request-ID payload.
- `dependency-analysis`, `build`, `submitqueue-land`, and `conclude` use generic batch reconciliation.
- `buildsignal` carries a build-ID payload, not a full Build. Its handler loads the build to find the batch and leaves build status untouched. The two Runway signal handlers read a request or batch correlation ID from `MergeResult`.
- **Speculation queue-isolation nudge:** after failing attributed culprit work (or the labelled payload fallback), a delivery that transitioned a batch may nudge `speculate` with another live batch. The primary re-derives current queue state; the failed batch is not reopened. This narrow protection for other work does not authorize replay of the dead-lettered command or guarantee a nudge after every partial reconciliation failure.
- **Hook report-and-stop:** the shared hook DLQ logs the raw event, counts it as dropped, and acknowledges it. A person decides whether to republish; the DLQ does not invoke hooks.

[Runway](../../../../doc/rfc/runway/workflow.md) resolves its own inbound dead letters by publishing a terminal FAILED result, not retrying merge execution. Stovepipe's [record](../../../../doc/rfc/stovepipe/steps/record.md), [build analysis](../../../../stovepipe/controller/buildanalysis/README.md), [issue tracking](../../../../stovepipe/controller/issuetrack/README.md), and [fix](../../../../stovepipe/controller/fix/README.md) handlers have their own bounded fact/history, abandonment, and cursor-fencing obligations. Those domain-specific rules are not permission to resume SubmitQueue work.

Agents must not choose recovery exceptions on their own; implement a new exception only when explicitly requested by the user or approved by the owning maintainer. State which durable terminal fact or abandonment authorizes each output and inspect its downstream effects: publishing to a normal stage can restart business work even if the DLQ handler itself calls no provider.

## See also

- [Error processing](../../../../platform/errs/README.md) — processor choice for primary and DLQ consumers.
- [Consumer framework](../../../../platform/consumer/README.md) — controller errors and ack/nack/reject.
- [SubmitQueue workflow](../../../../doc/rfc/submitqueue/workflow.md) — the primary pipeline and service ownership.
