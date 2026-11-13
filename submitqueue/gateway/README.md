# SubmitQueue Gateway

The gateway is the RPC entry point to SubmitQueue. It accepts `Land`, `Cancel`, `GetRequestSummaryByID`, `GetRequestSummaryByChangeURI`, `List`, `GetRequestHistoryByID`, `GetRequestHistoryByChangeURI`, and `Ping` calls. It validates edge-owned input and hands asynchronous work to the orchestrator through the message queue.

## Request receipts and current summaries

`Land` first creates an internal request summary in `accepting` state. That receipt prevents a failed publish from exposing work that was never admitted. After the start message is published, the gateway records the `accepted` log through the request materializer.

The materializer appends the log, chooses the winning current status, and activates or repairs the public projections:

- The authoritative request summary keyed by sqid.
- One change-URI mapping per submitted URI.
- The queue-ordered projection used by `List`.

`GetRequestSummaryByID` and `GetRequestSummaryByChangeURI` read authoritative summaries. `List` reads the queue projection and may briefly lag while later log materialization repairs a partial attempt. If recording `accepted` fails after publication, `Land` still succeeds because subsequent pipeline logs can advance the internal `accepting` summary and create the public projections.

## Request log ownership

The gateway owns the request log read model and is the only service that reads it.

- `Land` publishes first and then attempts to materialize `accepted`; publication is its success boundary. `Cancel` materializes `cancelling` before publishing so the user's intent is visible when the RPC returns.
- For statuses produced downstream, the orchestrator publishes entries to the `log` topic through `submitqueue/orchestrator/core/request.PublishLog`. The gateway consumes that topic and persists each entry through the same materializer (`submitqueue/gateway/core/request`).
- Orchestrator DLQ reconciliation transitions durable request state and publishes terminal log entries to the same `log` topic; the gateway remains the materializer.
- `GetRequestHistoryByID` and `GetRequestHistoryByChangeURI` read retained request-log rows directly.

The materializer appends every audit event, selects the current authoritative winner, and repairs the queue projection. The normal orchestrator pipeline does not read or write the request-log store directly.

## Final log recovery

The primary `gateway-log` consumer sends failed deliveries to `log_dlq` after a non-retryable error or exhaustion of its finite retry budget. The gateway's `gateway-log-dlq` consumer replays retained entries through the same log controller and materializer, with unlimited attempts, capped backoff, and no second-level DLQ. Valid log deliveries are acknowledged only after history, summary, URI mappings, and receipt projection succeed. Replays may append equivalent history rows; terminal-summary precedence and mapping repair are unchanged.

With valid envelope metadata, malformed payloads and payload-queue identity conflicts stay in `log_dlq`, with topic/group-tagged diagnostics and deserialize or identity-error counters. Retrying unchanged bad data does not repair it. This is retention, not isolated quarantine: poison records can occupy the fetch window and delay later recovery. Operators must durably export a bad record and record its disposition before deleting it; corrected payloads must be replayed through gateway materialization. No export/replay CLI is provided. Rolling back the gateway stops automatic recovery but must leave the backlog intact.

Invalid envelope metadata is logged and rejected before the controller runs. If no further DLQ exists, rejection may acknowledge and discard the delivery without retry; operators investigate the logged failure if needed.
