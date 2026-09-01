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
- For statuses produced downstream, the orchestrator publishes entries to the `log` topic through `submitqueue/core/request.PublishLog`. The gateway consumes that topic and persists each entry through the same materializer.
- Orchestrator DLQ reconciliation transitions durable request state and publishes terminal log entries to the same `log` topic; the gateway remains the materializer.
- `GetRequestHistoryByID` and `GetRequestHistoryByChangeURI` read retained request-log rows directly.

The materializer appends every audit event, selects the current authoritative winner, and repairs the queue projection. The normal orchestrator pipeline does not read or write the request-log store directly.
