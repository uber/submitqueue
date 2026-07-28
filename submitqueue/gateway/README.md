# SubmitQueue Gateway

The gateway is the RPC entry point to SubmitQueue. It accepts `Land`, `Cancel`, `GetRequestSummaryByID`, `GetRequestSummaryByChangeURI`, `List`, `GetRequestHistoryByID`, `GetRequestHistoryByChangeURI`, and `Ping` calls. It validates edge-owned input and hands asynchronous work to the orchestrator through the message queue.

## Request receipts and current summaries

`Land` creates gateway-owned receipt projections before publishing the request:

- An authoritative request summary keyed by sqid.
- One exact change-URI mapping per submitted URI.
- A queue-ordered receipt projection used by `List`.

`GetRequestSummaryByID` and `GetRequestSummaryByChangeURI` read authoritative summaries. `List` reads the queue projection and may briefly lag those summaries while eventual repair converges.

## Request log ownership

The gateway owns the request log read model and is the only service that reads it.

- For statuses produced synchronously by the gateway, such as `accepted` on `Land` and `cancelling` on `Cancel`, the gateway persists the event through the shared request-log materializer before returning or publishing.
- For statuses produced downstream, the orchestrator publishes entries to the `log` topic through `submitqueue/core/request.PublishLog`. The gateway consumes that topic and persists each entry through the same materializer.
- Orchestrator DLQ reconciliation materializes terminal repairs directly so the DLQ delivery remains unacknowledged until the log and public projections converge.
- `GetRequestHistoryByID` and `GetRequestHistoryByChangeURI` read retained request-log rows directly.

The materializer appends every audit event, selects the current authoritative winner, and repairs the queue projection. The normal orchestrator pipeline does not read or write the request-log store directly.
