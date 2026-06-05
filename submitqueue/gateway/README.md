# SubmitQueue Gateway

The gateway is the RPC entry point to SubmitQueue. It accepts `Land`, `Cancel`,
`Status`, and `Ping` calls, validates them at the edge, and hands work off to the
orchestrator pipeline asynchronously via the message queue.

## Request log ownership

The gateway is the **sole writer of the request log**. No other service persists
request log entries:

- For statuses it produces synchronously (`accepted` on `Land`, `cancelling` on
  `Cancel`), the gateway writes directly to storage so the entry is visible the
  moment the RPC returns.
- For statuses produced downstream, the orchestrator only *publishes* entries to
  the `log` topic via `submitqueue/core/request.PublishLog`. The gateway runs a
  consumer that drains the `log` topic and persists each entry to storage.

This keeps a single service responsible for the request log while letting the
orchestrator remain free of storage writes for it.
