# Runway

Runway owns the merge queues defined by the external contract in
[`api/runway/messagequeue`](../api/runway/messagequeue): it consumes merge-conflict-check and merge
requests, performs the work, and (eventually) publishes the result to the corresponding signal queue.
SubmitQueue is a client of these queues.

Runway service layout:

- `orchestrator/` — Orchestrator service: consumes the merge-conflict-check and merge queues.
