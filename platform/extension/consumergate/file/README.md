# File-Backed Consumer Gate

Stores gate state as plain files under an explicitly configured root directory for E2E tests and single-host development. Presence of a gate file means the gate is closed; deleting it opens the gate. See the [extension README](../README.md) and [doc/rfc/consumer-gate.md](../../../../doc/rfc/consumer-gate.md) for the contract and design rationale.

## Layout

```
{dir}/gates/{consumer_group}/all                         # gates every partition of the controller
{dir}/gates/{consumer_group}/p-{urlenc(partition)}       # gates one partition
{dir}/parked/{consumer_group}/{topic}/{urlenc(id)}.json  # one parked delivery record
```

Partition keys and message IDs may contain `/` (request IDs like `queue/1`), so they are URL-encoded in file names. Gate files carry human-readable JSON metadata (`reason`, `created_by`, `created_at_ms`); parked records carry the payload, attempt, and `parked_at_ms` while a delivery is blocked. The record is refreshed on each re-check of a still-closed gate and removed on the admit path once the gate opens, so the parked tree contains only currently blocked deliveries and does not retain payloads after release. All writes go through temp-file-plus-rename so readers never see partial JSON.

Gate state is not cached and the store never polls. Each delivery attempt reads its applicable gate files; a blocked delivery's re-check cadence is the consumer's postpone delay, so gate state is re-read on every redelivery.

## Operating it by hand

Pause a controller: write any JSON to `{dir}/gates/{group}/all`. Resume: `rm` the file. Inspect what a paused stage is holding: `ls`/`cat` under `{dir}/parked/{group}/`.

## Reach and limits

The E2E stack bind-mounts one test-owned host directory into every service container, and the test manipulates gates and reads parked records as local files. A file gates only processes that see that directory. State survives a process or container restart only if the configured directory survives it, and it does not survive node replacement without shared persistent storage. This implementation is not a fleet-wide production control plane; a production pause feature requires another shared `consumergate` backend.
