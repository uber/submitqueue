# File-Backed Consumer Gate

Stores gate state as plain files under a configured root directory. Presence of a gate file means the gate is closed; deleting it opens the gate. See the [extension README](../README.md) and [doc/rfc/consumer-gate.md](../../../../doc/rfc/consumer-gate.md) for the contract and design rationale.

## Layout

```
{dir}/gates/{consumer_group}/all                         # gates every partition of the controller
{dir}/gates/{consumer_group}/p-{urlenc(partition)}       # gates one partition
{dir}/parked/{consumer_group}/{topic}/{urlenc(id)}.json  # one parked delivery record
```

Partition keys and message IDs may contain `/` (request IDs like `queue/1`), so they are URL-encoded in file names. Gate files carry human-readable JSON metadata (`reason`, `created_by`, `created_at_ms`); parked records carry the payload, attempt, and `parked_at_ms` while a delivery is blocked. The record is deleted before the wait ends, so the parked tree contains only active waits and does not retain payloads after release or cancellation. All writes go through temp-file-plus-rename so readers never see partial JSON.

Gate state is not cached. Each delivery reads its applicable gate files, and each blocked delivery polls those files at the configured interval until they are absent.

## Operating it by hand

Pause a controller: write any JSON to `{dir}/gates/{group}/all`. Resume: `rm` the file. Inspect what a paused stage is holding: `ls`/`cat` under `{dir}/parked/{group}/`.

## Reach and limits

The directory is trivially shareable out of process — the e2e stack bind-mounts a host directory into every service container, and the test manipulates gates and reads parked records as local files. A file gates only the replicas that see the directory; fleet-wide pause needs the deployment platform to distribute the file, or a store-backed implementation of the same contract.
