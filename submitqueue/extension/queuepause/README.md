# Queue Pause Extension

Vendor-agnostic source of per-queue pause state. See [the pause and unpause RFC](../../../doc/rfc/submitqueue/queue-pause-and-unpause.md) for the design.

## Interfaces

### Store

Returns the pause state of a queue by name. The state is two independent switches: reject new land requests, and freeze processing of accepted requests. A queue with no entry is not paused, and an error means the backend could not be read.

The store only reports state. The gateway and orchestrator enforce it, so every deployment runs the same rejection and freezing logic and only the source of the state differs.

## Implementations

- `noop` never pauses a queue. It is the default when no pause state is configured.
- `file` reads a YAML file listing the queues that have switches set, and re-reads it on every `Get`, so editing the file pauses or unpauses a queue without a restart. The file must exist and parse when the store is constructed, so a mistyped path fails at startup. A later read or parse failure is returned as an error and never treated as not paused. Unknown fields are rejected so a misspelled switch cannot silently leave a queue unpaused.

- `fake` is an in-memory store for tests. A test sets a queue's state, changes it while consumers run, and can make every read fail. It is never used in production.

A deployer can supply its own implementation backed by a config service.

## Entities

`QueuePause` lives in `submitqueue/entity/queue_pause.go`.
