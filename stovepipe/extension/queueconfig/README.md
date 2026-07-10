# Queue Config Extension

Vendor-agnostic interface for providing Stovepipe queue configurations.

Pipeline stages read mutable runtime state from storage and read knobs such as `max_concurrent` from a config `Store` at call time, matching the SubmitQueue split documented in [submitqueue/extension/queueconfig/README.md](../../../submitqueue/extension/queueconfig/README.md).

## Interfaces

`Store` provides queue configurations by name via `Get` and `List`. See `queueconfig.go`.

## Entities

Queue configuration entity lives in `stovepipe/entity/queue_config.go` and carries deployment knobs (`max_concurrent`, `gate_wait_delay_ms`) separate from the mutable `Queue` row.

## Implementations

`default` returns the global wiring defaults for any non-empty queue name until a file- or service-backed store lands.
