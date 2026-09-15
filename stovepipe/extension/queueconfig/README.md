# Queue Config Extension

Vendor-agnostic interface for providing Stovepipe queue configurations.

Pipeline stages read mutable runtime state from storage and read knobs such as `max_concurrent` from a config `Store` at call time, matching the SubmitQueue split documented in [submitqueue/extension/queueconfig/README.md](../../../submitqueue/extension/queueconfig/README.md).

## Interfaces

`Store` provides queue configurations by name via `Get` and `List`. See `queueconfig.go`.

## Entities

Queue configuration entity lives in `stovepipe/entity/queue_config.go` and carries deployment knobs (`max_concurrent`, `gate_wait_delay_ms`, `minimum_build_admission_interval_ms`, `failure_cooldown_ms`) separate from the mutable `Queue` row. The minimum interval is start-to-start spacing between logical admissions. The failure cooldown applies after a build runner reports `failed`; cancellation and DLQ-forced failure do not apply it. Positive values enable each time policy; non-positive values disable it.

## Implementations

- `default` returns the global wiring defaults for any non-empty queue name. Both admission intervals are disabled.
- `yaml` loads a validated immutable snapshot from a file. Every queue entry specifies its concurrency, gate delay, general minimum admission interval, and failure cooldown.
