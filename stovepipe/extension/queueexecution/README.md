# Queue execution observation

The read-only Reader observes the queue-level pause control separately from enablement policy and validation results. Production hosts should evaluate the same queue-wide control used by their consumer gate. Stage or partition pauses cannot be flattened into this summary without an explicit aggregation policy.

Return unknown or an error when the control cannot be determined. GetQueueStatus preserves resolved policy with UNKNOWN execution for observation failures, while propagating cancellation. The no-op implementation reports running for an ungated standalone host. See the [queue-status contract](../../../doc/rfc/stovepipe/queue-status.md).
