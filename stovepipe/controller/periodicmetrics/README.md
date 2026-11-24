# Queue health metrics

[`Controller.Observe`](periodicmetrics.go) samples one queue through its storage and source-control factories. [`Schedule`](task.go) seeds one durable task per queue with a stable message identity, so all replicas can seed it at startup without creating additional tasks. Stovepipe gives seeding a total of 30 seconds and fails startup if any task cannot be seeded. With distinct subscriber IDs in a shared consumer group, one worker leases the queue partition, samples immediately, and holds the same delivery until one minute after the sample started.

Each observation has a 45-second timeout. Observation failures are logged and held for the next minute. Shutdown cancels in-flight observations; the queue retains the task for another worker. The subscription uses unlimited attempts without a DLQ so crashes or failed postponements cannot exhaust the recurring task. Queue delivery is at least once: a crash after emitting metrics but before postponing can repeat a sample during recovery. Hosts must share a consumer group and retain held messages rather than acknowledging or expiring them.

With a `stovepipe` scope, all series use the `stovepipe.periodicmetrics` prefix and a `queue` tag:

| Gauge | Value |
| --- | --- |
| `last_green_timestamp_seconds` | Last-green change creation time, in Unix seconds |
| `last_green_duration_seconds` | Age of the last-green change, in seconds |
| `ref_head_timestamp_seconds` | Ref-head change creation time, in Unix seconds |
| `last_green_behind_head_seconds` | Ref-head creation time minus last-green creation time, in seconds |

Ages and distances clamp to zero when timestamps run backwards. A never-green queue emits `last_green_missing` and `observed` counters without gauges. Invalid or unavailable timestamps increment the corresponding diagnostic counter and return an error. If only the ref-head lookup fails, the last-green gauges are still emitted. `observed` increments only when the full observation succeeds, including the never-green case.

Run the controller, recurring-task, and wiring tests with `./tool/bazel test //stovepipe/controller/periodicmetrics:go_default_test //service/stovepipe/server:go_default_test`.
