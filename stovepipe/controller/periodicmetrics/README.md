# Queue health metrics

Stovepipe seeds one durable task per queue at startup. Replicas with distinct subscriber IDs share the work: one worker samples the queue each minute and schedules the same task for the next run. A worker crash can cause a repeat sample during recovery.

Metrics use the `stovepipe.periodicmetrics` prefix and a `queue` tag:

| Gauge | Value |
| --- | --- |
| `last_green_timestamp_seconds` | Last-green change creation time, in Unix seconds |
| `last_green_duration_seconds` | Age of the last-green change, in seconds |
| `ref_head_timestamp_seconds` | Ref-head change creation time, in Unix seconds |
| `last_green_behind_head_seconds` | Ref-head creation time minus last-green creation time, in seconds |

A queue with no last-green change emits `last_green_missing` and `observed` without gauges.
