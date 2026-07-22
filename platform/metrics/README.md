# Metrics Utilities (`platform/metrics`)

The `metrics` package provides reusable helpers for emitting counters and histograms on a `tally.Scope`.

## Design

**Free functions on `tally.Scope`** — no wrapper types. Existing constructors accept `tally.Scope` and do not need to change.

**Operation lifecycle** — `Begin` and `Complete` tie operation metrics together. `Begin` captures the start time and emits `{name}.start`; `Complete` records duration and count on `{name}.finish`.

**Result tagging** — the finish histogram is tagged with `result=success`, `result=error`, or `result=cancel`. Cancellation is detected with `errors.Is(err, context.Canceled)`. Error classification tags are intentionally omitted because many call sites complete before classification occurs.

**Consistent naming** — named helpers follow the `{name}.{sub}` sub-scope pattern, producing metric paths such as `process.start` and `publish.attempts`.

## Operation Lifecycle

For any operation with a clear start and end, use `Begin` and `Complete`:

| Function | Emits |
|----------|-------|
| `Begin(scope, name, buckets, ...tags)` | `{name}.start` counter +1 and returns an `Op` |
| `op.Complete(err)` | `{name}.finish` histogram tagged with `result=success\|error\|cancel` |

`buckets` is required at `Begin` because operations differ widely in expected latency. The finish histogram records both the duration distribution and the number of completed operations, so `Complete` does not emit a separate counter.

```go
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
    op := metrics.Begin(c.scope, "process", metrics.LongLatencyBuckets)
    defer func() { op.Complete(retErr) }()

    // ... business logic ...
    return nil
}
```

## Named Helpers

For ad-hoc metrics that do not fit the operation lifecycle:

| Function | Emits | Example |
|----------|-------|---------|
| `NamedCounter(scope, name, counter, value, ...tags)` | `{name}.{counter}` counter | `publish.attempts` |
| `NamedHistogram(scope, name, histogram, buckets, ...tags)` | `{name}.{histogram}` histogram | `process.duration` |

```go
metrics.NamedCounter(c.scope, "publish", "attempts", 1)

h := metrics.NamedHistogram(c.scope, "process", "duration", metrics.FastLatencyBuckets)
h.RecordDuration(elapsed)
```

Use `tally.Scope` directly for gauges:

```go
c.scope.SubScope("consumer").Gauge("pending_messages").Update(float64(len(pending)))
```

### Why histograms, not timers

Durations are recorded as histograms rather than timers. Timer percentiles cannot be combined accurately across time series, while bucketed histogram counts can be summed to reconstruct a combined distribution for correct aggregate percentiles.

## Tags

Use `NewTag` to pass dimensional tags to a helper:

```go
op := metrics.Begin(c.scope, "process", metrics.LongLatencyBuckets, metrics.NewTag("queue", req.Queue))
defer func() { op.Complete(retErr) }()

metrics.NamedCounter(c.scope, "publish", "attempts", 1, metrics.NewTag("topic", c.topic))
```

## Latency Buckets

There is no default bucket set. The package exports three common sets:

| Set | Range | Use for |
|-----|-------|---------|
| `FastLatencyBuckets` | ~100µs – 5s | Fast in-process work such as scoring, cache lookups, and CPU-bound operations |
| `StorageLatencyBuckets` | ~1ms – 1m | Storage and message-queue round trips such as database reads, writes, publishing, and consuming |
| `LongLatencyBuckets` | ~5ms – 4h | Long-running pipeline work and external calls such as builds, merges, pushes, and provider calls |

Pass one of these sets or a custom `tally.DurationBuckets` to `Begin` or `NamedHistogram`.
