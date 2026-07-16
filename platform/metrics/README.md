# Metrics Utilities (`platform/metrics`)

The `metrics` package provides reusable helpers for emitting counters, histograms, and gauges on a `tally.Scope`. It standardizes metric names across controllers and integrates with `platform/errs` for automatic error classification tags.

## Design

**Free functions on `tally.Scope`** — no wrapper types. Existing constructors accept `tally.Scope` and don't need to change.

**Operation lifecycle** — `Begin` and `Complete` tie the full metrics lifecycle together. `Begin` captures the start time and emits `{name}.called`; `Complete` emits succeeded/failed counters and a latency histogram. This prevents mismatched or forgotten metrics calls.

**Error-aware tagging** — `ErrorTags` integrates with `platform/errs` to produce `error_origin=user|infra`, `retryable=true|false`, and `dependency=true` tags automatically. `Complete` uses these to tag latency metrics on failure.

**Consistent naming** — all Named helpers follow the `{name}.{sub}` sub-scope pattern, producing structured metric paths like `process.called`, `publish.attempts`, `consumer.pending_messages`.

## Operation Lifecycle

For any operation with a clear start/end, use `Begin`/`Complete`:

| Function | Emits |
|----------|-------|
| `Begin(scope, name, ...tags)` | `{name}.called` counter +1, returns `Op` |
| `op.Complete(err, buckets)` | `{name}.succeeded` or `{name}.failed` counter, `{name}.latency` histogram (recorded with the given `buckets`) — tagged with `result=success\|error` and error classification tags on failure |

`buckets` is required — there is no default. Operations differ widely in expected latency, so the caller passes the bucket set (see [Latency Buckets](#latency-buckets)) that matches the operation.

```go
// RPC controller
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (resp *pb.LandResponse, retErr error) {
    op := metrics.Begin(c.scope, "land")
    defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

    // ... business logic ...
    return &pb.LandResponse{Sqid: request.ID}, nil
}

// Queue controller
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
    op := metrics.Begin(c.scope, "process")
    defer func() { op.Complete(retErr, metrics.LongLatencyBuckets) }()

    // ... business logic ...
    return nil
}
```

On success, `Complete` emits:
- `{name}.succeeded` counter +1
- `{name}.latency` histogram tagged `result=success`

On failure, `Complete` emits:
- `{name}.failed` counter +1
- `{name}.latency` histogram tagged `result=error`, `error_origin=user|infra`, `retryable=true|false`, and optionally `dependency=true`

## Named Helpers

For ad-hoc metrics that don't fit the Begin/Complete lifecycle. All follow the `{name}.{sub}` sub-scope pattern:

| Function | Emits | Example |
|----------|-------|---------|
| `NamedCounter(scope, name, counter, value, ...tags)` | `{name}.{counter}` counter | `publish.attempts` |
| `NamedHistogram(scope, name, histogram, buckets, ...tags)` | `{name}.{histogram}` histogram | `process.duration` |
| `NamedGauge(scope, name, gauge, value, ...tags)` | `{name}.{gauge}` gauge | `consumer.pending_messages` |

```go
// Count a specific sub-event
metrics.NamedCounter(c.scope, "publish", "attempts", 1)

// Record a one-shot sub-latency as a histogram (pass the bucket set that fits)
metrics.NamedHistogram(c.scope, "publish", "queue_latency", metrics.StorageLatencyBuckets).RecordDuration(elapsed)

// Track current queue depth (goes up and down)
metrics.NamedGauge(c.scope, "consumer", "pending_messages", float64(len(pending)))

// Reuse a histogram on a hot path (store on struct, call RecordDuration per invocation)
h := metrics.NamedHistogram(c.scope, "process", "duration", metrics.FastLatencyBuckets)
h.RecordDuration(elapsed)
```

### Why histograms, not timers

Durations are recorded as **histograms**, never timers. A timer ships raw durations and the monitoring backend derives percentiles (p50/p99/max) **per time series** — one series per unique combination of metric name and tag values, so each distinct tag value (region, zone, …) multiplies the series count. The moment a dashboard or alert spans more than one series — rolling up a tag you didn't pin to a single value — the backend has to combine already-aggregated per-series statistics, and timer percentiles don't combine: the p99 across N series is not the average, max, or any function of each series' p99. Only the (count-weighted) mean survives, so precision degrades as the number of aggregated series grows — and high-cardinality tags make it worse. The rollups you reach for during an incident ("p99 across the whole region") are exactly the imprecise ones. Bucketed histograms merge exactly: summing per-series bucket counts reconstructs the true combined distribution, so every percentile stays accurate at any aggregation level and over any time window.

## Error Tags

`ErrorTags` classifies errors using `platform/errs` and returns tags for dimensional filtering:

| Tag | Values | Source |
|-----|--------|--------|
| `error_origin` | `user`, `infra` | `errs.IsUserError` |
| `retryable` | `true`, `false` | `errs.IsRetryable` |
| `dependency` | `true` (only when applicable) | `errs.IsDependencyError` |

```go
tags := metrics.ErrorTags(err)
// Generic error:     [{error_origin, infra}, {retryable, false}]
// User error:        [{error_origin, user},  {retryable, false}]
// Retryable error:   [{error_origin, infra}, {retryable, true}]
// Dependency error:  [{error_origin, infra}, {retryable, false}, {dependency, true}]
```

## Tags

Use `NewTag` to pass additional dimensional tags to any helper:

```go
op := metrics.Begin(c.scope, "process", metrics.NewTag("queue", req.Queue))
defer func() { op.Complete(retErr, metrics.LongLatencyBuckets) }()

metrics.NamedCounter(c.scope, "publish", "attempts", 1, metrics.NewTag("topic", c.topic))
```

## Latency Buckets

There is **no default** bucket set: operations range from sub-millisecond in-memory work to multi-hour builds, and one set can't serve all of them well. Both `Op.Complete` and `NamedHistogram` require the caller to pass buckets, so resolution concentrates where the operation's latency actually lands and buckets far outside that range don't waste series cardinality. The package exports three common sets:

| Set | Range | Use for |
|-----|-------|---------|
| `FastLatencyBuckets` | ~100µs – 5s | Fast in-process work: scoring, cache lookups, CPU-bound operations |
| `StorageLatencyBuckets` | ~1ms – 1m | Storage and message-queue round-trips: DB reads/writes, publish/consume, RPC handlers |
| `LongLatencyBuckets` | ~5ms – 4h | Long-running pipeline work and external calls: builds, merges, git pushes, provider calls |

Pass one of these, or your own `tally.DurationBuckets` when none fits:

```go
defer func() { op.Complete(retErr, metrics.StorageLatencyBuckets) }()

h := metrics.NamedHistogram(c.scope, "build", "duration", tally.DurationBuckets{ /* custom */ })
```
