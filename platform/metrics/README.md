# Metrics Utilities (`platform/metrics`)

The `metrics` package provides reusable helpers for emitting counters, timers, histograms, and gauges on a `tally.Scope`. It standardizes metric names across controllers and integrates with `platform/errs` for automatic error classification tags.

## Design

**Free functions on `tally.Scope`** — no wrapper types. Existing constructors accept `tally.Scope` and don't need to change.

**Operation lifecycle** — `Begin` and `Complete` tie the full metrics lifecycle together. `Begin` captures the start time and emits `{name}.called`; `Complete` emits succeeded/failed counters, a latency timer, and a latency histogram. This prevents mismatched or forgotten metrics calls.

**Error-aware tagging** — `ErrorTags` integrates with `platform/errs` to produce `error_origin=user|infra`, `retryable=true|false`, and `dependency=true` tags automatically. `Complete` uses these to tag latency metrics on failure.

**Consistent naming** — all Named helpers follow the `{name}.{sub}` sub-scope pattern, producing structured metric paths like `process.called`, `publish.attempts`, `consumer.pending_messages`.

## Operation Lifecycle

For any operation with a clear start/end, use `Begin`/`Complete`:

| Function | Emits |
|----------|-------|
| `Begin(scope, name, ...tags)` | `{name}.called` counter +1, returns `Op` |
| `op.Complete(err)` | `{name}.succeeded` or `{name}.failed` counter, `{name}.latency` timer, `{name}.latency_histogram` histogram — all tagged with `result=success\|error` and error classification tags on failure |

```go
// RPC controller
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (resp *pb.LandResponse, retErr error) {
    op := metrics.Begin(c.scope, "land")
    defer func() { op.Complete(retErr) }()

    // ... business logic ...
    return &pb.LandResponse{Sqid: request.ID}, nil
}

// Queue controller
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
    op := metrics.Begin(c.scope, "process")
    defer func() { op.Complete(retErr) }()

    // ... business logic ...
    return nil
}
```

On success, `Complete` emits:
- `{name}.succeeded` counter +1
- `{name}.latency` timer tagged `result=success`
- `{name}.latency_histogram` histogram tagged `result=success`

On failure, `Complete` emits:
- `{name}.failed` counter +1
- `{name}.latency` timer tagged `result=error`, `error_origin=user|infra`, `retryable=true|false`, and optionally `dependency=true`
- `{name}.latency_histogram` histogram with the same tags

## Named Helpers

For ad-hoc metrics that don't fit the Begin/Complete lifecycle. All follow the `{name}.{sub}` sub-scope pattern:

| Function | Emits | Example |
|----------|-------|---------|
| `NamedCounter(scope, name, counter, value, ...tags)` | `{name}.{counter}` counter | `publish.attempts` |
| `NamedTimer(scope, name, timer, duration, ...tags)` | `{name}.{timer}` timer | `publish.queue_latency` |
| `NamedHistogram(scope, name, histogram, buckets, ...tags)` | `{name}.{histogram}` histogram | `process.duration` |
| `NamedGauge(scope, name, gauge, value, ...tags)` | `{name}.{gauge}` gauge | `consumer.pending_messages` |

```go
// Count a specific sub-event
metrics.NamedCounter(c.scope, "publish", "attempts", 1)

// Record a specific sub-latency
metrics.NamedTimer(c.scope, "publish", "queue_latency", elapsed)

// Track current queue depth (goes up and down)
metrics.NamedGauge(c.scope, "consumer", "pending_messages", float64(len(pending)))

// Create a reusable histogram (store on struct, call RecordDuration per invocation)
h := metrics.NamedHistogram(c.scope, "process", "duration", tally.DurationBuckets{...})
h.RecordDuration(elapsed)
```

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
defer func() { op.Complete(retErr) }()

metrics.NamedCounter(c.scope, "publish", "attempts", 1, metrics.NewTag("topic", c.topic))
```

## Latency Buckets

`Complete` uses default latency buckets (5ms to 4h) automatically, suitable for both fast RPCs and long-running operations like builds and merges:

```
5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, 30s, 1m, 2m, 5m, 10m, 30m, 1h, 2h, 4h
```

For custom histograms, pass your own buckets to `NamedHistogram`:

```go
h := metrics.NamedHistogram(c.scope, "build", "duration", tally.DurationBuckets{...})
```
