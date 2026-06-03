# Queue Abstractions

Vendor-agnostic interfaces for pub/sub messaging systems.

## Interfaces

### Queue
Creates publishers and subscribers.

### Publisher
Publishes messages to topics.

```go
type Publisher interface {
    Publish(ctx context.Context, topic string, message queue.Message) error
    PublishAfter(ctx context.Context, topic string, message queue.Message, delayMs int64) error
    Close() error
}
```

**`PublishAfter`** inserts a fresh message that becomes visible to subscribers only after `delayMs`. It is distinct from `Nack(requeueAfterMillis)` even though both can produce "next delivery happens at T+delay":

- `Nack` is "this delivery failed, try again" — it bumps `retry_count` and eventually trips DLQ.
- `PublishAfter` is "postpone this work" — `retry_count` resets to 0, DLQ stays available for true failures.

Use `PublishAfter` for self-driven poll loops (e.g. the orchestrator's `buildsignal` consumer re-publishing itself between `Status` calls). Use `Nack` for processing failures.

### Subscriber
Consumes messages from topics with per-subscription configuration.

```go
type Subscriber interface {
    Subscribe(ctx context.Context, topic string, config SubscriptionConfig) (<-chan Delivery, error)
    Close() error
}
```

### Delivery
Message with acknowledgment operations.

```go
type Delivery interface {
    Message() queue.Message
    Ack(ctx context.Context) error
    Nack(ctx context.Context, requeueAfterMillis int64) error
    Reject(ctx context.Context, reason string) error
    ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error
    DeliveryID() string
    Attempt() int
    ReceivedAt() int64
    Metadata() map[string]string
}
```

- **Ack** — message processed successfully, remove from queue
- **Nack** — processing failed, requeue for retry after delay
- **Reject** — poison pill, move to DLQ (or ack if DLQ disabled)
- **ExtendVisibilityTimeout** — extend processing window for long-running work

### SubscriptionConfig

Per-subscription configuration for polling, batching, leasing, retries, and DLQ:

```go
cfg := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")
cfg.PollIntervalMs = 50
cfg.BatchSize = 20
cfg.VisibilityTimeoutMs = 60000
cfg.Retry.MaxAttempts = 3
cfg.DLQ.Enabled = true
```

See `subscription_config.go` for all fields and defaults.

## Usage

```go
q, _ := NewQueue(config)
defer q.Close()

// Publish
pub := q.Publisher()
msg := queue.NewMessage("id", []byte("payload"), "partition-key", nil)
pub.Publish(ctx, "topic", msg)

// Subscribe
sub := q.Subscriber()
cfg := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")
deliveries, _ := sub.Subscribe(ctx, "topic", cfg)
for delivery := range deliveries {
    if err := process(delivery.Message().Payload); err != nil {
        delivery.Nack(ctx, 0)  // Retry
        continue
    }
    delivery.Ack(ctx)
}
```

## Implementing a Backend

1. Create `extension/queue/{backend}/` directory
2. Implement `Queue`, `Publisher`, `Subscriber`, `Delivery` interfaces
3. Map `queue.Message` to backend format

See `extension/queue/mysql/` for the reference implementation.
