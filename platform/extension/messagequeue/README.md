# Queue Abstractions

Vendor-agnostic interfaces for pub/sub messaging systems.

## Interfaces

### Queue
Creates publishers and subscribers.

### Publisher
Publishes messages to topics.

```go
type Publisher interface {
    Publish(ctx context.Context, topic string, message entityqueue.Message) error
    Close() error
}
```

(`entityqueue` is `github.com/uber/submitqueue/platform/base/messagequeue`.)

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
    Message() entityqueue.Message
    Ack(ctx context.Context) error
    Nack(ctx context.Context) error
    Postpone(ctx context.Context, delayMs int64) error
    Reject(ctx context.Context, reason string) error
    ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error
    DeliveryID() string
    Attempt() int
    ReceivedAt() int64
    Metadata() map[string]string
}
```

- **Ack** — message processed successfully, remove from queue
- **Nack** — processing failed, requeue for immediate retry
- **Postpone** — processed successfully but must wait: redeliver after delay, without consuming retry budget; the message is a barrier its partition waits behind
- **Reject** — poison pill, move to DLQ (or ack if DLQ disabled)
- **ExtendVisibilityTimeout** — extend processing window for long-running work

**`Postpone` vs `Nack` vs `ExtendVisibilityTimeout`:** `Nack` is a failure — the message is immediately eligible again, the redelivery counts toward `Retry.MaxAttempts` and eventually trips the DLQ, and later offsets in the partition keep flowing past the nacked message (a failed message must not halt its partition). `Postpone` is a deliberate wait — the redelivery happens after the chosen delay, resets the failure streak (it restarts at attempt 1), and blocks the partition behind it until it redelivers, in order. `ExtendVisibilityTimeout` is neither: the delivery is still being processed and stays in flight.

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
import entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"

q, _ := NewQueue(config)
defer q.Close()

// Publish
pub := q.Publisher()
msg := entityqueue.NewMessage("id", []byte("payload"), "partition-key", nil)
pub.Publish(ctx, "topic", msg)

// Subscribe
sub := q.Subscriber()
cfg := extqueue.DefaultSubscriptionConfig("worker-1", "consumer-group")
deliveries, _ := sub.Subscribe(ctx, "topic", cfg)
for delivery := range deliveries {
    if err := process(delivery.Message().Payload); err != nil {
        delivery.Nack(ctx)  // Retry
        continue
    }
    delivery.Ack(ctx)
}
```

## Message IDs

A message ID is the deduplication key, scoped to its topic and partition key. A backend matches a publish against messages it still holds — including ones already consumed, since reclamation is lazy and may lag delivery by an unbounded interval — and a collision is reported to the publisher as a success that stored nothing. There is no error to retry and no row to deliver.

The ID therefore names the *occasion* to publish, not the entity published about. An entity's own ID buys exactly one message for that entity for as long as the backend remembers the first, so a stage that announces a batch at creation and another that wakes it after a land would collide, and the wake-up would vanish.

Producers do not choose IDs by hand. They publish through `platform/publish`, whose `IntentID(entityID, cause...)` composes the entity with the cause of this particular message: a retry of the same cause dedups, which is what makes redelivery safe, while a new cause about the same entity can never be swallowed. `UniqueID` is the fallback for a cause with nothing stable to name it by, and it trades that idempotency for guaranteed delivery.

Backends must treat the ID as opaque and must not derive routing, ordering, or storage layout from its structure.

## Implementing a Backend

1. Create `platform/extension/messagequeue/{backend}/` directory
2. Implement `Queue`, `Publisher`, `Subscriber`, `Delivery` interfaces
3. Map `entityqueue.Message` to backend format
4. Deduplicate publishes on (topic, partition key, message ID)

See `platform/extension/messagequeue/mysql/` for the reference implementation.
