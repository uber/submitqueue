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
    Close() error
}
```

### Subscriber
Consumes messages from topics.

```go
type Subscriber interface {
    Subscribe(ctx context.Context, topic string) (<-chan Delivery, error)
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
    ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error
    DeliveryID() string
    Attempt() int
    ReceivedAt() int64
    Metadata() map[string]string
}
```

## Usage

```go
q, _ := NewQueue(config)
defer q.Close()

// Publish
pub := q.Publisher()
msg := queue.NewMessage("id", []byte("payload"))
pub.Publish(ctx, "topic", msg)

// Subscribe
sub := q.Subscriber()
deliveries, _ := sub.Subscribe(ctx, "topic")
for delivery := range deliveries {
    process(delivery.Message().Payload)
    delivery.Ack(ctx)
}
```

## Implementing a Backend

1. Create `extension/queue/{backend}/` directory
2. Implement `Queue`, `Publisher`, `Subscriber`, `Delivery` interfaces
3. Map `queue.Message` to backend format

