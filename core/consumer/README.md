# Consumer

The consumer package orchestrates queue message processing. It manages subscription lifecycle, message consumption, ack/nack, and graceful shutdown.

## Architecture

```
Consumer
  ├── Controller A (topic: "request")
  │     └── consumeLoop
  │           ├── processPartition("part-1")  ← serial per partition
  │           ├── processPartition("part-2")
  │           └── processPartition("part-3")
  └── Controller B (topic: "build")
        └── consumeLoop
              └── processPartition("part-1")
```

The consumer spawns one `consumeLoop` goroutine per controller. Each `consumeLoop` dispatches deliveries to per-partition goroutines, preserving ordering within each partition while processing different partitions in parallel.

## Interfaces

### Consumer

The top-level orchestrator. Register controllers, start consuming, and stop gracefully.

```go
registry, _ := consumer.NewTopicRegistry([]consumer.TopicConfig{
    {Key: consumer.TopicKeyRequest, Name: "request", Queue: q, Subscription: subConfig},
})

c := consumer.New(logger, scope, registry)

c.Register(myController)
c.Start(ctx)

// On shutdown:
if err := c.Stop(30000); err != nil {
    logger.Errorw("consumer stop error", "error", err)
}
```

### Controller

Business logic for processing queue messages. Implement this interface to handle deliveries for a specific topic.

```go
type Controller interface {
    Process(ctx context.Context, delivery Delivery) error
    Name() string
    TopicKey() TopicKey
    ConsumerGroup() string
}
```

### Delivery

A restricted view of a queue delivery exposed to controllers. Hides Ack/Nack/Reject (handled automatically by Consumer) while exposing message data and `ExtendVisibilityTimeout`.

## TopicRegistry

The `TopicRegistry` maps topic keys to queue backends, topic names, and subscription configs. This decouples controllers from infrastructure wiring.

```go
registry, _ := consumer.NewTopicRegistry([]consumer.TopicConfig{
    {
        Key:          consumer.TopicKeyRequest,
        Name:         "request",
        Queue:        q,
        Subscription: extqueue.DefaultSubscriptionConfig("worker-1", "orchestrator"),
    },
    {
        Key:   consumer.TopicKeyBuild,
        Name:  "build",
        Queue: q,
        // No Subscription — publish-only topic
    },
})
```

**Topic keys** are fixed identifiers for pipeline stages (e.g., `TopicKeyRequest`, `TopicKeyBuild`). The actual queue topic name is configured separately, so library consumers can use their own naming conventions.

## Error Handling

Controllers signal processing outcome via the return value of `Process()`:

- **`return nil`** — success, message is acked.
- **`return errs.NewRetryableError(err)`** — retryable failure, message is nacked for retry.
- **`return err`** — non-retryable error (e.g. poison pill), message is rejected and removed from the queue to prevent infinite retry loops.

```go
func (c *MyController) Process(ctx context.Context, delivery consumer.Delivery) error {
    msg := delivery.Message()

    result, err := c.service.Process(ctx, msg.Payload)
    if err != nil {
        if isTransient(err) {
            return errs.NewRetryableError(err)  // nack → retry
        }
        return err  // reject → DLQ
    }

    return nil  // ack → done
}
```

## Lifecycle

1. **Register** controllers before starting.
2. **Start** subscribes to all topics and spawns consume loops. Startup is atomic — if any subscription fails, all started subscriptions are cleaned up.
3. **Stop** cancels all subscriptions and waits for goroutines to finish (with timeout budget split across controllers).

Once stopped, the consumer cannot be restarted — `Register()` and `Start()` return errors.
