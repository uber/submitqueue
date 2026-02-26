# Consumer

The consumer package orchestrates queue message processing. It manages subscription lifecycle, message consumption, ack/nack, and graceful shutdown.

## Interfaces

### Consumer

The top-level orchestrator. Register controllers, start consuming, and stop gracefully.

```go
c := consumer.New(logger, scope, queue, "worker-hostname")

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
    Topic() string
    ConsumerGroup() string
    SubscriptionConfig(subscriberName string) queue.SubscriptionConfig
}
```

### Delivery

A restricted view of a queue delivery exposed to controllers. Hides Ack/Nack (handled automatically by Consumer) while exposing message data and `ExtendVisibilityTimeout`.

## Error Handling

Controllers signal processing outcome via the return value of `Process()`:

- **`return nil`** — success, message is acked.
- **`return errs.NewRetryableError(err)`** — retryable failure, message is nacked for retry.
- **`return err`** — non-retryable error (e.g. poison pill), message is rejected and removed from the queue to prevent infinite retry loops.

## Lifecycle

1. **Register** controllers before starting.
2. **Start** subscribes to all topics and spawns consume loops.
3. **Stop** cancels all subscriptions and waits for goroutines to finish (with timeout).

Once stopped, the consumer cannot be restarted — `Register()` and `Start()` return errors.
