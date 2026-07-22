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
    {Key: topickey.TopicKeyStart, Name: "request", Queue: q, Subscription: subConfig},
})

c := consumer.New(logger, scope, registry,
    errs.NewClassifierProcessor(
        genericerrs.Classifier,
        mysqlerrs.Classifier,
    ),
)

c.Register(myController)
c.Start(ctx)

// On shutdown:
if err := c.Stop(30000); err != nil {
    logger.Errorw("consumer stop error", "error", err)
}
```

The fourth argument is the `errs.ErrorProcessor` the consumer runs over every non-nil controller error before deciding ack/nack/reject. See `platform/errs/README.md` for the contract; in short, a primary pipeline consumer takes `errs.NewClassifierProcessor(...)` with the project's standard classifiers, and a DLQ-reconciliation consumer takes `errs.AlwaysRetryableProcessor`.

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
        Key:          topickey.TopicKeyStart,
        Name:         "request",
        Queue:        q,
        Subscription: extqueue.DefaultSubscriptionConfig("worker-1", "orchestrator"),
    },
    {
        Key:   topickey.TopicKeyBuild,
        Name:  "build",
        Queue: q,
        // No Subscription — publish-only topic
    },
})
```

**Topic keys** are fixed identifiers for pipeline stages (e.g., `TopicKeyStart`, `TopicKeyBuild`). Constants live in each domain's `core/topickey` package; this package defines only the `TopicKey` type and registry machinery. The actual queue topic name is configured separately, so library consumers can use their own naming conventions.

## Error Handling

The consumer passes every non-nil controller error through the configured `errs.ErrorProcessor` once and then uses `errs.IsRetryable` to decide the transport action:

- **`return nil`** — success, message is acked.
- **non-nil, retryable after processing** — message is nacked for redelivery (visibility timeout drives the retry delay).
- **non-nil, non-retryable after processing** — message is rejected, which moves it to the DLQ if one is configured for the subscription, or simply acks-and-drops if not.

Controllers therefore have two equally valid ways to surface a transient failure:

1. Return an unclassified error and let a classifier wired into the processor recognise it (e.g. a `*gomysql.MySQLError` with a deadlock code → `mysqlerrs.Classifier` → retryable).
2. Return `errs.NewRetryableError(...)` (or `NewUserError`, `NewDependencyError`, ...) explicitly when the controller already knows the right verdict — these framework wraps short-circuit any classifier walk.

```go
func (c *MyController) Process(ctx context.Context, delivery consumer.Delivery) error {
    msg := delivery.Message()

    result, err := c.service.Process(ctx, msg.Payload)
    if err != nil {
        if isUserCaused(err) {
            return errs.NewUserError(err)   // reject → DLQ, never retried
        }
        return err                          // let the processor classify; nack if retryable
    }

    return nil  // ack → done
}
```

When the consumer is wired with `errs.AlwaysRetryableProcessor` (DLQ reconciliation), the framework overrides this: every non-nil error is forced retryable so the DLQ message comes back for another attempt. See `submitqueue/orchestrator/controller/dlq/README.md`.

The consumer records `process.controller_latency` after the error processor runs. Every series has `result=success|error|cancel`; error and cancellation series also include `error_origin=user|infra`, `retryable=true|false`, and `dependency=true|false`. These dimensions therefore describe the processed error that drives ack, nack, or reject behavior rather than the controller's raw return value.

## Lifecycle

1. **Register** controllers before starting.
2. **Start** subscribes to all topics and spawns consume loops. Startup is atomic — if any subscription fails, all started subscriptions are cleaned up.
3. **Stop** cancels all subscriptions and waits for goroutines to finish (with timeout budget split across controllers).

Once stopped, the consumer cannot be restarted — `Register()` and `Start()` return errors.
