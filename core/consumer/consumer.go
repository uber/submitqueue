package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/extension/queue"
	"go.uber.org/zap"
)

const (
	// startupCleanupTimeoutMs is the timeout for cleaning up subscriptions when
	// a controller fails to start during Start().
	startupCleanupTimeoutMs = 30000
)

// Consumer orchestrates multiple queue consumers. It handles subscription lifecycle,
// message consumption, ack/nack, and graceful shutdown for the entire pipeline.
// Start(), Register() and Stop() are always called in this order so they do not need to be concurrently-safe between
// one another, but the implementation must be thread-safe between message processing and Register()/Stop() operations.
type Consumer interface {
	// Register adds a controller to the consumer. Must be called before Start().
	Register(controller Controller) error

	// Start subscribes to all registered controllers' topics and begins consuming messages.
	// Context is cancelled when the consumer is stopped, the implementation should propagate it to the controllers
	// running message processing. The implementation can react immediately to the context cancellation by returning `ctx.Err()` instead of starting the message processing,
	// but can also opt out to defer the cancellation after the message processing routine is set up.
	// Start() will only be called once at the application startup, so it does not need to be idempotent.
	Start(ctx context.Context) error

	// Stop gracefully shuts down all controllers with the specified timeout.
	// timeoutMs is the maximum time in milliseconds to wait for graceful shutdown.
	// Returns error if shutdown times out.
	// Stop() will only be called once at the application shutdown, so it does not need to be idempotent.
	Stop(timeoutMs int64) error
}

// consumer implements the Consumer interface.
type consumer struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	registry     TopicRegistry

	mu            sync.Mutex
	stopped       bool
	controllers   []Controller
	subscriptions map[TopicKey]*activeSubscription // topicKey -> subscription
}

// activeSubscription tracks the state of an active subscription.
type activeSubscription struct {
	controller Controller
	cancelFunc context.CancelFunc
	done       chan struct{} // Closed when consumeLoop exits
}

// New creates a new consumer.
// registry provides queue and subscription config for topics.
func New(logger *zap.SugaredLogger, scope tally.Scope, registry TopicRegistry) Consumer {
	return &consumer{
		logger:        logger,
		metricsScope:  scope.SubScope("consumer"),
		registry:      registry,
		subscriptions: make(map[TopicKey]*activeSubscription),
	}
}

// Register adds a controller to the consumer. Must be called before Start().
// Returns error if a controller for the same topic key is already registered or if the consumer is stopped.
func (m *consumer) Register(controller Controller) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return fmt.Errorf("consumer is stopped")
	}

	// Check for duplicate topic key registration.
	// O(n) scan is fine here — controller count is in the single digits.
	for _, c := range m.controllers {
		if c.TopicKey() == controller.TopicKey() {
			return fmt.Errorf("controller for topic key %s already registered", controller.TopicKey())
		}
	}

	m.controllers = append(m.controllers, controller)

	m.logger.Infow("registered controller",
		"controller", controller.Name(),
		"topic_key", controller.TopicKey(),
		"consumer_group", controller.ConsumerGroup(),
	)

	return nil
}

// Start subscribes to all registered controllers' topics and begins consuming messages.
// Spawns a goroutine per controller that processes deliveries and acks/nacks automatically.
func (m *consumer) Start(ctx context.Context) error {
	// Hold the lock for the entire subscribe loop so that startup is atomic:
	// either all controllers subscribe successfully or none remain active.
	// This also ensures Stop() cannot interleave with a partially-started state.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return fmt.Errorf("consumer is stopped")
	}

	if len(m.controllers) == 0 {
		return fmt.Errorf("no controllers registered")
	}

	m.logger.Infow("starting consumer",
		"controller_count", len(m.controllers),
	)

	for _, controller := range m.controllers {
		if err := m.subscribe(ctx, controller); err != nil {
			// Cleanup any started controllers. Include cleanup error if any.
			cleanupErr := m.unsubscribeAll(startupCleanupTimeoutMs)
			startErr := fmt.Errorf("failed to start controller %s: %w", controller.Name(), err)
			return errors.Join(startErr, cleanupErr)
		}
	}

	m.logger.Infow("consumer started",
		"active_subscriptions", len(m.subscriptions),
	)

	return nil
}

// subscribe subscribes a controller to its topic and spawns a consumption goroutine.
func (m *consumer) subscribe(ctx context.Context, controller Controller) error {
	topicKey := controller.TopicKey()
	consumerGroup := controller.ConsumerGroup()

	// Get subscription config from registry
	config, ok := m.registry.SubscriptionConfig(topicKey, consumerGroup)
	if !ok {
		return fmt.Errorf("no subscription config for topic key %s, consumer group %s", topicKey, consumerGroup)
	}

	// Get queue for this topic key
	q, ok := m.registry.Queue(topicKey)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topicKey)
	}

	// Resolve the actual topic name for subscribing
	topicName, ok := m.registry.TopicName(topicKey)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topicKey)
	}

	subscriber := q.Subscriber()
	deliveryChan, err := subscriber.Subscribe(ctx, topicName, config)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	// Create cancellable context for this controller
	controllerCtx, cancel := context.WithCancel(ctx)

	// Track active subscription
	done := make(chan struct{})
	sub := &activeSubscription{
		controller: controller,
		cancelFunc: cancel,
		done:       done,
	}
	m.subscriptions[topicKey] = sub

	// Spawn consumption goroutine
	go m.consumeLoop(controllerCtx, controller, deliveryChan, done, config.BatchSize)

	m.logger.Infow("controller started",
		"controller", controller.Name(),
		"topic_key", topicKey,
		"consumer_group", consumerGroup,
	)

	return nil
}

// consumeLoop dispatches deliveries to per-partition worker goroutines.
// Each partition gets its own goroutine, so a slow message on one partition
// does not block other partitions. Per-partition ordering is preserved.
//
// Goroutine model:
//
//	consumeLoop (this goroutine)        ← reads from deliveryChan
//	  ├── processPartition("part-1")    ← spawned lazily on first message
//	  ├── processPartition("part-2")
//	  └── processPartition("part-N")
//
// Shutdown sequence:
//  1. ctx is cancelled (by Stop or parent context)
//  2. consumeLoop exits the select loop and runs the deferred cleanup
//  3. All partition channels are closed, causing processPartition goroutines to
//     drain remaining buffered messages and return (range loop ends)
//  4. wg.Wait() blocks until all partition goroutines have exited
//  5. close(done) signals to unsubscribeAll that this controller is fully stopped
//
// Any messages buffered in partition channels but not processed before ctx
// cancellation are safe to drop — the queue's visibility timeout will make
// them visible again for redelivery (at-least-once semantics).
func (m *consumer) consumeLoop(ctx context.Context, controller Controller, deliveryChan <-chan queue.Delivery, done chan struct{}, batchSize int) {
	defer close(done)

	topicKey := controller.TopicKey()

	controllerScope := m.metricsScope.Tagged(map[string]string{
		"controller": controller.Name(),
		"topic_key":  topicKey.String(),
	})

	m.logger.Debugw("consume loop started",
		"controller", controller.Name(),
		"topic_key", topicKey,
	)

	// partitionChs maps partition keys to per-partition delivery channels.
	// Each channel is created lazily on the first message for that partition
	// and is never removed — partitions are stable for the lifetime of a subscription.
	partitionChs := make(map[string]chan queue.Delivery)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			m.logger.Infow("consume loop stopped",
				"controller", controller.Name(),
				"topic_key", topicKey,
			)
			m.shutdownPartitions(partitionChs, &wg)
			return

		case delivery, ok := <-deliveryChan:
			if !ok {
				m.logger.Infow("delivery channel closed",
					"controller", controller.Name(),
					"topic_key", topicKey,
				)
				m.shutdownPartitions(partitionChs, &wg)
				return
			}

			// Route delivery to its partition's channel, creating the channel
			// and spawning a processPartition goroutine if this is the first
			// message for that partition.
			partitionKey := delivery.Message().PartitionKey
			ch, exists := partitionChs[partitionKey]
			if !exists {
				ch = make(chan queue.Delivery, batchSize)
				partitionChs[partitionKey] = ch
				wg.Add(1)
				go func(pCh <-chan queue.Delivery) {
					defer wg.Done()
					m.processPartition(ctx, controller, pCh, controllerScope)
				}(ch)
			}

			// Send to the partition channel. If ctx is cancelled while the
			// channel buffer is full, we exit — the undelivered message will
			// be retried after visibility timeout.
			select {
			case ch <- delivery:
			case <-ctx.Done():
				m.shutdownPartitions(partitionChs, &wg)
				return
			}
		}
	}
}

// shutdownPartitions closes all partition channels to signal processPartition
// goroutines to exit, then waits for them to finish draining.
func (m *consumer) shutdownPartitions(partitionChs map[string]chan queue.Delivery, wg *sync.WaitGroup) {
	for _, ch := range partitionChs {
		close(ch)
	}
	wg.Wait()
}

// processPartition drains a per-partition channel and processes deliveries serially.
// It runs in its own goroutine (one per partition key). Deliveries within a partition
// are processed in order — the next delivery is not started until the current one
// completes (ack/nack/reject).
//
// The loop exits when either:
//   - deliveryCh is closed (consumeLoop cleanup)
//   - ctx is cancelled (graceful shutdown)
//
// On context cancellation, the current delivery being read from the channel is
// dropped without processing. This is safe because the queue's visibility timeout
// ensures unprocessed messages are redelivered.
func (m *consumer) processPartition(ctx context.Context, controller Controller, deliveryCh <-chan queue.Delivery, scope tally.Scope) {
	for delivery := range deliveryCh {
		select {
		case <-ctx.Done():
			return
		default:
			m.processDelivery(ctx, controller, delivery, scope)
		}
	}
}

// processDelivery calls the controller and performs ack/nack based on the result.
func (m *consumer) processDelivery(ctx context.Context, controller Controller, delivery queue.Delivery, controllerScope tally.Scope) {
	start := time.Now()
	controllerScope.Counter("messages_received").Inc(1)

	msg := delivery.Message()
	topicKey := controller.TopicKey()

	m.logger.Debugw("processing delivery",
		"controller", controller.Name(),
		"topic_key", topicKey,
		"message_id", msg.ID,
		"partition_key", msg.PartitionKey,
		"attempt", delivery.Attempt(),
	)

	// Wrap delivery to hide Ack/Nack from controller
	wrapped := &deliveryWrapper{delivery: delivery}

	// Call controller with wrapped delivery
	err := controller.Process(ctx, wrapped)

	elapsed := time.Since(start)

	// By convention, Controller can only return context.Canceled if it is cancelled by the context, i.e. when consumer is stopped or application is shutting down
	isCanceled := errors.Is(err, context.Canceled)

	// Track latency with success/failure tags
	successTag := "true"
	if err != nil {
		if isCanceled {
			successTag = "cancel"
		} else {
			successTag = "false"
		}
	}

	latencyScope := controllerScope.Tagged(map[string]string{
		"success": successTag,
	})
	latencyScope.Timer("controller_latency").Record(elapsed)

	if err != nil {
		// Check if the error is non-retryable (poison pill message)
		if !errs.IsRetryable(err) {
			m.logger.Errorw("non-retryable controller error, rejecting message",
				"controller", controller.Name(),
				"topic_key", controller.TopicKey(),
				"message_id", msg.ID,
				"partition_key", msg.PartitionKey,
				"attempt", delivery.Attempt(),
				"error", err,
				"elapsed_ms", elapsed.Milliseconds(),
			)

			controllerScope.Counter("non_retryable_errors").Inc(1)

			// Reject moves to DLQ (or acks if DLQ disabled)
			if rejectErr := delivery.Reject(ctx, err.Error()); rejectErr != nil {
				m.logger.Errorw("failed to reject non-retryable message",
					"controller", controller.Name(),
					"topic_key", controller.TopicKey(),
					"message_id", msg.ID,
					"error", rejectErr,
				)
				controllerScope.Counter("reject_errors").Inc(1)
			}
			return
		}

		// Controller returned retryable error - nack message for retry
		// This includes cancelled controllers.
		what := "error"
		if isCanceled {
			what = "cancel"
		}
		m.logger.Errorw("controller error or cancel, nacking message",
			"what", what,
			"controller", controller.Name(),
			"topic_key", topicKey,
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
			"error", err,
			"elapsed_ms", elapsed.Milliseconds(),
		)

		controllerScope.Counter("controller_errors").Inc(1)

		// Nack with no delay - let visibility timeout handle retry delay
		nackStart := time.Now()
		if nackErr := delivery.Nack(ctx, 0); nackErr != nil {
			m.logger.Errorw("failed to nack message",
				"controller", controller.Name(),
				"topic_key", topicKey,
				"message_id", msg.ID,
				"error", nackErr,
			)
			controllerScope.Counter("nack_errors").Inc(1)
		} else {
			controllerScope.Counter("nack_count").Inc(1)
			nackScope := controllerScope.Tagged(map[string]string{
				"operation": "nack",
				"success":   "true",
			})
			nackScope.Timer("ack_nack_latency").Record(time.Since(nackStart))
		}
		return
	}

	// Controller succeeded - ack message
	ackStart := time.Now()
	if ackErr := delivery.Ack(ctx); ackErr != nil {
		m.logger.Errorw("failed to ack message",
			"controller", controller.Name(),
			"topic_key", topicKey,
			"message_id", msg.ID,
			"error", ackErr,
		)
		controllerScope.Counter("ack_errors").Inc(1)
		ackScope := controllerScope.Tagged(map[string]string{
			"operation": "ack",
			"success":   "false",
		})
		ackScope.Timer("ack_nack_latency").Record(time.Since(ackStart))
		return
	}

	controllerScope.Counter("messages_processed").Inc(1)
	controllerScope.Counter("ack_count").Inc(1)

	ackScope := controllerScope.Tagged(map[string]string{
		"operation": "ack",
		"success":   "true",
	})
	ackScope.Timer("ack_nack_latency").Record(time.Since(ackStart))

	m.logger.Debugw("message processed successfully",
		"controller", controller.Name(),
		"topic_key", topicKey,
		"message_id", msg.ID,
		"partition_key", msg.PartitionKey,
		"attempt", delivery.Attempt(),
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// Stop gracefully shuts down all handlers with the specified timeout.
// Cancels all subscription contexts and waits for consumption goroutines to finish.
// timeoutMs is the maximum time in milliseconds to wait for graceful shutdown.
// Returns error if shutdown times out.
// Stop() is not idempotent and can only be called once.
func (m *consumer) Stop(timeoutMs int64) error {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()

	m.logger.Infow("stopping consumer",
		"active_subscriptions", len(m.subscriptions),
		"timeout_ms", timeoutMs,
	)

	err := m.unsubscribeAll(timeoutMs)

	m.logger.Infow("consumer stopped")

	return err
}

// unsubscribeAll cancels all subscription contexts and waits for their consumeLoop
// goroutines to exit.
//
// The timeout budget is shared across all subscriptions — each subscription gets
// the remaining time after the previous one finishes. This ensures Stop() returns
// within the caller's specified timeoutMs even if some controllers are slow to drain.
//
// timeoutMs is the maximum time in milliseconds to wait for all controllers to stop.
// Returns error on timeout, nil on success.
func (m *consumer) unsubscribeAll(timeoutMs int64) error {
	// Cancel all subscription contexts
	for topicKey, sub := range m.subscriptions {
		m.logger.Debugw("stopping controller",
			"controller", sub.controller.Name(),
			"topic_key", topicKey,
		)
		sub.cancelFunc()
	}

	// Wait for each subscription to finish, splitting the timeout budget across them
	remaining := time.Duration(timeoutMs) * time.Millisecond
	var timedOutControllers []string
	for topicKey, sub := range m.subscriptions {
		start := time.Now()
		select {
		case <-sub.done:
			// Controller stopped gracefully
		case <-time.After(remaining):
			m.logger.Errorw("timeout waiting for controller to stop",
				"controller", sub.controller.Name(),
				"topic_key", topicKey,
			)
			timedOutControllers = append(timedOutControllers, sub.controller.Name())
		}
		elapsed := time.Since(start)
		remaining -= elapsed
		if remaining < 0 {
			remaining = 0
		}
	}

	// Clear subscriptions
	m.subscriptions = make(map[TopicKey]*activeSubscription)

	if len(timedOutControllers) > 0 {
		return fmt.Errorf("timeout waiting for controllers to stop: %v", timedOutControllers)
	}

	m.logger.Debugw("all controllers stopped gracefully")
	return nil
}
