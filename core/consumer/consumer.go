package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/extension/queue"
	"go.uber.org/zap"
)

// Consumer orchestrates multiple queue consumers. It handles subscription lifecycle,
// message consumption, ack/nack, and graceful shutdown for the entire pipeline.
type Consumer interface {
	// Register adds a controller to the consumer. Must be called before Start().
	Register(controller Controller) error

	// Start subscribes to all registered controllers' topics and begins consuming messages.
	Start(ctx context.Context) error

	// Stop gracefully shuts down all controllers with the specified timeout.
	// timeoutMs is the maximum time in milliseconds to wait for graceful shutdown.
	// Returns error if shutdown times out.
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
	subscriptions map[Topic]*activeSubscription // topic -> subscription
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
		subscriptions: make(map[Topic]*activeSubscription),
	}
}

// Register adds a controller to the consumer. Must be called before Start().
// Returns error if a controller for the same topic is already registered or if the consumer is stopped.
func (m *consumer) Register(controller Controller) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return fmt.Errorf("consumer is stopped")
	}

	// Check for duplicate topic registration.
	// O(n) scan is fine here — controller count is in the single digits.
	for _, c := range m.controllers {
		if c.Topic() == controller.Topic() {
			return fmt.Errorf("controller for topic %s already registered", controller.Topic())
		}
	}

	m.controllers = append(m.controllers, controller)

	m.logger.Infow("registered controller",
		"controller", controller.Name(),
		"topic", controller.Topic(),
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
			// Cleanup any started controllers with short timeout (30 seconds).
			// Ignore error since we're returning the subscribe error.
			_ = m.unsubscribeAll(30000)
			return fmt.Errorf("failed to start controller %s: %w", controller.Name(), err)
		}
	}

	m.logger.Infow("consumer started",
		"active_subscriptions", len(m.subscriptions),
	)

	return nil
}

// subscribe subscribes a controller to its topic and spawns a consumption goroutine.
func (m *consumer) subscribe(ctx context.Context, controller Controller) error {
	topic := controller.Topic()
	consumerGroup := controller.ConsumerGroup()

	// Get subscription config from registry
	config, ok := m.registry.SubscriptionConfig(topic, consumerGroup)
	if !ok {
		return fmt.Errorf("no subscription config for topic %s, consumer group %s", topic, consumerGroup)
	}

	// Get queue for this topic
	q, ok := m.registry.Queue(topic)
	if !ok {
		return fmt.Errorf("no queue registered for topic %s", topic)
	}

	subscriber := q.Subscriber()
	deliveryChan, err := subscriber.Subscribe(ctx, topic.String(), config)
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
	m.subscriptions[topic] = sub

	// Spawn consumption goroutine
	go m.consumeLoop(controllerCtx, controller, deliveryChan, done)

	m.logger.Infow("controller started",
		"controller", controller.Name(),
		"topic", topic,
		"consumer_group", consumerGroup,
	)

	return nil
}

// consumeLoop processes deliveries for a controller, calling ack/nack based on controller result.
func (m *consumer) consumeLoop(ctx context.Context, controller Controller, deliveryChan <-chan queue.Delivery, done chan struct{}) {
	defer close(done)

	topic := controller.Topic()

	controllerScope := m.metricsScope.Tagged(map[string]string{
		"controller": controller.Name(),
		"topic":      topic.String(),
	})

	m.logger.Debugw("consume loop started",
		"controller", controller.Name(),
		"topic", topic,
	)

	for {
		select {
		case <-ctx.Done():
			m.logger.Infow("consume loop stopped",
				"controller", controller.Name(),
				"topic", topic,
			)
			return

		case delivery, ok := <-deliveryChan:
			if !ok {
				m.logger.Infow("delivery channel closed",
					"controller", controller.Name(),
					"topic", topic,
				)
				return
			}

			m.processDelivery(ctx, controller, delivery, controllerScope)
		}
	}
}

// processDelivery calls the controller and performs ack/nack based on the result.
func (m *consumer) processDelivery(ctx context.Context, controller Controller, delivery queue.Delivery, controllerScope tally.Scope) {
	start := time.Now()
	controllerScope.Counter("messages_received").Inc(1)

	msg := delivery.Message()
	topic := controller.Topic()

	m.logger.Debugw("processing delivery",
		"controller", controller.Name(),
		"topic", topic,
		"message_id", msg.ID,
		"partition_key", msg.PartitionKey,
		"attempt", delivery.Attempt(),
	)

	// Wrap delivery to hide Ack/Nack from controller
	wrapped := &deliveryWrapper{delivery: delivery}

	// Call controller with wrapped delivery
	err := controller.Process(ctx, wrapped)

	elapsed := time.Since(start)

	// Track latency with success/failure tags
	successTag := "true"
	if err != nil {
		successTag = "false"
	}

	latencyScope := controllerScope.Tagged(map[string]string{
		"success": successTag,
	})
	latencyScope.Timer("controller_latency").Record(elapsed)

	if err != nil {
		// Check if the error is non-retryable (poison pill message)
		if IsNonRetryable(err) {
			m.logger.Errorw("non-retryable controller error, rejecting message",
				"controller", controller.Name(),
				"topic", controller.Topic(),
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
					"topic", controller.Topic(),
					"message_id", msg.ID,
					"error", rejectErr,
				)
				controllerScope.Counter("reject_errors").Inc(1)
			}
			return
		}

		// Controller returned retryable error - nack message for retry
		m.logger.Errorw("controller error, nacking message",
			"controller", controller.Name(),
			"topic", topic,
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
				"topic", topic,
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
			"topic", topic,
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
		"topic", topic,
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

// unsubscribeAll stops all active controllers (must be called with lock held).
// timeoutMs is the maximum time in milliseconds to wait for all controllers to stop.
// Returns error on timeout, nil on success.
func (m *consumer) unsubscribeAll(timeoutMs int64) error {
	// Cancel all subscription contexts
	for topic, sub := range m.subscriptions {
		m.logger.Debugw("stopping controller",
			"controller", sub.controller.Name(),
			"topic", topic,
		)
		sub.cancelFunc()
	}

	// Wait for each subscription to finish, splitting the timeout budget across them
	remaining := time.Duration(timeoutMs) * time.Millisecond
	var timedOut bool
	for topic, sub := range m.subscriptions {
		start := time.Now()
		select {
		case <-sub.done:
			// Controller stopped gracefully
		case <-time.After(remaining):
			m.logger.Errorw("timeout waiting for controller to stop",
				"controller", sub.controller.Name(),
				"topic", topic,
			)
			timedOut = true
		}
		elapsed := time.Since(start)
		remaining -= elapsed
		if remaining < 0 {
			remaining = 0
		}
	}

	// Clear subscriptions
	m.subscriptions = make(map[Topic]*activeSubscription)

	if timedOut {
		return fmt.Errorf("timeout waiting for controllers to stop")
	}

	m.logger.Debugw("all controllers stopped gracefully")
	return nil
}
