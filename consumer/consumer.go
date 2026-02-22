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
	Stop(timeoutMs int64)
}

// consumer implements the Consumer interface.
type consumer struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	queue          queue.Queue
	subscriberName string // Unique worker ID (hostname, pod name)

	mu            sync.Mutex
	controllers   []Controller
	subscriptions map[string]*activeSubscription // topic -> subscription
}

// activeSubscription tracks the state of an active subscription.
type activeSubscription struct {
	controller Controller
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// New creates a new consumer.
// subscriberName is the unique worker identifier used for partition leasing (e.g., hostname, pod name).
func New(logger *zap.SugaredLogger, scope tally.Scope, q queue.Queue, subscriberName string) Consumer {
	return &consumer{
		logger:         logger,
		metricsScope:   scope.SubScope("consumer"),
		queue:          q,
		subscriberName: subscriberName,
		subscriptions:  make(map[string]*activeSubscription),
	}
}

// Register adds a controller to the consumer. Must be called before Start().
// Returns error if a controller for the same topic is already registered.
func (m *consumer) Register(controller Controller) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate topic registration
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
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.controllers) == 0 {
		return fmt.Errorf("no controllers registered")
	}

	m.logger.Infow("starting consumer",
		"subscriber_name", m.subscriberName,
		"controller_count", len(m.controllers),
	)

	for _, controller := range m.controllers {
		if err := m.subscribe(ctx, controller); err != nil {
			// Cleanup any started controllers with short timeout (5 seconds)
			m.unsubscribeAll(5000)
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
	// Get controller's subscription config
	config := controller.SubscriptionConfig(m.subscriberName)

	// Subscribe to topic
	subscriber := m.queue.Subscriber()
	deliveryChan, err := subscriber.Subscribe(ctx, controller.Topic(), config)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	// Create cancellable context for this controller
	controllerCtx, cancel := context.WithCancel(ctx)

	// Track active subscription
	sub := &activeSubscription{
		controller: controller,
		cancelFunc: cancel,
	}
	m.subscriptions[controller.Topic()] = sub

	// Spawn consumption goroutine
	sub.wg.Add(1)
	go m.consumeLoop(controllerCtx, controller, deliveryChan, &sub.wg)

	m.logger.Infow("controller started",
		"controller", controller.Name(),
		"topic", controller.Topic(),
		"consumer_group", controller.ConsumerGroup(),
	)

	return nil
}

// consumeLoop processes deliveries for a controller, calling ack/nack based on controller result.
func (m *consumer) consumeLoop(ctx context.Context, controller Controller, deliveryChan <-chan queue.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	controllerScope := m.metricsScope.Tagged(map[string]string{
		"controller": controller.Name(),
		"topic":      controller.Topic(),
	})

	m.logger.Debugw("consume loop started",
		"controller", controller.Name(),
		"topic", controller.Topic(),
	)

	for {
		select {
		case <-ctx.Done():
			m.logger.Infow("consume loop stopped",
				"controller", controller.Name(),
				"topic", controller.Topic(),
			)
			return

		case delivery, ok := <-deliveryChan:
			if !ok {
				m.logger.Infow("delivery channel closed",
					"controller", controller.Name(),
					"topic", controller.Topic(),
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

	m.logger.Debugw("processing delivery",
		"controller", controller.Name(),
		"topic", controller.Topic(),
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
		// Controller returned error - nack message for retry
		m.logger.Errorw("controller error, nacking message",
			"controller", controller.Name(),
			"topic", controller.Topic(),
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
				"topic", controller.Topic(),
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
			"topic", controller.Topic(),
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
		"topic", controller.Topic(),
		"message_id", msg.ID,
		"partition_key", msg.PartitionKey,
		"attempt", delivery.Attempt(),
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// Stop gracefully shuts down all handlers with the specified timeout.
// Cancels all subscription contexts and waits for consumption goroutines to finish.
// timeoutMs is the maximum time in milliseconds to wait for graceful shutdown.
func (m *consumer) Stop(timeoutMs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Infow("stopping consumer",
		"active_subscriptions", len(m.subscriptions),
		"timeout_ms", timeoutMs,
	)

	m.unsubscribeAll(timeoutMs)

	m.logger.Infow("consumer stopped")
}

// unsubscribeAll stops all active controllers (must be called with lock held).
// timeoutMs is the maximum time in milliseconds to wait for all controllers to stop.
func (m *consumer) unsubscribeAll(timeoutMs int64) {
	// Cancel all subscription contexts
	for topic, sub := range m.subscriptions {
		m.logger.Debugw("stopping controller",
			"controller", sub.controller.Name(),
			"topic", topic,
		)
		sub.cancelFunc()
	}

	// Wait for all consumption goroutines to finish (with timeout)
	done := make(chan struct{})
	go func() {
		for _, sub := range m.subscriptions {
			sub.wg.Wait()
		}
		close(done)
	}()

	timeout := time.Duration(timeoutMs) * time.Millisecond
	select {
	case <-done:
		m.logger.Debugw("all controllers stopped gracefully")
	case <-time.After(timeout):
		m.logger.Errorw("timeout waiting for controllers to stop",
			"timeout_ms", timeoutMs,
		)
	}

	// Clear subscriptions
	m.subscriptions = make(map[string]*activeSubscription)
}
