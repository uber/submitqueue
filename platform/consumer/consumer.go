// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

const (
	// startupCleanupTimeoutMs is the timeout for cleaning up subscriptions when
	// a controller fails to start during Start().
	startupCleanupTimeoutMs = 30000

	// defaultGateRecheckDelayMs is how long a gate-blocked delivery is
	// postponed before it redelivers and re-checks the gate (milliseconds).
	// It bounds the release latency after a gate opens.
	defaultGateRecheckDelayMs = int64(1000)
)

// Consumer orchestrates multiple queue consumers. It handles subscription lifecycle,
// message consumption, ack/nack, and graceful shutdown for the entire pipeline.
// Start(), Register() and Stop() are always called in this order so they do not need to be concurrently-safe between
// one another, but the implementation must be thread-safe between message processing and Register()/Stop() operations.
type Consumer interface {
	// Register adds a controller to the consumer. Must be called before Start().
	Register(controller Controller) error

	// Start subscribes to all registered controllers' topics and begins consuming messages.
	// ctx governs only the synchronous subscribe calls; consume loops run independently
	// and must be terminated by calling Stop().
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
	processor    errs.ErrorProcessor
	gate         consumergate.Gate

	// gateRecheckDelayMs is how long a gate-blocked delivery is postponed
	// before it redelivers and re-checks the gate. Fixed to
	// defaultGateRecheckDelayMs by New; a field (not the const) so in-package
	// tests can exercise the re-check path quickly.
	gateRecheckDelayMs int64

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
//
// registry provides queue and subscription config for topics. processor is the
// error-classification policy applied exactly once per failing controller
// return — typically errs.NewClassifierProcessor(...) for primary pipeline
// consumers (per-node classifier walk that preserves controller-attached
// framework wraps), or errs.AlwaysRetryableProcessor for narrowly-scoped
// consumers such as DLQ reconciliation that must redeliver on any failure.
// scope is used as provided so wiring can distinguish primary and DLQ consumers
// without introducing duplicate consumer sub-scopes. processor must not be nil;
// callers that genuinely want no transformation can pass
// errs.NewClassifierProcessor() with no classifiers.
//
// gate is the consumer-gate implementation consulted before each delivery
// reaches its controller. Pass noop.New() (from
// platform/extension/consumergate/noop) for services that do not need runtime
// gating. gate must not be nil.
func New(logger *zap.SugaredLogger, scope tally.Scope, registry TopicRegistry, processor errs.ErrorProcessor, gate consumergate.Gate) Consumer {
	return &consumer{
		logger:             logger,
		metricsScope:       scope,
		registry:           registry,
		processor:          processor,
		gate:               gate,
		gateRecheckDelayMs: defaultGateRecheckDelayMs,
		subscriptions:      make(map[TopicKey]*activeSubscription),
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

	// Manage the controller lifecycle independently of the caller's context.
	controllerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

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
//  1. ctx is cancelled (by Stop)
//  2. consumeLoop exits the select loop and runs the deferred cleanup
//  3. All partition channels are closed, causing processPartition goroutines to
//     drain remaining buffered messages and return (range loop ends)
//  4. wg.Wait() blocks until all partition goroutines have exited
//  5. close(done) signals to unsubscribeAll that this controller is fully stopped
//
// Any messages buffered in partition channels but not processed before ctx
// cancellation are safe to drop — the queue's visibility timeout will make
// them visible again for redelivery (at-least-once semantics).
func (m *consumer) consumeLoop(ctx context.Context, controller Controller, deliveryChan <-chan extqueue.Delivery, done chan struct{}, batchSize int) {
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
	partitionChs := make(map[string]chan extqueue.Delivery)
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
				ch = make(chan extqueue.Delivery, batchSize)
				partitionChs[partitionKey] = ch
				wg.Add(1)
				go func(pCh <-chan extqueue.Delivery) {
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
func (m *consumer) shutdownPartitions(partitionChs map[string]chan extqueue.Delivery, wg *sync.WaitGroup) {
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
//   - ctx is cancelled (by Stop)
//
// On context cancellation, the current delivery being read from the channel is
// dropped without processing. This is safe because the queue's visibility timeout
// ensures unprocessed messages are redelivered.
func (m *consumer) processPartition(ctx context.Context, controller Controller, deliveryCh <-chan extqueue.Delivery, scope tally.Scope) {
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
func (m *consumer) processDelivery(ctx context.Context, controller Controller, delivery extqueue.Delivery, controllerScope tally.Scope) {
	const opName = "process"

	// Consumer gate: a delivery whose gate is closed is recorded as parked and
	// postponed (barrier + re-check on redelivery); a false return also covers
	// shutdown-while-checking, where the delivery is left in flight so its
	// visibility lapses into a normal redelivery. Either way there is nothing
	// further to do here. Gate read errors fail open inside checkGate.
	if !m.checkGate(ctx, controller, delivery, controllerScope) {
		return
	}

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
	start := time.Now()
	op := metrics.Begin(controllerScope, opName, metrics.LongLatencyBuckets)
	err := controller.Process(ctx, wrapped)

	elapsed := time.Since(start)

	var completionTags []metrics.Tag
	if err != nil {
		// Single explicit classification pass through the configured
		// ErrorProcessor. Primary consumers use a classifier-based processor
		// (preserves controller framework wraps); DLQ consumers use the
		// always-retryable processor (forces redelivery on any error).
		err = m.processor.Process(err)
		completionTags = controllerClassificationTags(err)
	}

	// Complete only after classification so the finish histogram carries the
	// verdict used for ack/nack/reject behavior.
	op.Complete(err, completionTags...)

	if err != nil {
		// A failure outcome wins over a recorded hold — a hold is only honored
		// on success, so retry accounting and dead-lettering stay meaningful.
		if wrapped.held {
			metrics.NamedCounter(controllerScope, opName, "hold_ignored", 1)
			m.logger.Warnw("hold recorded but controller returned error, failure outcome wins",
				"controller", controller.Name(),
				"topic_key", topicKey,
				"message_id", msg.ID,
				"partition_key", msg.PartitionKey,
			)
		}

		// By convention, Controller can only return context.Canceled if it is
		// cancelled by the processing context during shutdown.
		isCanceled := errors.Is(err, context.Canceled)

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

			// Reject moves to DLQ (or acks if DLQ disabled)
			rejectOp := metrics.Begin(controllerScope, "reject", metrics.StorageLatencyBuckets)
			rejectErr := delivery.Reject(ctx, err.Error())
			rejectOp.Complete(rejectErr)
			if rejectErr != nil {
				m.logger.Errorw("failed to reject non-retryable message",
					"controller", controller.Name(),
					"topic_key", controller.TopicKey(),
					"message_id", msg.ID,
					"error", rejectErr,
				)
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

		// Nack requeues immediately - the visibility timeout spaces retries
		nackOp := metrics.Begin(controllerScope, "nack", metrics.StorageLatencyBuckets)
		nackErr := delivery.Nack(ctx)
		nackOp.Complete(nackErr)
		if nackErr != nil {
			m.logger.Errorw("failed to nack message",
				"controller", controller.Name(),
				"topic_key", topicKey,
				"message_id", msg.ID,
				"error", nackErr,
			)
		}
		return
	}

	// Controller succeeded with a recorded hold - postpone instead of acking.
	// The message redelivers after the delay as a partition barrier, without
	// consuming retry budget. A failed postpone is abandoned like a failed ack:
	// the visibility timeout lapses into a normal redelivery, so the hold
	// loop's liveness never depends on this write succeeding.
	if wrapped.held {
		postponeOp := metrics.Begin(controllerScope, "postpone", metrics.StorageLatencyBuckets)
		postponeErr := delivery.Postpone(ctx, wrapped.holdDelayMs)
		postponeOp.Complete(postponeErr)
		if postponeErr != nil {
			m.logger.Errorw("failed to postpone held message",
				"controller", controller.Name(),
				"topic_key", topicKey,
				"message_id", msg.ID,
				"error", postponeErr,
			)
			return
		}

		m.logger.Debugw("message held, postponed for redelivery",
			"controller", controller.Name(),
			"topic_key", topicKey,
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"delay_ms", wrapped.holdDelayMs,
			"elapsed_ms", elapsed.Milliseconds(),
		)
		return
	}

	// Controller succeeded - ack message
	ackOp := metrics.Begin(controllerScope, "ack", metrics.StorageLatencyBuckets)
	ackErr := delivery.Ack(ctx)
	ackOp.Complete(ackErr)
	if ackErr != nil {
		m.logger.Errorw("failed to ack message",
			"controller", controller.Name(),
			"topic_key", topicKey,
			"message_id", msg.ID,
			"error", ackErr,
		)
		return
	}

	m.logger.Debugw("message processed successfully",
		"controller", controller.Name(),
		"topic_key", topicKey,
		"message_id", msg.ID,
		"partition_key", msg.PartitionKey,
		"attempt", delivery.Attempt(),
		"elapsed_ms", elapsed.Milliseconds(),
	)
}

// checkGate clears a delivery through the consumer gate before it reaches the
// controller. It returns true when the delivery may proceed, false when the
// gate handled it: a blocked delivery is recorded as parked and postponed, so
// the same message redelivers after the re-check delay and re-enters here —
// the gate never waits in memory and never holds a lease.
//
// Failures fail open: if gate state cannot be read, processing proceeds and
// the failure is surfaced via logs and metrics. Park is best-effort (the
// record is observability, never the outcome), and a failed postpone is
// abandoned like a failed ack — the visibility timeout lapses into a normal
// redelivery, which re-checks the gate.
func (m *consumer) checkGate(ctx context.Context, controller Controller, delivery extqueue.Delivery, scope tally.Scope) bool {
	const opName = "gate"

	msg := delivery.Message()
	consumerGroup := controller.ConsumerGroup()
	topic := controller.TopicKey().String()

	entry, err := m.gate.Enter(ctx, consumergate.Key{ConsumerGroup: consumerGroup, PartitionKey: msg.PartitionKey})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Shutting down: leave the delivery in flight; visibility lapses
			// into a normal redelivery.
			return false
		}
		metrics.NamedCounter(scope, opName, "enter_errors", 1)
		m.logger.Errorw("gate check failed, failing open",
			"consumer_group", consumerGroup,
			"topic", topic,
			"message_id", msg.ID,
			"error", err,
		)
		return true
	}

	descriptor := consumergate.DeliveryDescriptor{
		Topic:     topic,
		MessageID: msg.ID,
		Payload:   msg.Payload,
		Attempt:   delivery.Attempt(),
	}

	if !entry.Blocked() {
		// Unconditional best-effort cleanup: if this delivery was parked on an
		// earlier re-check, the gate has opened and the record must go so
		// observers see an empty parked set. A no-op when never parked.
		if unparkErr := entry.Unpark(ctx, descriptor); unparkErr != nil {
			metrics.NamedCounter(scope, opName, "unpark_errors", 1)
			m.logger.Warnw("failed to remove parked record on admit",
				"consumer_group", consumerGroup,
				"topic", topic,
				"message_id", msg.ID,
				"error", unparkErr,
			)
		}
		return true
	}

	// Blocked: record the observation, then postpone the delivery so the
	// partition waits behind it (barrier) and the gate is re-checked on
	// redelivery without burning retry budget.
	if parkErr := entry.Park(ctx, descriptor); parkErr != nil {
		metrics.NamedCounter(scope, opName, "park_errors", 1)
		m.logger.Warnw("failed to write parked record, postponing anyway",
			"consumer_group", consumerGroup,
			"topic", topic,
			"message_id", msg.ID,
			"error", parkErr,
		)
	}

	metrics.NamedCounter(scope, opName, "parked", 1)
	postponeOp := metrics.Begin(scope, "postpone", metrics.StorageLatencyBuckets)
	postponeErr := delivery.Postpone(ctx, m.gateRecheckDelayMs)
	postponeOp.Complete(postponeErr)
	if postponeErr != nil {
		m.logger.Errorw("failed to postpone gated delivery, leaving in flight",
			"consumer_group", consumerGroup,
			"topic", topic,
			"message_id", msg.ID,
			"error", postponeErr,
		)
	}
	return false
}

func controllerClassificationTags(err error) []metrics.Tag {
	origin := "infra"
	if errs.IsRetryable(err) {
		origin = "infra_retryable"
	} else if errs.IsUserError(err) {
		origin = "user"
	}

	dependency := "no"
	if errs.IsDependencyError(err) {
		dependency = "yes"
	}

	return []metrics.Tag{
		metrics.NewTag("origin", origin),
		metrics.NewTag("dependency", dependency),
	}
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
