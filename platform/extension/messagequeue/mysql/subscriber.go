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

package mysql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	"github.com/uber/submitqueue/platform/metrics"
)

const (
	// workerStopTimeout is the maximum time to wait for a partition worker to
	// exit after its context is cancelled.
	workerStopTimeout = 30 * time.Second

	// leaseReleaseTimeout is the timeout for releasing partition leases during
	// shutdown. Uses a fresh context since the subscription context is cancelled.
	leaseReleaseTimeout = 30 * time.Second

	// subscriptionShutdownTimeout is the maximum time to wait for the
	// managePartitions goroutine to finish during Close().
	subscriptionShutdownTimeout = 30 * time.Second

	// watermarkAdvancementLimit is the max number of message offsets fetched per
	// advanceWatermark call. Watermark advancement is incremental and idempotent,
	// so it converges over multiple calls even with large backlogs.
	watermarkAdvancementLimit = 1000

	// gcIdleTickInterval controls how often GC runs during idle poll ticks.
	// GC runs every Nth idle tick instead of every tick to avoid excessive
	// queries when many partitions are idle (e.g., 50 idle partitions at 100ms
	// poll interval = 500 GC queries/sec without throttling).
	gcIdleTickInterval = 100
)

// HookSignal identifies the type of subscriber lifecycle event.
// Named after behavioral concerns (what happened) rather than implementation
// details (which loop ran), so signal names remain stable across refactors.
type HookSignal int

const (
	// SignalDeliveryCheck is sent after the subscriber checks a partition for
	// deliverable messages (including watermark advancement).
	SignalDeliveryCheck HookSignal = iota

	// SignalPartitionUpdate is sent after the subscriber evaluates partition
	// ownership (discovery, rebalance, lease renewal, heartbeat).
	SignalPartitionUpdate
)

type subscriber struct {
	logger             *zap.SugaredLogger
	scope              tally.Scope
	messageStore       messageStore
	offsetStore        offsetStore
	leaseStore         partitionLeaseStore
	heartbeatStore     subscriberHeartbeatStore
	deliveryStateStore deliveryStateStore
	mu                 sync.RWMutex
	closed             bool

	// Active subscriptions
	subscriptions map[string]*subscription
	subMu         sync.Mutex

	// OnSignal receives typed lifecycle signals. Nil in production.
	// Consumers filter by signal type to wait for specific events.
	OnSignal chan HookSignal
}

type subscription struct {
	topic      string
	config     extqueue.SubscriptionConfig
	deliveryCh chan extqueue.Delivery
	cancelFunc context.CancelFunc

	// wg tracks the single managePartitions supervisor goroutine.
	// Close() waits on this to know the entire subscription is shut down.
	wg sync.WaitGroup

	// workerWg tracks all partition worker goroutines independently of wg.
	// During shutdown, managePartitions waits on workerWg before closing
	// deliveryCh to guarantee no worker can send on a closed channel.
	workerWg sync.WaitGroup

	// workers maps partition keys to their active worker goroutines.
	// Only accessed by the managePartitions goroutine for reads/reconciliation,
	// but mutations are protected by workersMu since stopPartitionWorker may
	// be called during shutdown.
	workers   map[string]*partitionWorker
	workersMu sync.Mutex

	// lastDiscoveredPartitions is cached from the most recent
	// DiscoverAndAcquirePartitions call. Used by fairShareCap during
	// rebalance to avoid a redundant discovery query.
	lastDiscoveredPartitions []string
}

// partitionWorker handles polling and delivering messages for a single partition.
// Each worker runs in its own goroutine, polling the DB on a ticker and sending
// deliveries to the shared deliveryCh.
type partitionWorker struct {
	partitionKey string
	sub          *subscription
	subscriber   *subscriber
	// cancelFunc cancels this worker's context, causing run() to exit.
	cancelFunc context.CancelFunc
	// done is closed when run() returns, signaling the worker has fully stopped.
	done chan struct{}
	// offsetInitialized tracks whether the offset has been initialized for this
	// partition. Set once on the first successful poll, avoiding repeated
	// initialization calls on every tick.
	offsetInitialized bool
	// gcCounter counts idle poll ticks. GC only runs every gcIdleTickInterval
	// ticks to avoid excessive queries when many partitions are idle.
	gcCounter int
}

// sqlDelivery implements extqueue.Delivery for SQL queue
type sqlDelivery struct {
	msg        entityqueue.Message
	deliveryID string
	attempt    int
	receivedAt int64
	metadata   map[string]string

	// Backend-specific fields for ack/nack
	subscriber    *subscriber
	topic         string
	partitionKey  string
	offset        int64
	messageID     string
	consumerGroup string

	// DLQ configuration for Reject
	dlqConfig extqueue.DLQConfig

	// Track acknowledgment state
	mu           sync.Mutex
	acknowledged bool
}

func newSQLDelivery(
	msg entityqueue.Message,
	deliveryID string,
	attempt int,
	metadata map[string]string,
	subscriber *subscriber,
	topic string,
	partitionKey string,
	offset int64,
	messageID string,
	consumerGroup string,
	dlqConfig extqueue.DLQConfig,
) *sqlDelivery {
	return &sqlDelivery{
		msg:           msg,
		deliveryID:    deliveryID,
		attempt:       attempt,
		receivedAt:    time.Now().UnixMilli(),
		metadata:      metadata,
		subscriber:    subscriber,
		topic:         topic,
		partitionKey:  partitionKey,
		offset:        offset,
		messageID:     messageID,
		consumerGroup: consumerGroup,
		dlqConfig:     dlqConfig,
		acknowledged:  false,
	}
}

// Message implements extqueue.Delivery.Message
func (d *sqlDelivery) Message() entityqueue.Message {
	return d.msg
}

// DeliveryID implements extqueue.Delivery.DeliveryID
func (d *sqlDelivery) DeliveryID() string {
	return d.deliveryID
}

// Attempt implements extqueue.Delivery.Attempt
func (d *sqlDelivery) Attempt() int {
	return d.attempt
}

// ReceivedAt implements extqueue.Delivery.ReceivedAt
func (d *sqlDelivery) ReceivedAt() int64 {
	return d.receivedAt
}

// Metadata implements extqueue.Delivery.Metadata
func (d *sqlDelivery) Metadata() map[string]string {
	return d.metadata
}

// Ack implements extqueue.Delivery.Ack
func (d *sqlDelivery) Ack(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	// Mark as acked in delivery state (per consumer group).
	// Watermark advancement is deferred to the poll loop to reduce per-ack
	// latency from 4-5 DB round trips to 1.
	if err := d.subscriber.deliveryStateStore.MarkAcked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset); err != nil {
		return err
	}

	d.acknowledged = true
	return nil
}

// Nack implements extqueue.Delivery.Nack
func (d *sqlDelivery) Nack(ctx context.Context, requeueAfterMillis int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	// Mark as nacked in delivery state (per consumer group, with delay and retry_count)
	if err := d.subscriber.deliveryStateStore.MarkNacked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset, requeueAfterMillis); err != nil {
		return err
	}

	d.subscriber.logger.Debugw("message nacked",
		"topic", d.topic,
		"partition_key", d.partitionKey,
		"message_id", d.messageID,
		"requeue_after_millis", requeueAfterMillis,
	)

	d.acknowledged = true
	return nil
}

// Reject implements extqueue.Delivery.Reject
func (d *sqlDelivery) Reject(ctx context.Context, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	if d.dlqConfig.Enabled {
		// Move to DLQ
		if err := d.subscriber.messageStore.MoveToDLQ(
			ctx, d.topic, d.partitionKey, d.messageID, d.attempt, reason, d.dlqConfig.TopicSuffix,
		); err != nil {
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}

		// Mark as acked in delivery state. Watermark advancement is deferred
		// to the poll loop, same as Ack.
		if err := d.subscriber.deliveryStateStore.MarkAcked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset); err != nil {
			return fmt.Errorf("mark acked after DLQ move: %w", err)
		}

	} else {
		// DLQ disabled — mark as acked (remove from processing).
		// Watermark advancement is deferred to the poll loop, same as Ack.
		if err := d.subscriber.deliveryStateStore.MarkAcked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset); err != nil {
			return err
		}

	}

	d.acknowledged = true
	return nil
}

// ExtendVisibilityTimeout implements extqueue.Delivery.ExtendVisibilityTimeout
func (d *sqlDelivery) ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return fmt.Errorf("delivery %s already acknowledged, cannot extend visibility timeout", d.deliveryID)
	}

	// Extend visibility without incrementing retry_count
	if err := d.subscriber.deliveryStateStore.ExtendVisibility(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset, durationMillis); err != nil {
		return err
	}

	return nil
}

func NewSubscriber(logger *zap.SugaredLogger, scope tally.Scope, messageStore messageStore, offsetStore offsetStore, leaseStore partitionLeaseStore, heartbeatStore subscriberHeartbeatStore, deliveryStateStore deliveryStateStore) *subscriber {
	return &subscriber{
		logger:             logger.Named("subscriber"),
		scope:              scope.SubScope("subscriber"),
		messageStore:       messageStore,
		offsetStore:        offsetStore,
		leaseStore:         leaseStore,
		heartbeatStore:     heartbeatStore,
		deliveryStateStore: deliveryStateStore,
		subscriptions:      make(map[string]*subscription),
	}
}

// emitSignal sends a signal on OnSignal if set. Blocks until the signal is
// received, allowing tests to synchronize by controlling when signals are drained.
// Production code does not set OnSignal, so this is a no-op outside tests.
func (s *subscriber) emitSignal(sig HookSignal) {
	if ch := s.OnSignal; ch != nil {
		ch <- sig
	}
}

// advanceWatermark advances offset_acked to the highest contiguous acked offset.
// All operations are idempotent — safe to call from multiple paths (Reject, retry-limit,
// poll loop) and safe to retry on failure.
func (s *subscriber) advanceWatermark(ctx context.Context, consumerGroup, topic, partitionKey string) error {
	currentOffset, err := s.offsetStore.GetAckedOffset(ctx, topic, partitionKey, consumerGroup)
	if err != nil {
		return fmt.Errorf("get acked offset for watermark advance: %w", err)
	}

	offsets, err := s.messageStore.GetOffsetsAbove(ctx, topic, partitionKey, currentOffset, watermarkAdvancementLimit)
	if err != nil {
		return fmt.Errorf("get message offsets for watermark advance: %w", err)
	}

	newWatermark, err := s.deliveryStateStore.AdvanceWatermark(ctx, consumerGroup, topic, partitionKey, currentOffset, offsets)
	if err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}

	if newWatermark > currentOffset {
		if err := s.offsetStore.UpdateAckedOffset(ctx, topic, partitionKey, newWatermark, consumerGroup); err != nil {
			return fmt.Errorf("update acked offset after watermark advance: %w", err)
		}
	}
	return nil
}

// Subscribe starts consuming messages from the specified topic
func (s *subscriber) Subscribe(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (_ <-chan extqueue.Delivery, retErr error) {
	op := metrics.Begin(s.scope, "subscribe", metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return nil, ErrSubscriberClosed
	}

	// Create subscription key (topic + consumer group must be unique)
	subKey := topic + ":" + config.ConsumerGroup

	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Check if already subscribed
	if sub, exists := s.subscriptions[subKey]; exists {
		s.logger.Debugw("reusing existing subscription", "topic", topic, "consumer_group", config.ConsumerGroup)
		return sub.deliveryCh, nil
	}

	s.logger.Infow("creating new subscription",
		"topic", topic,
		"consumer_group", config.ConsumerGroup,
		"subscriber_name", config.SubscriberName,
		"poll_interval_ms", config.PollIntervalMs,
		"batch_size", config.BatchSize,
	)

	// Create new subscription
	// Use a cancellable context for managing the subscription lifecycle
	// and close will cancel the context to signal goroutines to stop
	subCtx, cancel := context.WithCancel(context.Background())
	sub := &subscription{
		topic:      topic,
		config:     config,
		deliveryCh: make(chan extqueue.Delivery, config.BatchSize*2),
		cancelFunc: cancel,
		workers:    make(map[string]*partitionWorker),
	}

	s.subscriptions[subKey] = sub

	// Track active subscription
	metrics.NamedGauge(s.scope, "subscribe", "active_subscriptions", 1, metrics.NewTag("topic", topic))

	// Start the supervisor goroutine. It will discover partitions, acquire
	// leases, and spawn per-partition worker goroutines. The supervisor runs
	// until the subscription context is cancelled (via Close or explicit cancel).
	sub.wg.Add(1)
	go s.managePartitions(subCtx, sub)

	s.logger.Debugw("subscription created", "topic", topic, "consumer_group", config.ConsumerGroup, "subscriber_name", config.SubscriberName)
	return sub.deliveryCh, nil
}

// managePartitions is the supervisor goroutine. It discovers partitions, reconciles
// workers, and renews leases. Each partition gets its own worker goroutine.
//
// There is exactly one managePartitions goroutine per subscription, started by
// Subscribe(). It is the only goroutine that calls reconcilePartitionWorkers,
// so no concurrent reconciliation can occur.
//
// Goroutine hierarchy:
//
//	managePartitions (this goroutine)    <- supervisor, tracked by sub.wg
//	  +-- partitionWorker("part-1")     <- tracked by sub.workerWg
//	  +-- partitionWorker("part-2")
//	  +-- partitionWorker("part-N")
//
// Shutdown sequence (triggered by ctx cancellation):
//  1. stopAllWorkers: cancels each worker's context and removes from map
//  2. releaseAllLeases: releases DB partition leases (fresh context, not cancelled)
//  3. workerWg.Wait(): blocks until all workers have fully exited -- this ensures
//     no worker can send on deliveryCh after step 4
//  4. close(deliveryCh): safe because step 3 guarantees no senders remain
//  5. managePartitions returns -> wg.Done() fires -> Close() unblocks
func (s *subscriber) managePartitions(ctx context.Context, sub *subscription) {
	defer sub.wg.Done()

	cfg := sub.config
	// Common log fields for all operations in this subscription's lifecycle.
	logFields := []interface{}{
		"topic", sub.topic,
		"consumer_group", cfg.ConsumerGroup,
		"subscriber_name", cfg.SubscriberName,
	}

	discoveryTicker := time.NewTicker(time.Duration(cfg.PollIntervalMs) * time.Millisecond)
	defer discoveryTicker.Stop()

	leaseTicker := time.NewTicker(time.Duration(cfg.LeaseRenewalIntervalMs) * time.Millisecond)
	defer leaseTicker.Stop()

	// Send initial heartbeat so this subscriber is immediately visible to
	// ActiveSubscribers. Without this, other subscribers compute incorrect
	// fair shares until the first leaseTicker fires.
	// Initial heartbeat failure is non-fatal — the next leaseTicker fires within
	// LeaseRenewalIntervalMs and retries.
	if err := s.sendHeartbeat(ctx, sub); err != nil {
		s.logger.Errorw("initial heartbeat failed", append(logFields, "error", err)...)
	}

	for {
		select {
		case <-ctx.Done():
			s.stopAllWorkers(sub)
			// Release all leases on shutdown with a fresh context
			cleanupCtx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
			defer cancel()

			// Best-effort shutdown cleanup — log errors but don't block shutdown.
			// Leases expire naturally after LeaseDurationMs if release fails.
			// Heartbeat becomes stale after the same duration.
			if err := s.releaseAllLeases(cleanupCtx, sub); err != nil {
				s.logger.Errorw("failed to release leases during shutdown", append(logFields, "error", err)...)
			}
			if err := s.deregisterHeartbeat(cleanupCtx, sub); err != nil {
				s.logger.Errorw("failed to deregister heartbeat during shutdown", append(logFields, "error", err)...)
			}

			// Wait for all workers to fully exit, then close channel
			sub.workerWg.Wait()
			close(sub.deliveryCh)
			return

		case <-leaseTicker.C:
			// Fetch leased partitions once for this tick — shared by rebalance
			// and renewLeases to avoid redundant queries.
			leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
			if err != nil {
				s.logger.Errorw("get leased partitions failed", append(logFields, "error", err)...)
				// Skip rebalance+renew on this tick; retry next tick.
				if err := s.sendHeartbeat(ctx, sub); err != nil {
					s.logger.Errorw("heartbeat failed during lease error recovery", append(logFields, "error", err)...)
				}
				s.emitSignal(SignalPartitionUpdate)
				continue
			}

			// Rebalance, renew, and heartbeat are independent operations.
			// Each can fail without affecting the others — the next tick retries.
			if err := s.rebalance(ctx, sub, leasedPartitions); err != nil {
				s.logger.Errorw("rebalance failed", append(logFields, "error", err)...)
			}
			if err := s.renewLeases(ctx, sub, leasedPartitions); err != nil {
				s.logger.Errorw("lease renewal failed", append(logFields, "error", err)...)
			}
			if err := s.sendHeartbeat(ctx, sub); err != nil {
				s.logger.Errorw("periodic heartbeat failed", append(logFields, "error", err)...)
			}
			s.emitSignal(SignalPartitionUpdate)

		case <-discoveryTicker.C:
			if err := s.discoverAndReconcileWorkers(ctx, sub); err != nil {
				s.logger.Errorw("partition discovery failed, will retry on next tick", append(logFields, "error", err)...)
			}
			s.emitSignal(SignalPartitionUpdate)
		}
	}
}

// discoverAndReconcileWorkers discovers new partitions and reconciles workers.
// Uses load-based fair share to limit how many partitions this subscriber acquires.
func (s *subscriber) discoverAndReconcileWorkers(ctx context.Context, sub *subscription) error {
	cfg := sub.config

	// Get current leased partitions for fair share computation.
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get leased partitions: %w", err)
	}

	// Use cached discovered partitions from last tick for fair share cap.
	// On the first tick, lastDiscoveredPartitions is nil → fairShareCap uses
	// only owned partitions, which gives unlimited cap for new subscribers.
	sub.workersMu.Lock()
	cachedDiscovered := sub.lastDiscoveredPartitions
	sub.workersMu.Unlock()

	maxPartitions, err := s.fairShareCap(ctx, sub, leasedPartitions, cachedDiscovered)
	if err != nil {
		return fmt.Errorf("compute fair share cap: %w", err)
	}

	// Discover and try to acquire leases for new partitions.
	// Returns discovered partitions to cache for the next tick.
	_, discoveredPartitions, err := s.leaseStore.DiscoverAndAcquirePartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs, maxPartitions)
	if err != nil {
		return fmt.Errorf("discover and acquire partitions: %w", err)
	}

	// Cache discovered partitions for fairShareCap reuse by rebalance and next tick.
	sub.workersMu.Lock()
	sub.lastDiscoveredPartitions = discoveredPartitions
	sub.workersMu.Unlock()

	// Refresh leased partitions after acquisition (new leases may have been acquired)
	leasedPartitions, err = s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get leased partitions after acquire: %w", err)
	}

	s.reconcilePartitionWorkers(ctx, sub, leasedPartitions)
	return nil
}

// reconcilePartitionWorkers diffs the current set of workers against the current
// set of leases and starts/stops workers to match. This is the core of the
// supervisor's control loop.
//
// Thread safety: only called from the single managePartitions goroutine, so the
// snapshot of workers read under the lock does not change between unlock and the
// subsequent start/stop calls. The lock is held briefly to read state, then
// released before blocking operations (stop may wait up to workerStopTimeout).
func (s *subscriber) reconcilePartitionWorkers(ctx context.Context, sub *subscription, currentLeases []string) {
	leaseSet := make(map[string]struct{}, len(currentLeases))
	for _, pk := range currentLeases {
		leaseSet[pk] = struct{}{}
	}

	sub.workersMu.Lock()

	// Find workers to stop (no longer leased)
	var toStop []string
	for pk := range sub.workers {
		if _, ok := leaseSet[pk]; !ok {
			toStop = append(toStop, pk)
		}
	}

	// Find partitions to start (newly leased)
	var toStart []string
	for _, pk := range currentLeases {
		if _, ok := sub.workers[pk]; !ok {
			toStart = append(toStart, pk)
		}
	}

	sub.workersMu.Unlock()

	// Stop workers for partitions no longer leased
	for _, pk := range toStop {
		s.stopPartitionWorker(sub, pk)
	}

	// Start workers for newly leased partitions
	for _, pk := range toStart {
		s.startPartitionWorker(ctx, sub, pk)
	}
}

// startPartitionWorker creates and starts a worker goroutine for a partition.
// The worker is tracked in sub.workers (for reconciliation) and sub.workerWg
// (for shutdown synchronization).
func (s *subscriber) startPartitionWorker(ctx context.Context, sub *subscription, partitionKey string) {
	workerCtx, cancel := context.WithCancel(ctx)

	w := &partitionWorker{
		partitionKey: partitionKey,
		sub:          sub,
		subscriber:   s,
		cancelFunc:   cancel,
		done:         make(chan struct{}),
	}

	sub.workersMu.Lock()
	sub.workers[partitionKey] = w
	sub.workersMu.Unlock()

	sub.workerWg.Add(1)
	go w.run(workerCtx)

	s.logger.Debugw("started partition worker",
		"topic", sub.topic,
		"partition_key", partitionKey,
	)
}

// stopPartitionWorker cancels a worker's context and removes it from the workers
// map. The worker is removed immediately (before confirming exit) so that
// reconciliation can start a replacement if the lease is re-acquired. The old
// worker's context is cancelled, so its DB calls will fail and it will exit
// imminently. workerWg still tracks the old goroutine, so Close() blocks until
// it fully exits -- preventing sends on a closed deliveryCh.
//
// The select with workerStopTimeout is purely for observability: if the worker
// takes longer than expected to exit, a warning is logged but no action is needed
// since workerWg handles the hard guarantee.
func (s *subscriber) stopPartitionWorker(sub *subscription, partitionKey string) {
	sub.workersMu.Lock()
	w, ok := sub.workers[partitionKey]
	if !ok {
		sub.workersMu.Unlock()
		return
	}
	sub.workersMu.Unlock()

	w.cancelFunc()

	// Always remove from map so reconcile can start a replacement if needed.
	// The old worker's context is cancelled so it will exit imminently.
	// workerWg still tracks it for shutdown -- Close() won't return until it exits.
	sub.workersMu.Lock()
	delete(sub.workers, partitionKey)
	sub.workersMu.Unlock()

	select {
	case <-w.done:
		s.logger.Debugw("stopped partition worker",
			"topic", sub.topic,
			"partition_key", partitionKey,
		)
	case <-time.After(workerStopTimeout):
		s.logger.Warnw("partition worker stop timeout, worker will drain in background",
			"topic", sub.topic,
			"partition_key", partitionKey,
		)
	}
}

// stopAllWorkers stops all partition workers for a subscription.
func (s *subscriber) stopAllWorkers(sub *subscription) {
	sub.workersMu.Lock()
	keys := make([]string, 0, len(sub.workers))
	for pk := range sub.workers {
		keys = append(keys, pk)
	}
	sub.workersMu.Unlock()

	for _, pk := range keys {
		s.stopPartitionWorker(sub, pk)
	}
}

// run is the per-partition goroutine loop. It polls the DB on a ticker and
// sends fetched messages to the shared deliveryCh. Each partition worker runs
// independently -- a slow or blocked partition does not affect other partitions.
//
// Lifecycle:
//   - Started by startPartitionWorker, tracked by sub.workerWg
//   - Stopped when ctx is cancelled (lease lost, shutdown, or explicit stop)
//   - Closing w.done signals stopPartitionWorker that the goroutine has exited
func (w *partitionWorker) run(ctx context.Context) {
	defer close(w.done)
	defer w.sub.workerWg.Done()

	pollTicker := time.NewTicker(time.Duration(w.sub.config.PollIntervalMs) * time.Millisecond)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			// Errors are logged here rather than propagated because run() is a
			// long-lived goroutine on a ticker. There is no upstream caller to
			// return to — the only recovery is to retry on the next tick, which
			// happens automatically. All pollAndDeliver operations are idempotent.
			if err := w.pollAndDeliver(ctx); err != nil {
				w.subscriber.logger.Errorw("poll failed",
					"topic", w.sub.topic,
					"partition_key", w.partitionKey,
					"consumer_group", w.sub.config.ConsumerGroup,
					"subscriber_name", w.sub.config.SubscriberName,
					"error", err,
				)
			}
			w.subscriber.emitSignal(SignalDeliveryCheck)
		}
	}
}

// pollAndDeliver fetches messages from this worker's partition and delivers them.
// Returns an error if any DB operation fails — the caller logs once and the ticker
// retries on the next tick. All operations are idempotent, so retries are safe.
//
// Design note: GetDeliveryState and MarkDelivered are called per-message rather than
// batched. This keeps the store interfaces simple and the delivery logic straightforward.
// Partition leasing guarantees a single writer, so the TOCTOU gap between
// GetDeliveryState and MarkDelivered cannot cause incorrect behavior — no other
// worker can mutate the same (consumer_group, topic, partition_key, offset).
func (w *partitionWorker) pollAndDeliver(ctx context.Context) error {
	start := time.Now()
	s := w.subscriber
	sub := w.sub
	cfg := sub.config
	partitionKey := w.partitionKey

	// Initialize offset for this partition once per worker lifetime
	if !w.offsetInitialized {
		if err := s.offsetStore.Initialize(ctx, sub.topic, partitionKey, cfg.ConsumerGroup); err != nil {
			return fmt.Errorf("initialize offset: %w", err)
		}
		w.offsetInitialized = true
	}

	// Get current offset for this partition
	currentOffset, err := s.offsetStore.GetAckedOffset(ctx, sub.topic, partitionKey, cfg.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get acked offset: %w", err)
	}

	// Fetch messages from immutable log; defer-visible rows (visible_after > now)
	// are skipped at the SQL layer.
	rows, err := s.messageStore.FetchByOffset(ctx, sub.topic, partitionKey, currentOffset, time.Now().UnixMilli(), cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("fetch messages: %w", err)
	}

	messageCount := 0
	for _, row := range rows {
		// Check per-consumer-group deliverability via delivery state.
		// Single query replaces separate IsDeliverable + GetRetryCount calls.
		state, found, err := s.deliveryStateStore.GetDeliveryState(ctx, cfg.ConsumerGroup, sub.topic, partitionKey, row.Offset)
		if err != nil {
			return fmt.Errorf("get delivery state offset=%d: %w", row.Offset, err)
		}

		// Determine deliverability in-memory:
		//   !found → new message, deliverable
		//   state.Acked → already processed, skip
		//   state.InvisibleUntil > now → in-flight or nack delay, skip
		now := time.Now().UnixMilli()
		if found && (state.Acked || state.InvisibleUntil > now) {
			continue
		}

		// Mark as delivered (in-flight) in delivery state.
		// Returns the resulting retry_count, avoiding a separate GetRetryCount call.
		retryCount, err := s.deliveryStateStore.MarkDelivered(ctx, cfg.ConsumerGroup, sub.topic, partitionKey, row.Offset, cfg.VisibilityTimeoutMs)
		if err != nil {
			return fmt.Errorf("mark delivered offset=%d: %w", row.Offset, err)
		}

		// Check if message has exceeded retry limit
		if retryCount >= cfg.Retry.MaxAttempts {
			s.logger.Warnw("message exceeded retry limit",
				"topic", sub.topic,
				"consumer_group", cfg.ConsumerGroup,
				"partition_key", partitionKey,
				"message_id", row.ID,
				"retry_count", retryCount,
			)

			// Move to DLQ if enabled — must succeed before marking acked,
			// otherwise the message is lost from both main queue and DLQ.
			if cfg.DLQ.Enabled {
				if err := s.messageStore.MoveToDLQ(ctx, sub.topic, partitionKey, row.ID, retryCount, "exceeded retry limit", cfg.DLQ.TopicSuffix); err != nil {
					return fmt.Errorf("move to DLQ message=%s: %w", row.ID, err)
				}
			}

			// Mark as acked so watermark can advance past it.
			// Watermark advancement is deferred to the poll loop.
			if err := s.deliveryStateStore.MarkAcked(ctx, cfg.ConsumerGroup, sub.topic, partitionKey, row.Offset); err != nil {
				return fmt.Errorf("mark acked after retry limit message=%s: %w", row.ID, err)
			}
			continue
		}

		// Create message (value type)
		msg := entityqueue.NewMessage(row.ID, row.Payload, row.PartitionKey, row.Metadata)
		msg.PublishedAt = row.PublishedAt

		// Calculate message age for metrics
		messageAge := time.Duration(time.Now().UnixMilli()-row.PublishedAt) * time.Millisecond
		metrics.NamedTimer(s.scope, "poll", "message_age", messageAge,
			metrics.NewTag("topic", sub.topic),
			metrics.NewTag("partition_key", partitionKey),
		)

		// Create delivery ID from offset
		deliveryID := strconv.FormatInt(row.Offset, 10)

		// Create delivery metadata
		deliveryMetadata := map[string]string{
			"topic":         sub.topic,
			"partition_key": partitionKey,
			"offset":        deliveryID,
		}

		// Add DLQ-specific metadata if this is a DLQ message
		if row.FailedAt > 0 {
			deliveryMetadata["dlq.failed_at"] = fmt.Sprintf("%d", row.FailedAt)
		}
		if row.FailureCount > 0 {
			deliveryMetadata["dlq.failure_count"] = fmt.Sprintf("%d", row.FailureCount)
		}
		if row.LastError != "" {
			deliveryMetadata["dlq.last_error"] = row.LastError
		}
		if row.OriginalTopic != "" {
			deliveryMetadata["dlq.original_topic"] = row.OriginalTopic
		}

		// Create SQL delivery implementation
		delivery := newSQLDelivery(
			msg,
			deliveryID,
			retryCount+1, // RetryCount is 0-based, Attempt is 1-based
			deliveryMetadata,
			s,
			sub.topic,
			partitionKey,
			row.Offset,
			row.ID,
			cfg.ConsumerGroup,
			cfg.DLQ,
		)

		// Deliver message
		select {
		case sub.deliveryCh <- delivery:
			messageCount++
		case <-ctx.Done():
			return nil
		}
	}

	// Advance watermark periodically (on every poll tick).
	// This is deferred from Ack() to reduce per-ack latency to 1 DB call.
	// advanceWatermark is idempotent and incremental — safe to call every tick.
	if err := s.advanceWatermark(ctx, cfg.ConsumerGroup, sub.topic, partitionKey); err != nil {
		s.logger.Warnw("watermark advancement failed",
			"topic", sub.topic,
			"partition_key", partitionKey,
			"consumer_group", cfg.ConsumerGroup,
			"error", err,
		)
	}

	// Run GC periodically (throttled to every Nth idle tick)
	if messageCount == 0 {
		w.gcCounter++
		if w.gcCounter >= gcIdleTickInterval {
			w.gcCounter = 0
			if err := w.garbageCollect(ctx); err != nil {
				return fmt.Errorf("garbage collect: %w", err)
			}
		}
	} else {
		w.gcCounter = 0
	}

	// Record poll metrics
	if messageCount > 0 {
		elapsed := time.Since(start)
		metrics.NamedCounter(s.scope, "poll", "messages_delivered", int64(messageCount),
			metrics.NewTag("topic", sub.topic),
			metrics.NewTag("partition_key", partitionKey),
		)
		metrics.NamedTimer(s.scope, "poll", "latency", elapsed,
			metrics.NewTag("topic", sub.topic),
			metrics.NewTag("partition_key", partitionKey),
		)
	}

	return nil
}

// garbageCollect orchestrates GC by querying the offsetStore for the minimum
// acked offset across all consumer groups, then telling the messageStore to
// delete messages up to that offset. This keeps each store querying only its
// own table.
func (w *partitionWorker) garbageCollect(ctx context.Context) error {
	s := w.subscriber

	minOffset, found, err := s.offsetStore.GetMinAckedOffset(ctx, w.sub.topic, w.partitionKey)
	if err != nil {
		return fmt.Errorf("get min acked offset: %w", err)
	}
	if !found {
		return nil
	}

	if _, err := s.messageStore.GarbageCollect(ctx, w.sub.topic, w.partitionKey, minOffset); err != nil {
		return fmt.Errorf("delete messages: %w", err)
	}

	return nil
}

// renewLeases renews leases for all partitions owned by this worker.
func (s *subscriber) renewLeases(ctx context.Context, sub *subscription, leasedPartitions []string) error {
	cfg := sub.config

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.RenewLease(ctx, sub.topic, partitionKey, cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs); err != nil {
			return fmt.Errorf("renew lease partition=%s: %w", partitionKey, err)
		}
	}
	return nil
}

// releaseAllLeases releases all leases for a topic.
func (s *subscriber) releaseAllLeases(ctx context.Context, sub *subscription) error {
	cfg := sub.config
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get leased partitions for release: %w", err)
	}

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.ReleaseLease(ctx, sub.topic, partitionKey, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
			return fmt.Errorf("release lease partition=%s: %w", partitionKey, err)
		}
	}
	return nil
}

// sendHeartbeat sends a heartbeat for this subscriber.
func (s *subscriber) sendHeartbeat(ctx context.Context, sub *subscription) error {
	cfg := sub.config
	if err := s.heartbeatStore.Heartbeat(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// deregisterHeartbeat removes this subscriber's heartbeat entry during shutdown.
func (s *subscriber) deregisterHeartbeat(ctx context.Context, sub *subscription) error {
	cfg := sub.config
	if err := s.heartbeatStore.Deregister(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
		return fmt.Errorf("deregister heartbeat: %w", err)
	}
	return nil
}

// rebalance checks if this subscriber holds more partitions than its fair share
// and releases extras so other subscribers can pick them up.
func (s *subscriber) rebalance(ctx context.Context, sub *subscription, owned []string) error {
	cfg := sub.config

	// Use cached discovered partitions from the most recent discovery tick.
	sub.workersMu.Lock()
	discoveredPartitions := sub.lastDiscoveredPartitions
	sub.workersMu.Unlock()

	maxPart, err := s.fairShareCap(ctx, sub, owned, discoveredPartitions)
	if err != nil {
		return fmt.Errorf("compute fair share cap: %w", err)
	}
	if maxPart == 0 || len(owned) <= maxPart {
		return nil
	}

	// Sort deterministically so the same partitions are released across runs.
	sort.Strings(owned)

	// Release excess partitions
	for _, pk := range owned[maxPart:] {
		if err := s.leaseStore.ReleaseLease(ctx, sub.topic, pk, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
			return fmt.Errorf("release partition %s during rebalance: %w", pk, err)
		}

		// Stop the worker immediately to prevent duplicate processing.
		s.stopPartitionWorker(sub, pk)

		s.logger.Infow("released partition for rebalance",
			"topic", sub.topic,
			"consumer_group", cfg.ConsumerGroup,
			"partition_key", pk,
			"owned", len(owned),
			"max_partitions", maxPart,
		)
	}
	return nil
}

// fairShareCap computes the max partitions this subscriber should own.
// Returns (maxPart, error). maxPart=0 means unlimited.
// owned is the caller-provided list of leased partitions.
// discoveredPartitions is an optional pre-fetched list of all known partitions;
// if nil, only owned partitions are used for fair share computation.
func (s *subscriber) fairShareCap(ctx context.Context, sub *subscription, owned []string, discoveredPartitions []string) (int, error) {
	cfg := sub.config

	active, err := s.heartbeatStore.ActiveSubscribers(ctx, sub.topic, cfg.ConsumerGroup, cfg.LeaseDurationMs)
	if err != nil {
		return 0, err
	}
	if len(active) <= 1 {
		return 0, nil
	}

	activeSubscribers := len(active)

	// Count all known partitions as the union of owned + discovered.
	// Using max(owned, discovered) would undercount when some partitions
	// have leases but no messages, or vice versa.
	partitionSet := make(map[string]struct{}, len(owned))
	for _, pk := range owned {
		partitionSet[pk] = struct{}{}
	}
	if discoveredPartitions != nil {
		for _, pk := range discoveredPartitions {
			partitionSet[pk] = struct{}{}
		}
	}
	totalPartitions := len(partitionSet)

	// ceil(totalPartitions / activeSubscribers)
	maxPart := (totalPartitions + activeSubscribers - 1) / activeSubscribers
	if maxPart < 1 {
		maxPart = 1
	}

	return maxPart, nil
}

// Close gracefully shuts down the subscriber and all its subscriptions.
//
// For each subscription:
//  1. Cancels the subscription context, triggering managePartitions shutdown
//  2. Wraps sub.wg.Wait() in a goroutine with subscriptionShutdownTimeout so
//     Close() does not block indefinitely if a subscription hangs
//  3. managePartitions internally handles stopping workers and closing deliveryCh
//     (see managePartitions shutdown sequence)
func (s *subscriber) Close() (retErr error) {
	op := metrics.Begin(s.scope, "close")
	defer func() { op.Complete(retErr) }()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.logger.Infow("closing subscriber")

	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Cancel all subscriptions
	for _, sub := range s.subscriptions {
		s.logger.Debugw("closing subscription",
			"topic", sub.topic,
			"consumer_group", sub.config.ConsumerGroup,
		)
		sub.cancelFunc()

		// Wait for the managePartitions goroutine to finish. We wrap the
		// blocking Wait in a goroutine so we can enforce a timeout -- if
		// managePartitions is stuck, we log a warning and move on rather
		// than blocking Close() indefinitely.
		done := make(chan struct{})
		go func() {
			sub.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Graceful shutdown completed
		case <-time.After(subscriptionShutdownTimeout):
			s.logger.Warnw("subscription shutdown timeout",
				"topic", sub.topic,
				"consumer_group", sub.config.ConsumerGroup,
			)
		}

		// Update metrics
		metrics.NamedGauge(s.scope, "subscribe", "active_subscriptions", 0, metrics.NewTag("topic", sub.topic))
	}

	s.subscriptions = make(map[string]*subscription)

	s.closed = true

	s.logger.Infow("subscriber closed")
	return nil
}
