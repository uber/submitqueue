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

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity/queue"
	extqueue "github.com/uber/submitqueue/extension/queue"
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
)

type subscriber struct {
	logger         *zap.SugaredLogger
	scope          tally.Scope
	messageStore   messageStore
	offsetStore    offsetStore
	leaseStore     partitionLeaseStore
	heartbeatStore subscriberHeartbeatStore
	mu             sync.RWMutex
	closed         bool

	// Active subscriptions
	subscriptions map[string]*subscription
	subMu         sync.Mutex
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
}

// sqlDelivery implements extqueue.Delivery for SQL queue
type sqlDelivery struct {
	msg        queue.Message
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
	msg queue.Message,
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
func (d *sqlDelivery) Message() queue.Message {
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

	// Perform acknowledgment
	if err := d.subscriber.offsetStore.AckMessage(ctx, d.topic, d.partitionKey, d.messageID, d.offset, d.consumerGroup, d.subscriber.messageStore); err != nil {
		return err
	}

	metrics.NamedCounter(d.subscriber.scope, "ack", "messages_acked", 1,
		metrics.Tag{Key: "topic", Value: d.topic},
		metrics.Tag{Key: "partition_key", Value: d.partitionKey},
	)

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

	// Set visibility timeout to make message visible after requeueAfter duration
	if err := d.subscriber.messageStore.SetVisibilityTimeout(ctx, d.topic, d.partitionKey, d.messageID, requeueAfterMillis); err != nil {
		return fmt.Errorf("failed to set visibility timeout for nack of message %s in topic %s partition %s: %w", d.messageID, d.topic, d.partitionKey, err)
	}

	metrics.NamedCounter(d.subscriber.scope, "nack", "messages_nacked", 1,
		metrics.Tag{Key: "topic", Value: d.topic},
		metrics.Tag{Key: "partition_key", Value: d.partitionKey},
	)

	d.subscriber.logger.Infow("message nacked",
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

		// Update offset tracking
		if err := d.subscriber.offsetStore.UpdateAckedOffset(
			ctx, d.topic, d.partitionKey, d.offset, d.consumerGroup,
		); err != nil {
			// Log but don't fail — message is already in DLQ
			d.subscriber.logger.Warnw("failed to update offset after DLQ move",
				"topic", d.topic,
				"message_id", d.messageID,
				"error", err,
			)
			metrics.NamedCounter(d.subscriber.scope, "reject", "offset_update_after_dlq_errors", 1,
				metrics.Tag{Key: "topic", Value: d.topic},
				metrics.Tag{Key: "partition_key", Value: d.partitionKey},
			)
		}

		metrics.NamedCounter(d.subscriber.scope, "reject", "messages_rejected_to_dlq", 1,
			metrics.Tag{Key: "topic", Value: d.topic},
			metrics.Tag{Key: "partition_key", Value: d.partitionKey},
		)
	} else {
		// DLQ disabled — fall back to ack (remove from queue)
		if err := d.subscriber.offsetStore.AckMessage(
			ctx, d.topic, d.partitionKey, d.messageID, d.offset, d.consumerGroup, d.subscriber.messageStore,
		); err != nil {
			return err
		}

		metrics.NamedCounter(d.subscriber.scope, "reject", "messages_rejected_no_dlq", 1,
			metrics.Tag{Key: "topic", Value: d.topic},
			metrics.Tag{Key: "partition_key", Value: d.partitionKey},
		)
	}

	d.acknowledged = true
	return nil
}

// ExtendVisibilityTimeout implements extqueue.Delivery.ExtendVisibilityTimeout
func (d *sqlDelivery) ExtendVisibilityTimeout(ctx context.Context, durationMillis int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	if err := d.subscriber.messageStore.SetVisibilityTimeout(ctx, d.topic, d.partitionKey, d.messageID, durationMillis); err != nil {
		return err
	}

	metrics.NamedCounter(d.subscriber.scope, "extend_visibility", "visibility_extended", 1,
		metrics.Tag{Key: "topic", Value: d.topic},
		metrics.Tag{Key: "partition_key", Value: d.partitionKey},
	)

	return nil
}

func NewSubscriber(logger *zap.SugaredLogger, scope tally.Scope, messageStore messageStore, offsetStore offsetStore, leaseStore partitionLeaseStore, heartbeatStore subscriberHeartbeatStore) *subscriber {
	logger = logger.Named("queue_mysql_subscriber")
	scope = scope.SubScope("queue_mysql_subscriber")
	logger.Info("created subscriber")

	return &subscriber{
		logger:         logger,
		scope:          scope,
		messageStore:   messageStore,
		offsetStore:    offsetStore,
		leaseStore:     leaseStore,
		heartbeatStore: heartbeatStore,
		subscriptions:  make(map[string]*subscription),
	}
}

// Subscribe starts consuming messages from the specified topic
func (s *subscriber) Subscribe(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (_ <-chan extqueue.Delivery, retErr error) {
	op := metrics.Begin(s.scope, "subscribe")
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

	metrics.NamedGauge(s.scope, "subscribe", "active_subscriptions", 1,
		metrics.Tag{Key: "topic", Value: topic},
	)

	// Start the supervisor goroutine. It will discover partitions, acquire
	// leases, and spawn per-partition worker goroutines. The supervisor runs
	// until the subscription context is cancelled (via Close or explicit cancel).
	sub.wg.Add(1)
	go s.managePartitions(subCtx, sub)

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
//	managePartitions (this goroutine)    ← supervisor, tracked by sub.wg
//	  ├── partitionWorker("part-1")     ← tracked by sub.workerWg
//	  ├── partitionWorker("part-2")
//	  └── partitionWorker("part-N")
//
// Shutdown sequence (triggered by ctx cancellation):
//  1. stopAllWorkers: cancels each worker's context and removes from map
//  2. releaseAllLeases: releases DB partition leases (fresh context, not cancelled)
//  3. workerWg.Wait(): blocks until all workers have fully exited — this ensures
//     no worker can send on deliveryCh after step 4
//  4. close(deliveryCh): safe because step 3 guarantees no senders remain
//  5. managePartitions returns → wg.Done() fires → Close() unblocks
func (s *subscriber) managePartitions(ctx context.Context, sub *subscription) {
	defer sub.wg.Done()

	discoveryTicker := time.NewTicker(time.Duration(sub.config.PollIntervalMs) * time.Millisecond)
	defer discoveryTicker.Stop()

	leaseTicker := time.NewTicker(time.Duration(sub.config.LeaseRenewalIntervalMs) * time.Millisecond)
	defer leaseTicker.Stop()

	// Send initial heartbeat so this subscriber is immediately visible to
	// ActiveSubscribers. Without this, other subscribers compute incorrect
	// fair shares until the first leaseTicker fires.
	s.sendHeartbeat(ctx, sub)

	for {
		select {
		case <-ctx.Done():
			s.stopAllWorkers(sub)
			// Release all leases on shutdown with a fresh context
			cleanupCtx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
			defer cancel()
			s.releaseAllLeases(cleanupCtx, sub)
			s.deregisterHeartbeat(cleanupCtx, sub)
			// Wait for all workers to fully exit, then close channel
			sub.workerWg.Wait()
			close(sub.deliveryCh)
			return

		case <-leaseTicker.C:
			s.rebalance(ctx, sub)
			s.renewLeases(ctx, sub)
			s.sendHeartbeat(ctx, sub)

		case <-discoveryTicker.C:
			s.discoverAndReconcileWorkers(ctx, sub)
		}
	}
}

// discoverAndReconcileWorkers discovers new partitions and reconciles workers.
// Uses load-based fair share to limit how many partitions this subscriber acquires.
func (s *subscriber) discoverAndReconcileWorkers(ctx context.Context, sub *subscription) {
	cfg := sub.config

	// Compute fair share cap based on partition weights and active subscribers
	maxPartitions := s.computeMaxPartitions(ctx, sub)

	// Discover and try to acquire leases for new partitions
	acquiredCount, err := s.leaseStore.DiscoverAndAcquirePartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs, maxPartitions)
	if err == nil && acquiredCount > 0 {
		metrics.NamedCounter(s.scope, "discover_and_reconcile", "leases_acquired", int64(acquiredCount),
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
	}

	// Get currently leased partitions
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		s.logger.Warnw("failed to get leased partitions", "topic", sub.topic, "error", err)
		return
	}

	s.reconcilePartitionWorkers(ctx, sub, leasedPartitions)
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
// it fully exits — preventing sends on a closed deliveryCh.
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
	// workerWg still tracks it for shutdown — Close() won't return until it exits.
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
// independently — a slow or blocked partition does not affect other partitions.
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
			w.pollAndDeliver(ctx)
		}
	}
}

// pollAndDeliver fetches messages from this worker's partition and delivers them.
func (w *partitionWorker) pollAndDeliver(ctx context.Context) {
	start := time.Now()
	s := w.subscriber
	sub := w.sub
	cfg := sub.config
	partitionKey := w.partitionKey

	// Initialize offset for this partition once per worker lifetime
	if !w.offsetInitialized {
		if err := s.offsetStore.Initialize(ctx, sub.topic, partitionKey, cfg.ConsumerGroup); err != nil {
			s.logger.Warnw("offset initialization failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
			return
		}
		w.offsetInitialized = true
	}

	// Get current offset for this partition
	currentOffset, err := s.offsetStore.GetAckedOffset(ctx, sub.topic, partitionKey, cfg.ConsumerGroup)
	if err != nil {
		s.logger.Warnw("get current offset failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
		return
	}

	// Fetch messages for this partition
	rows, err := s.messageStore.FetchByOffset(ctx, sub.topic, partitionKey, currentOffset, cfg.BatchSize, cfg.VisibilityTimeoutMs)
	if err != nil {
		s.logger.Warnw("fetch messages failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
		return
	}

	messageCount := 0
	for _, row := range rows {
		// Check if message has exceeded retry limit (persistent retry_count from DB)
		if row.RetryCount >= cfg.Retry.MaxAttempts {
			s.logger.Warnw("message exceeded retry limit",
				"topic", sub.topic,
				"partition_key", partitionKey,
				"message_id", row.ID,
				"retry_count", row.RetryCount,
			)

			// Move to DLQ if enabled
			if cfg.DLQ.Enabled {
				dlqTopic := sub.topic + cfg.DLQ.TopicSuffix
				if err := s.messageStore.MoveToDLQ(ctx, sub.topic, partitionKey, row.ID, row.RetryCount, "exceeded retry limit", cfg.DLQ.TopicSuffix); err != nil {
					s.logger.Warnw("failed to move message to DLQ",
						"topic", sub.topic,
						"dlq_topic", dlqTopic,
						"message_id", row.ID,
						"error", err,
					)
					metrics.NamedCounter(s.scope, "poll_and_deliver", "dlq_move_errors", 1,
						metrics.Tag{Key: "topic", Value: sub.topic},
						metrics.Tag{Key: "partition_key", Value: partitionKey},
					)
				} else {
					s.logger.Infow("moved message to DLQ",
						"topic", sub.topic,
						"dlq_topic", dlqTopic,
						"message_id", row.ID,
						"retry_count", row.RetryCount,
					)
					metrics.NamedCounter(s.scope, "poll_and_deliver", "messages_moved_to_dlq", 1,
						metrics.Tag{Key: "topic", Value: sub.topic},
						metrics.Tag{Key: "partition_key", Value: partitionKey},
					)

					// Update offset since message is now processed (moved to DLQ)
					if err := s.offsetStore.UpdateAckedOffset(ctx, sub.topic, partitionKey, row.Offset, cfg.ConsumerGroup); err != nil {
						s.logger.Warnw("failed to update offset after DLQ move",
							"topic", sub.topic,
							"partition_key", partitionKey,
							"offset", row.Offset,
							"error", err,
						)
						metrics.NamedCounter(s.scope, "poll_and_deliver", "offset_update_after_dlq_errors", 1,
							metrics.Tag{Key: "topic", Value: sub.topic},
							metrics.Tag{Key: "partition_key", Value: partitionKey},
						)
					}
				}
			}
			continue
		}

		// Create message (value type)
		msg := queue.NewMessage(row.ID, row.Payload, row.PartitionKey, row.Metadata)
		msg.PublishedAt = row.PublishedAt

		// Calculate message age for scope
		messageAge := time.Duration(time.Now().UnixMilli()-row.PublishedAt) * time.Millisecond
		metrics.NamedTimer(s.scope, "poll_and_deliver", "message_age", messageAge,
			metrics.Tag{Key: "topic", Value: sub.topic},
			metrics.Tag{Key: "partition_key", Value: partitionKey},
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
			row.RetryCount+1, // RetryCount is 0-based, Attempt is 1-based
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
			return
		}
	}

	// Record scope
	if messageCount > 0 {
		elapsed := time.Since(start)
		tags := []metrics.Tag{
			{Key: "topic", Value: sub.topic},
			{Key: "partition_key", Value: partitionKey},
		}
		metrics.NamedCounter(s.scope, "poll_and_deliver", "messages_received", int64(messageCount), tags...)
		metrics.NamedTimer(s.scope, "poll_and_deliver", "poll_latency", elapsed, tags...)

		s.logger.Debugw("delivered messages",
			"topic", sub.topic,
			"partition_key", partitionKey,
			"count", messageCount,
			"duration_ms", elapsed.Milliseconds(),
		)
	}
}

// renewLeases renews leases for all partitions owned by this worker
func (s *subscriber) renewLeases(ctx context.Context, sub *subscription) {
	cfg := sub.config
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		s.logger.Warnw("failed to get leased partitions for renewal",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "renew_leases", "get_partitions_errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
		return
	}

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.RenewLease(ctx, sub.topic, partitionKey, cfg.SubscriberName, cfg.ConsumerGroup, cfg.LeaseDurationMs); err != nil {
			s.logger.Warnw("failed to renew lease",
				"topic", sub.topic,
				"partition_key", partitionKey,
				"error", err,
			)
			metrics.NamedCounter(s.scope, "renew_leases", "renew_errors", 1,
				metrics.Tag{Key: "topic", Value: sub.topic},
				metrics.Tag{Key: "partition_key", Value: partitionKey},
			)
		}
	}
}

// releaseAllLeases releases all leases for a topic
func (s *subscriber) releaseAllLeases(ctx context.Context, sub *subscription) {
	cfg := sub.config
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		s.logger.Warnw("failed to get leased partitions for release",
			"topic", sub.topic,
			"error", err,
		)
		return
	}

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.ReleaseLease(ctx, sub.topic, partitionKey, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
			s.logger.Warnw("failed to release lease",
				"topic", sub.topic,
				"partition_key", partitionKey,
				"error", err,
			)
			metrics.NamedCounter(s.scope, "release_all_leases", "release_errors", 1,
				metrics.Tag{Key: "topic", Value: sub.topic},
				metrics.Tag{Key: "partition_key", Value: partitionKey},
			)
		}
	}
}

// sendHeartbeat sends a heartbeat for this subscriber.
// ctx is used for cancellation/timeout on the underlying DB call.
// The function does not return an error because heartbeat failure is non-fatal;
// it logs a warning and the next tick will retry.
func (s *subscriber) sendHeartbeat(ctx context.Context, sub *subscription) {
	cfg := sub.config
	if err := s.heartbeatStore.Heartbeat(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
		s.logger.Warnw("failed to send subscriber heartbeat",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "heartbeat", "errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
		return
	}
	metrics.NamedCounter(s.scope, "heartbeat", "sent", 1,
		metrics.Tag{Key: "topic", Value: sub.topic},
	)
}

// deregisterHeartbeat removes this subscriber's heartbeat entry during shutdown.
// ctx is a fresh timeout context created during shutdown cleanup.
func (s *subscriber) deregisterHeartbeat(ctx context.Context, sub *subscription) {
	cfg := sub.config
	if err := s.heartbeatStore.Deregister(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
		s.logger.Warnw("failed to deregister subscriber heartbeat",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "heartbeat", "deregister_errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
		return
	}
	metrics.NamedCounter(s.scope, "heartbeat", "deregistrations", 1,
		metrics.Tag{Key: "topic", Value: sub.topic},
	)
}

// computeMaxPartitions returns the maximum number of partitions this subscriber
// should own, based on even distribution across active subscribers.
//
// The formula is ceil(totalPartitions / activeSubscribers), which ensures all
// partitions are assigned even when they don't divide evenly. At most one
// subscriber will hold one extra partition.
//
// Returns 0 (unlimited) when fair share cannot be computed (error or single subscriber).
func (s *subscriber) computeMaxPartitions(ctx context.Context, sub *subscription) int {
	maxPart, _, err := s.fairShareCap(ctx, sub)
	if err != nil {
		s.logger.Warnw("failed to compute fair share cap",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "fair_share_cap", "errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
		return 0
	}
	return maxPart
}

// rebalance checks if this subscriber holds more partitions than its fair share
// and releases extras so other subscribers can pick them up.
func (s *subscriber) rebalance(ctx context.Context, sub *subscription) {
	cfg := sub.config

	maxPart, owned, err := s.fairShareCap(ctx, sub)
	if err != nil {
		s.logger.Warnw("failed to compute fair share cap for rebalance",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "fair_share_cap", "errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
		return
	}
	if maxPart == 0 || len(owned) <= maxPart {
		return
	}

	// Sort deterministically so the same partitions are released across runs.
	sort.Strings(owned)

	// Release excess partitions
	for _, pk := range owned[maxPart:] {
		if err := s.leaseStore.ReleaseLease(ctx, sub.topic, pk, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
			s.logger.Warnw("failed to release partition during rebalance",
				"topic", sub.topic,
				"partition_key", pk,
				"error", err,
			)
			metrics.NamedCounter(s.scope, "rebalance", "release_errors", 1,
				metrics.Tag{Key: "topic", Value: sub.topic},
				metrics.Tag{Key: "partition_key", Value: pk},
			)
			continue
		}

		// Stop the worker immediately to prevent duplicate processing.
		s.stopPartitionWorker(sub, pk)

		s.logger.Infow("released partition for rebalance",
			"topic", sub.topic,
			"partition_key", pk,
			"owned", len(owned),
			"max_partitions", maxPart,
		)
		metrics.NamedCounter(s.scope, "rebalance", "partitions_released", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
	}
}

// fairShareCap computes the max partitions this subscriber should own.
// Returns (maxPart, ownedPartitions, error). maxPart=0 means unlimited.
func (s *subscriber) fairShareCap(ctx context.Context, sub *subscription) (int, []string, error) {
	cfg := sub.config

	active, err := s.heartbeatStore.ActiveSubscribers(ctx, sub.topic, cfg.ConsumerGroup, cfg.LeaseDurationMs)
	if err != nil {
		return 0, nil, err
	}
	if len(active) <= 1 {
		return 0, nil, nil
	}

	activeSubscribers := len(active)
	metrics.NamedGauge(s.scope, "fair_share_cap", "active_subscribers", float64(activeSubscribers),
		metrics.Tag{Key: "topic", Value: sub.topic},
	)

	owned, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		return 0, nil, err
	}

	// Count all known partitions as the union of owned + discovered.
	// Using max(owned, discovered) would undercount when some partitions
	// have leases but no messages, or vice versa.
	partitionSet := make(map[string]struct{}, len(owned))
	for _, pk := range owned {
		partitionSet[pk] = struct{}{}
	}
	allPartitions, err := s.leaseStore.DiscoverPartitions(ctx, sub.topic)
	if err != nil {
		s.logger.Warnw("failed to discover partitions, using owned partitions only",
			"topic", sub.topic,
			"error", err,
		)
		metrics.NamedCounter(s.scope, "fair_share_cap", "discover_partitions_errors", 1,
			metrics.Tag{Key: "topic", Value: sub.topic},
		)
	} else {
		for _, pk := range allPartitions {
			partitionSet[pk] = struct{}{}
		}
	}
	totalPartitions := len(partitionSet)

	// ceil(totalPartitions / activeSubscribers)
	maxPart := (totalPartitions + activeSubscribers - 1) / activeSubscribers
	if maxPart < 1 {
		maxPart = 1
	}

	return maxPart, owned, nil
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

	s.logger.Info("closing subscriber")

	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Cancel all subscriptions
	for topic, sub := range s.subscriptions {
		s.logger.Debugw("closing subscription", "topic", topic)
		sub.cancelFunc()

		// Wait for the managePartitions goroutine to finish. We wrap the
		// blocking Wait in a goroutine so we can enforce a timeout — if
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
			s.logger.Warnw("subscription shutdown timeout", "topic", topic)
		}

		metrics.NamedGauge(s.scope, "close", "active_subscriptions", 0,
			metrics.Tag{Key: "topic", Value: topic},
		)
	}

	s.subscriptions = make(map[string]*subscription)

	s.closed = true

	s.logger.Info("subscriber closed")
	return nil
}
