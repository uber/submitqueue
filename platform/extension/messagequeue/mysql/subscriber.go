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
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/platform/base/failure"
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

	// heartbeatPurgeAfterLeaseDurations sets the age threshold for purging
	// abandoned heartbeat rows, as a multiple of LeaseDurationMs (10x = 5min
	// at defaults). Well past every transient window in the protocol — a row
	// that stale belongs to a subscriber that crashed without deregistering.
	// Purging a live-but-stalled subscriber's row is harmless: its next
	// heartbeat re-inserts it.
	heartbeatPurgeAfterLeaseDurations = 10

	// idleLeaseReleaseAfterLeaseDurations sets how long an owned partition
	// must stay drained (no stored messages, hence absent from discovery)
	// before its lease is released, as a multiple of LeaseDurationMs (2x =
	// 60s at defaults). Long enough to keep ownership sticky across brief
	// quiet spells on stable partition keys; short enough that short-lived
	// keys (one batch, one request) don't hold leases, offset rows, and
	// polling workers forever. A released partition rejoins through normal
	// discovery as soon as a message arrives.
	idleLeaseReleaseAfterLeaseDurations = 2

	// leasePurgeAfterLeaseDurations sets the age threshold for purging
	// abandoned lease rows, as a multiple of LeaseDurationMs. Covers holders
	// that crashed while owning a drained partition: acquisition only probes
	// discovered partitions, so nothing would ever steal (and thereby
	// refresh or remove) a stale lease on a partition with no messages.
	leasePurgeAfterLeaseDurations = 10

	// maxRetryBackoffMs bounds how long one failed message can pin its
	// partition's contiguous ack watermark. Callers may choose a lower cap;
	// this ceiling also applies when MaxBackoffMs is unset.
	maxRetryBackoffMs = int64(time.Minute / time.Millisecond)
)

// gcTickInterval is the number of poll ticks between garbage collection runs.
// GC runs every Nth tick regardless of delivery activity; without throttling,
// many partitions polling in lockstep would flood the store with queries
// (e.g., 50 partitions at 100ms poll interval = 500 GC queries/sec). A var so
// tests can shorten it; production always uses the default.
var gcTickInterval = 100

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

	// done is closed once managePartitions has exited, which implies
	// deliveryCh is already closed. Subscribe consults it to tell a live
	// subscription from one whose ctx was cancelled independently of Close,
	// so it can replace the stale entry instead of handing a new caller a
	// closed channel. A channel rather than a flag: signalling it must not
	// require a lock (see managePartitions).
	done chan struct{}

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

	// drainedSince tracks, per owned partition absent from discovery, when
	// this subscriber first observed it drained (no stored messages left).
	// Drives idle-lease release: partitions drained beyond the grace period
	// are released so fully-consumed short-lived partition keys don't hold
	// leases, offset rows, and polling workers forever. Only accessed by the
	// single managePartitions goroutine — no locking needed.
	drainedSince map[string]time.Time
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
	// gcCounter counts poll ticks since the last garbage collection run.
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

	// retry is the subscription's retry budget. Nack needs it to recognise the
	// attempt that spends the last of it, which is the one delivery still
	// holding the reason the message is about to be dead-lettered for.
	retry extqueue.RetryConfig

	// failure is why this message was dead-lettered, reassembled from the row.
	// Only meaningful when failed is set, i.e. when this is a redelivery from
	// a DLQ topic.
	failure failure.Failure
	// failed records whether this message arrived from a DLQ topic.
	failed bool

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
	retry extqueue.RetryConfig,
	f failure.Failure,
	failed bool,
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
		retry:         retry,
		failure:       f,
		failed:        failed,
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
//
// When this attempt has spent the retry budget, the message is dead-lettered
// here rather than nacked, so it carries f — the reason it actually failed.
// The poll loop would otherwise pick it up on the next round and dead-letter
// it with a generic reason, f having been discarded when this delivery ended.
// That path still exists as a backstop for a delivery that never reaches Nack
// at all (a crash, or a missed ack redelivered by the visibility timeout); it
// is just no longer the common one.
//
// Message count is unchanged: at attempt N the next poll would see retry_count
// = N and dead-letter iff N >= MaxAttempts, which is exactly the condition
// below.
func (d *sqlDelivery) Nack(ctx context.Context, f failure.Failure) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	if d.retry.MaxAttempts > 0 && d.attempt >= d.retry.MaxAttempts {
		d.subscriber.logger.Warnw("message exhausted retry budget, dead-lettering",
			"topic", d.topic,
			"partition_key", d.partitionKey,
			"message_id", d.messageID,
			"attempt", d.attempt,
			"max_attempts", d.retry.MaxAttempts,
			"reason", f.Message,
		)
		return d.deadLetter(ctx, f)
	}

	retryDelayMs := retryBackoffMs(d.retry, d.attempt)
	if err := d.subscriber.deliveryStateStore.MarkNacked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset, retryDelayMs); err != nil {
		return err
	}

	d.subscriber.logger.Debugw("message nacked",
		"topic", d.topic,
		"partition_key", d.partitionKey,
		"message_id", d.messageID,
		"retry_delay_ms", retryDelayMs,
	)

	d.acknowledged = true
	return nil
}

// Postpone implements extqueue.Delivery.Postpone
func (d *sqlDelivery) Postpone(ctx context.Context, delayMs int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	// Mark as postponed in delivery state (per consumer group): invisible for
	// the delay, retry_count reset, partition barrier until redelivery.
	if err := d.subscriber.deliveryStateStore.MarkPostponed(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset, delayMs); err != nil {
		return err
	}

	d.subscriber.logger.Debugw("message postponed",
		"topic", d.topic,
		"partition_key", d.partitionKey,
		"message_id", d.messageID,
		"delay_millis", delayMs,
	)

	d.acknowledged = true
	return nil
}

// Reject implements extqueue.Delivery.Reject
func (d *sqlDelivery) Reject(ctx context.Context, f failure.Failure) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.acknowledged {
		return &ErrAlreadyAcknowledged{DeliveryID: d.deliveryID}
	}

	return d.deadLetter(ctx, f)
}

// deadLetter ends this delivery by moving the message to the DLQ with f
// recorded against it, or by simply acking when no DLQ is configured — there
// is nowhere else to put it, and leaving it would redeliver forever.
//
// Callers hold d.mu.
func (d *sqlDelivery) deadLetter(ctx context.Context, f failure.Failure) error {
	if d.dlqConfig.Enabled {
		// Move to DLQ
		if err := d.subscriber.messageStore.MoveToDLQ(
			ctx, d.topic, d.partitionKey, d.messageID, d.attempt, f, d.dlqConfig.TopicSuffix,
		); err != nil {
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}
	}

	// Mark as acked in delivery state. Watermark advancement is deferred
	// to the poll loop, same as Ack.
	if err := d.subscriber.deliveryStateStore.MarkAcked(ctx, d.consumerGroup, d.topic, d.partitionKey, d.offset); err != nil {
		return fmt.Errorf("mark acked after DLQ move: %w", err)
	}

	d.acknowledged = true
	return nil
}

// Failure implements extqueue.Delivery.Failure
func (d *sqlDelivery) Failure() (failure.Failure, bool) {
	return d.failure, d.failed
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

// Subscribe starts consuming messages from the specified topic. The returned
// channel is closed when ctx is cancelled or Close is called, whichever comes
// first. Consumption that must outlive a request-scoped ctx and be torn down
// only by Close therefore needs a detached context (context.WithoutCancel).
func (s *subscriber) Subscribe(ctx context.Context, topic string, config extqueue.SubscriptionConfig) (_ <-chan extqueue.Delivery, retErr error) {
	op := metrics.Begin(s.scope, "subscribe", metrics.StorageLatencyBuckets, metrics.NewTag("topic", topic))
	defer func() { op.Complete(retErr) }()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		return nil, ErrSubscriberClosed
	}
	if err := validateRetryConfig(config.Retry); err != nil {
		return nil, fmt.Errorf("subscribe topic %q: %w: %v", topic, ErrInvalidConfig, err)
	}

	// Create subscription key (topic + consumer group must be unique)
	subKey := topic + ":" + config.ConsumerGroup

	s.subMu.Lock()
	defer s.subMu.Unlock()

	// A subscription whose supervisor already exited (its ctx was cancelled
	// without Close, which would have cleared the map) has a closed
	// deliveryCh. Evicting it here and falling through to build a fresh one
	// keeps that closed channel from being handed to the next caller.
	if sub, exists := s.subscriptions[subKey]; exists {
		select {
		case <-sub.done:
			delete(s.subscriptions, subKey)
		default:
			s.logger.Debugw("reusing existing subscription", "topic", topic, "consumer_group", config.ConsumerGroup)
			return sub.deliveryCh, nil
		}
	}

	s.logger.Infow("creating new subscription",
		"topic", topic,
		"consumer_group", config.ConsumerGroup,
		"subscriber_name", config.SubscriberName,
		"poll_interval_ms", config.PollIntervalMs,
		"batch_size", config.BatchSize,
	)

	// Derived from the caller's ctx, so cancelling ctx tears this subscription
	// down exactly as Close does. Close cancels subCtx directly and so remains
	// effective for a caller whose ctx never completes.
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscription{
		topic:      topic,
		config:     config,
		deliveryCh: make(chan extqueue.Delivery, config.BatchSize*2),
		cancelFunc: cancel,
		done:       make(chan struct{}),
		workers:    make(map[string]*partitionWorker),
	}

	s.subscriptions[subKey] = sub

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
//  5. managePartitions returns -> done and wg.Done() fire -> Close() unblocks
func (s *subscriber) managePartitions(ctx context.Context, sub *subscription) {
	defer sub.wg.Done()
	// Deferred so every exit path marks the subscription stale for Subscribe.
	// Must not take s.subMu: Close holds it across cancelFunc()+wg.Wait() for
	// every subscription, so locking here would deadlock against the very
	// Close that triggered this shutdown.
	defer close(sub.done)

	cfg := sub.config
	// Common log fields for all operations in this subscription's lifecycle.
	logFields := []interface{}{
		"topic", sub.topic,
		"consumer_group", cfg.ConsumerGroup,
		"subscriber_name", cfg.SubscriberName,
	}

	discoveryTicker := time.NewTicker(time.Duration(cfg.PartitionDiscoveryIntervalMs) * time.Millisecond)
	defer discoveryTicker.Stop()

	leaseTicker := time.NewTicker(time.Duration(cfg.LeaseRenewalIntervalMs) * time.Millisecond)
	defer leaseTicker.Stop()

	// Orphan sweep pacing: an uncapped acquisition pass runs every two lease
	// durations. Two lease durations is past every transient window in the
	// protocol — an expiring lease, a crashed peer's heartbeat going stale —
	// so anything still unleased at sweep time is genuinely unclaimed.
	orphanSweepInterval := 2 * time.Duration(cfg.LeaseDurationMs) * time.Millisecond
	lastOrphanSweep := time.Now()

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
			// Renewal covers only the partitions kept after shedding; renewing
			// a just-released lease would spuriously fail with ErrLeaseExpired.
			released, err := s.rebalance(ctx, sub, leasedPartitions)
			if err != nil {
				s.logger.Errorw("rebalance failed", append(logFields, "error", err)...)
			}
			kept := leasedPartitions
			if len(released) > 0 {
				releasedSet := make(map[string]struct{}, len(released))
				for _, pk := range released {
					releasedSet[pk] = struct{}{}
				}
				kept = make([]string, 0, len(leasedPartitions))
				for _, pk := range leasedPartitions {
					if _, ok := releasedSet[pk]; !ok {
						kept = append(kept, pk)
					}
				}
			}
			if err := s.renewLeases(ctx, sub, kept); err != nil {
				s.logger.Errorw("lease renewal failed", append(logFields, "error", err)...)
			}
			if err := s.sendHeartbeat(ctx, sub); err != nil {
				s.logger.Errorw("periodic heartbeat failed", append(logFields, "error", err)...)
			}
			// Purge heartbeat rows abandoned by subscribers that never
			// deregistered (crashes) — without this the table grows
			// monotonically, since every process registers under a fresh
			// hostname-pid name.
			if err := s.heartbeatStore.PurgeStale(ctx, sub.topic, cfg.ConsumerGroup, heartbeatPurgeAfterLeaseDurations*cfg.LeaseDurationMs); err != nil {
				s.logger.Errorw("stale heartbeat purge failed", append(logFields, "error", err)...)
			}
			// Purge lease rows abandoned by holders that crashed while
			// owning a drained partition — acquisition only probes
			// discovered partitions, so nothing else ever refreshes or
			// removes a stale lease on a partition with no messages.
			if err := s.leaseStore.PurgeStale(ctx, sub.topic, cfg.ConsumerGroup, leasePurgeAfterLeaseDurations*cfg.LeaseDurationMs); err != nil {
				s.logger.Errorw("stale lease purge failed", append(logFields, "error", err)...)
			}
			s.emitSignal(SignalPartitionUpdate)

		case <-discoveryTicker.C:
			// Orphan sweep: periodically run acquisition with no cap. In a
			// healthy group the sweep is a no-op — TryAcquireLease cannot
			// steal a valid lease — but a partition left unleased for any
			// reason the cap arithmetic missed (divergent heartbeat views, a
			// subscriber that heartbeats without acquiring) is picked up by
			// whichever subscriber sweeps first. An over-cap grab is shed at
			// the next rebalance once a peer has spare capacity to take it.
			uncapped := time.Since(lastOrphanSweep) >= orphanSweepInterval
			if uncapped {
				lastOrphanSweep = time.Now()
			}
			if err := s.discoverAndReconcileWorkers(ctx, sub, uncapped); err != nil {
				s.logger.Errorw("partition discovery failed, will retry on next tick", append(logFields, "error", err)...)
			}
			s.emitSignal(SignalPartitionUpdate)
		}
	}
}

// discoverAndReconcileWorkers discovers new partitions and reconciles workers.
// Uses fair share to limit how many partitions this subscriber acquires;
// uncapped skips the fair-share cap entirely (the orphan sweep).
func (s *subscriber) discoverAndReconcileWorkers(ctx context.Context, sub *subscription, uncapped bool) error {
	cfg := sub.config

	// Get current leased partitions for fair share computation.
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic, cfg.SubscriberName, cfg.ConsumerGroup)
	if err != nil {
		return fmt.Errorf("get leased partitions: %w", err)
	}

	// Use cached discovered partitions from last tick for fair share cap.
	// On the first tick, lastDiscoveredPartitions is nil → fairShareCap sees
	// only owned partitions, so a joiner's first-tick cap floors at 1 and
	// ramps once discovery is cached.
	sub.workersMu.Lock()
	cachedDiscovered := sub.lastDiscoveredPartitions
	sub.workersMu.Unlock()

	// maxPartitions == 0 means unlimited (the orphan sweep, or an
	// uncontended single subscriber via fairShareCap).
	maxPartitions := 0
	if !uncapped {
		maxPartitions, err = s.fairShareCap(ctx, sub, leasedPartitions, cachedDiscovered)
		if err != nil {
			return fmt.Errorf("compute fair share cap: %w", err)
		}
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

	// Idle-lease release: an owned partition absent from discovery has no
	// stored messages left — everything was consumed and garbage-collected.
	// Held past the grace period, such a lease buys nothing (a worker
	// polling an empty partition forever) and on topics with short-lived
	// partition keys it leaks a lease row, an offsets row, and a goroutine
	// per key ever used. Release drops the partition entirely: reconcile
	// stops its worker, and if a message arrives later the partition
	// reappears in discovery and is reacquired like any new partition.
	grace := time.Duration(idleLeaseReleaseAfterLeaseDurations*cfg.LeaseDurationMs) * time.Millisecond
	var expired []string
	sub.drainedSince, expired = updateDrainedTracking(sub.drainedSince, leasedPartitions, discoveredPartitions, grace, time.Now())
	if len(expired) > 0 {
		released := make(map[string]struct{}, len(expired))
		for _, pk := range expired {
			// Delete this consumer group's offsets row first, while the
			// lease still guarantees exclusive ownership — nobody else can
			// be initializing the partition concurrently. Initialize
			// recreates the row if the partition ever comes back.
			if err := s.offsetStore.DeleteOffset(ctx, sub.topic, pk, cfg.ConsumerGroup); err != nil {
				// Retried next tick — the lease is still held, so the
				// partition stays tracked as drained.
				s.logger.Errorw("delete offsets for drained partition failed",
					"topic", sub.topic,
					"partition_key", pk,
					"error", err,
				)
				continue
			}
			if err := s.leaseStore.ReleaseLease(ctx, sub.topic, pk, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
				// Offsets row already deleted — harmless (the partition is
				// empty; Initialize recreates it on resurrection). Release
				// is retried next tick.
				s.logger.Errorw("release lease for drained partition failed",
					"topic", sub.topic,
					"partition_key", pk,
					"error", err,
				)
				continue
			}
			released[pk] = struct{}{}
			delete(sub.drainedSince, pk)

			// Stop the worker immediately rather than waiting for the
			// reconcile at the end of this tick: if a message arrived in the
			// window just before the release, another subscriber can acquire
			// the partition right away, and the old worker must not poll
			// alongside it. Mirrors the shed path in rebalance.
			s.stopPartitionWorker(sub, pk)

			metrics.NamedCounter(s.scope, "idle_lease", "released", 1, metrics.NewTag("topic", sub.topic))
			s.logger.Infow("released idle partition lease",
				"topic", sub.topic,
				"consumer_group", cfg.ConsumerGroup,
				"partition_key", pk,
			)
		}
		if len(released) > 0 {
			kept := make([]string, 0, len(leasedPartitions))
			for _, pk := range leasedPartitions {
				if _, ok := released[pk]; !ok {
					kept = append(kept, pk)
				}
			}
			leasedPartitions = kept
		}
	}

	s.reconcilePartitionWorkers(ctx, sub, leasedPartitions)
	return nil
}

// updateDrainedTracking recomputes, for every owned partition absent from
// this tick's discovery, when it was first observed drained. A partition is
// drained only when zero of its messages remain stored — in-flight,
// postponed, and unacked messages all keep rows in the messages table, so a
// partition with any outstanding work is always discovered. Partitions that
// reappear in discovery (or are no longer owned) are dropped from tracking;
// first-seen times carry over so the clock accumulates across ticks. Returns
// the updated tracking map and the partitions drained for at least grace
// (release candidates), sorted for determinism.
func updateDrainedTracking(prev map[string]time.Time, owned []string, discovered []string, grace time.Duration, now time.Time) (map[string]time.Time, []string) {
	discoveredSet := make(map[string]struct{}, len(discovered))
	for _, pk := range discovered {
		discoveredSet[pk] = struct{}{}
	}

	next := make(map[string]time.Time)
	var expired []string
	for _, pk := range owned {
		if _, live := discoveredSet[pk]; live {
			continue
		}
		since, tracked := prev[pk]
		if !tracked {
			since = now
		}
		next[pk] = since
		if now.Sub(since) >= grace {
			expired = append(expired, pk)
		}
	}
	sort.Strings(expired)
	return next, expired
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
				// A stopped worker has no next tick to recover on. Its context is
				// deliberately cancelled to interrupt any in-flight store call, so
				// the resulting error is part of normal teardown.
				if errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
					w.subscriber.logger.Infow("poll canceled while stopping partition worker",
						"topic", w.sub.topic,
						"partition_key", w.partitionKey,
						"consumer_group", w.sub.config.ConsumerGroup,
						"subscriber_name", w.sub.config.SubscriberName,
					)
					return
				}
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
func (w *partitionWorker) pollAndDeliver(ctx context.Context) (retErr error) {
	s := w.subscriber
	sub := w.sub
	cfg := sub.config
	partitionKey := w.partitionKey

	op := metrics.Begin(s.scope, "poll", metrics.StorageLatencyBuckets,
		metrics.NewTag("topic", sub.topic),
	)
	defer func() { op.Complete(retErr) }()

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

	// Fetch messages from the immutable log.
	rows, err := s.messageStore.FetchByOffset(ctx, sub.topic, partitionKey, currentOffset, cfg.BatchSize)
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
		//   state.InvisibleUntil > now → in-flight, nack delay, or postpone delay
		now := time.Now().UnixMilli()
		if found && (state.Acked || state.InvisibleUntil > now) {
			// A postponed message is a barrier: its partition waits for it, so
			// stop scanning instead of skipping past it. In-flight and nacked
			// messages are skipped — a failed delivery must not halt its partition.
			if !state.Acked && state.Postponed {
				break
			}
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
			//
			// The reason is generic here because this path has no failing
			// delivery to ask: Nack dead-letters the attempt that spends the
			// budget, carrying the real reason, so reaching this point means
			// the message never got that far — a crash, or a delivery whose
			// visibility timeout expired unacked.
			if cfg.DLQ.Enabled {
				retryLimitFailure := failure.New("exceeded retry limit")
				if err := s.messageStore.MoveToDLQ(ctx, sub.topic, partitionKey, row.ID, retryCount, retryLimitFailure, cfg.DLQ.TopicSuffix); err != nil {
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
		metrics.NamedHistogram(s.scope, "poll", "message_age", metrics.LongLatencyBuckets,
			metrics.NewTag("topic", sub.topic),
		).RecordDuration(messageAge)

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

		// Reassemble the failure recorded when this message was dead-lettered.
		// A malformed detail column degrades to the message alone rather than
		// failing the poll: a corrupt diagnostic must not stop delivery.
		var (
			rowFailure failure.Failure
			failed     = row.FailedAt > 0
		)
		if failed {
			decoded, err := failure.Decode(row.FailureDetail)
			if err != nil {
				s.logger.Warnw("ignoring unreadable failure detail on dlq message",
					"topic", sub.topic,
					"partition_key", partitionKey,
					"message_id", row.ID,
					"error", err,
				)
			}
			decoded.Message = row.LastError
			rowFailure = decoded
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
			cfg.Retry,
			rowFailure,
			failed,
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

	// Record poll metrics
	if messageCount > 0 {
		metrics.NamedCounter(s.scope, "poll", "messages_delivered", int64(messageCount),
			metrics.NewTag("topic", sub.topic),
		)
	}

	// GC runs every Nth tick regardless of delivery activity; an idle-only
	// gate starved continuously busy partitions of garbage collection.
	// Metrics are reported above so a GC failure cannot drop the delivery count.
	w.gcCounter++
	if w.gcCounter >= gcTickInterval {
		w.gcCounter = 0
		if err := w.garbageCollect(ctx); err != nil {
			return fmt.Errorf("garbage collect: %w", err)
		}
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
// and releases extras so other subscribers can pick them up. Returns the
// partitions actually released so the caller renews only the remainder —
// renewing a just-released lease would spuriously fail with ErrLeaseExpired.
// The owned slice is never mutated (the caller shares it with lease renewal).
// On error, partitions released before the failure are still returned.
func (s *subscriber) rebalance(ctx context.Context, sub *subscription, owned []string) (released []string, retErr error) {
	cfg := sub.config

	// Use cached discovered partitions from the most recent discovery tick.
	sub.workersMu.Lock()
	discoveredPartitions := sub.lastDiscoveredPartitions
	sub.workersMu.Unlock()

	maxPart, err := s.fairShareCap(ctx, sub, owned, discoveredPartitions)
	if err != nil {
		return nil, fmt.Errorf("compute fair share cap: %w", err)
	}
	if maxPart == 0 || len(owned) <= maxPart {
		return nil, nil
	}

	// Sort a copy deterministically so the same partitions are released
	// across runs without reordering the caller's slice.
	sortedOwned := make([]string, len(owned))
	copy(sortedOwned, owned)
	sort.Strings(sortedOwned)

	// Release excess partitions
	for _, pk := range sortedOwned[maxPart:] {
		if err := s.leaseStore.ReleaseLease(ctx, sub.topic, pk, cfg.SubscriberName, cfg.ConsumerGroup); err != nil {
			return released, fmt.Errorf("release partition %s during rebalance: %w", pk, err)
		}
		released = append(released, pk)

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
	return released, nil
}

// fairShareCap computes the max partitions this subscriber should own.
// Returns (maxPart, error). maxPart=0 means unlimited.
// owned is the caller-provided list of leased partitions.
// discoveredPartitions is an optional pre-fetched list of all known partitions;
// if nil, only owned partitions are used for fair share computation.
//
// The cap is remainder-aware: subscribers rank themselves in the sorted
// active list, the first (P mod N) ranks get floor(P/N)+1, and the rest get
// floor(P/N), so per-rank caps sum to exactly P. Independent ceil(P/N) caps
// sum to more than P and admit stable starvation states — e.g. P=12, N=5
// could settle at 3/3/3/3/0 with every subscriber at cap and nobody obliged
// to shed for the empty one. With caps summing to P, a subscriber over its
// cap implies another under its cap (rebalance sheds, the peer acquires),
// and an unleased partition implies a subscriber with spare cap to claim it
// — neither a starved subscriber nor a leftover partition is a stable state.
func (s *subscriber) fairShareCap(ctx context.Context, sub *subscription, owned []string, discoveredPartitions []string) (int, error) {
	cfg := sub.config

	active, err := s.heartbeatStore.ActiveSubscribers(ctx, sub.topic, cfg.ConsumerGroup, cfg.LeaseDurationMs)
	if err != nil {
		return 0, err
	}
	if len(active) <= 1 {
		return 0, nil
	}

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

	// Rank in the sorted active list. ActiveSubscribers row order is not
	// guaranteed, so sorting is what lets every subscriber derive the same
	// ranking from the same set without coordination.
	sort.Strings(active)
	n := len(active)
	rank := -1
	for i, name := range active {
		if name == cfg.SubscriberName {
			rank = i
			break
		}
	}

	var maxPart int
	if rank < 0 {
		// Own heartbeat not visible this tick (e.g. the write failed): fall
		// back to a conservative ceil over n+1 contenders instead of
		// claiming a rank that may belong to another subscriber.
		maxPart = (totalPartitions + n) / (n + 1)
	} else {
		maxPart = totalPartitions / n
		if rank < totalPartitions%n {
			maxPart++
		}
	}
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
	op := metrics.Begin(s.scope, "close", metrics.StorageLatencyBuckets)
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
	}

	s.subscriptions = make(map[string]*subscription)

	s.closed = true

	s.logger.Infow("subscriber closed")
	return nil
}

func retryBackoffMs(retry extqueue.RetryConfig, attempt int) int64 {
	backoffMs := retry.InitialBackoffMs
	if backoffMs <= 0 {
		return 0
	}

	maxBackoffMs := retry.MaxBackoffMs
	if maxBackoffMs <= 0 || maxBackoffMs > maxRetryBackoffMs {
		maxBackoffMs = maxRetryBackoffMs
	}
	if backoffMs >= maxBackoffMs {
		return maxBackoffMs
	}

	multiplier := retry.BackoffMultiplier
	if multiplier <= 1 || math.IsNaN(multiplier) || attempt <= 1 {
		return backoffMs
	}

	backoff := float64(backoffMs) * math.Pow(multiplier, float64(attempt-1))
	if backoff >= float64(maxBackoffMs) {
		return maxBackoffMs
	}
	return int64(backoff)
}

func validateRetryConfig(retry extqueue.RetryConfig) error {
	if retry.MaxAttempts < 0 {
		return fmt.Errorf("retry MaxAttempts must be non-negative, got %d", retry.MaxAttempts)
	}
	if retry.InitialBackoffMs < 0 {
		return fmt.Errorf("retry InitialBackoffMs must be non-negative, got %d", retry.InitialBackoffMs)
	}
	if retry.MaxBackoffMs < 0 {
		return fmt.Errorf("retry MaxBackoffMs must be non-negative, got %d", retry.MaxBackoffMs)
	}
	if retry.InitialBackoffMs > maxRetryBackoffMs {
		return fmt.Errorf("retry InitialBackoffMs must not exceed %d, got %d", maxRetryBackoffMs, retry.InitialBackoffMs)
	}
	if retry.MaxBackoffMs > maxRetryBackoffMs {
		return fmt.Errorf("retry MaxBackoffMs must not exceed %d, got %d", maxRetryBackoffMs, retry.MaxBackoffMs)
	}
	if retry.MaxBackoffMs > 0 && retry.InitialBackoffMs > retry.MaxBackoffMs {
		return fmt.Errorf("retry InitialBackoffMs (%d) must not exceed MaxBackoffMs (%d)", retry.InitialBackoffMs, retry.MaxBackoffMs)
	}
	if retry.BackoffMultiplier < 0 || math.IsNaN(retry.BackoffMultiplier) || math.IsInf(retry.BackoffMultiplier, 0) {
		return fmt.Errorf("retry BackoffMultiplier must be finite and non-negative, got %v", retry.BackoffMultiplier)
	}
	return nil
}
