package sql

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/entities/queue"
	extqueue "github.com/uber/submitqueue/extensions/queue"
)

type subscriber struct {
	config       Config
	logger       *zap.SugaredLogger
	metrics      tally.Scope
	messageStore messageStore
	offsetStore  offsetStore
	leaseStore   partitionLeaseStore
	mu           sync.RWMutex
	closed       bool

	// Active subscriptions
	subscriptions map[string]*subscription
	subMu         sync.Mutex
}

type subscription struct {
	topic      string
	deliveryCh chan extqueue.Delivery
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// sqlDelivery implements extqueue.Delivery for SQL queue
type sqlDelivery struct {
	msg        queue.Message
	deliveryID string
	attempt    int
	receivedAt int64
	metadata   map[string]string

	// Backend-specific fields for ack/nack
	subscriber   *subscriber
	topic        string
	partitionKey string
	offset       int64
	messageID    string

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
) *sqlDelivery {
	return &sqlDelivery{
		msg:          msg,
		deliveryID:   deliveryID,
		attempt:      attempt,
		receivedAt:   time.Now().UnixMilli(),
		metadata:     metadata,
		subscriber:   subscriber,
		topic:        topic,
		partitionKey: partitionKey,
		offset:       offset,
		messageID:    messageID,
		acknowledged: false,
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
	if err := d.subscriber.offsetStore.AckMessage(ctx, d.topic, d.partitionKey, d.messageID, d.offset, d.subscriber.messageStore); err != nil {
		return err
	}

	// Record metrics
	d.subscriber.metrics.Tagged(map[string]string{
		"topic":         d.topic,
		"partition_key": d.partitionKey,
	}).Counter("messages_acked").Inc(1)

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
	if err := d.subscriber.messageStore.SetVisibilityTimeout(ctx, d.topic, d.messageID, requeueAfterMillis); err != nil {
		d.subscriber.logger.Errorw("failed to set visibility timeout for nack",
			"topic", d.topic,
			"partition_key", d.partitionKey,
			"message_id", d.messageID,
			"error", err,
		)
		return err
	}

	// Record metrics
	d.subscriber.metrics.Tagged(map[string]string{
		"topic":         d.topic,
		"partition_key": d.partitionKey,
	}).Counter("messages_nacked").Inc(1)

	d.subscriber.logger.Infow("message nacked",
		"topic", d.topic,
		"partition_key", d.partitionKey,
		"message_id", d.messageID,
		"requeue_after_millis", requeueAfterMillis,
	)

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

	if err := d.subscriber.messageStore.SetVisibilityTimeout(ctx, d.topic, d.messageID, durationMillis); err != nil {
		return err
	}

	// Record metrics
	d.subscriber.metrics.Tagged(map[string]string{
		"topic":         d.topic,
		"partition_key": d.partitionKey,
	}).Counter("visibility_extended").Inc(1)

	return nil
}

func NewSubscriber(config Config, logger *zap.SugaredLogger, metrics tally.Scope, messageStore messageStore, offsetStore offsetStore, leaseStore partitionLeaseStore) *subscriber {
	logger.Infow("created subscriber",
		"consumer_group", config.ConsumerGroup,
		"worker_id", config.WorkerID,
		"poll_interval", config.PollInterval,
		"batch_size", config.BatchSize,
		"max_retry_attempts", config.Retry.MaxAttempts,
		"lease_renewal_interval", config.LeaseRenewalInterval,
	)

	return &subscriber{
		config:        config,
		logger:        logger,
		metrics:       metrics,
		messageStore:  messageStore,
		offsetStore:   offsetStore,
		leaseStore:    leaseStore,
		subscriptions: make(map[string]*subscription),
	}
}

// Subscribe starts consuming messages from the specified topic
func (s *subscriber) Subscribe(ctx context.Context, topic string) (<-chan extqueue.Delivery, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()

	if closed {
		s.logger.Errorw("subscribe failed: subscriber is closed", "topic", topic)
		return nil, fmt.Errorf("subscriber is closed")
	}

	// Validate topic name
	if err := validateTopicName(topic); err != nil {
		s.logger.Errorw("subscribe failed: invalid topic name", "topic", topic, "error", err)
		return nil, fmt.Errorf("subscribe failure: invalid topic name. err: %w", err)
	}

	s.subMu.Lock()
	defer s.subMu.Unlock()

	// Check if already subscribed
	if sub, exists := s.subscriptions[topic]; exists {
		s.logger.Debugw("reusing existing subscription", "topic", topic)
		return sub.deliveryCh, nil
	}

	s.logger.Infow("creating new subscription", "topic", topic)

	// Create new subscription
	// Use a cancellable context for managing the subscription lifecycle
	// and close will cancel the context to signal goroutines to stop
	subCtx, cancel := context.WithCancel(context.Background())
	sub := &subscription{
		topic:      topic,
		deliveryCh: make(chan extqueue.Delivery, s.config.BatchSize*2),
		cancelFunc: cancel,
	}

	s.subscriptions[topic] = sub

	// Track active subscription
	s.metrics.Tagged(map[string]string{"topic": topic}).Gauge("active_subscriptions").Update(1)

	// Start partition leasing and polling goroutine
	sub.wg.Add(1)
	go s.managePartitions(subCtx, sub)

	s.logger.Debugw("subscription created", "topic", topic, "consumer_group", s.config.ConsumerGroup, "worker_id", s.config.WorkerID)
	return sub.deliveryCh, nil
}

// managePartitions discovers partitions, acquires leases, and polls messages
func (s *subscriber) managePartitions(ctx context.Context, sub *subscription) {
	defer sub.wg.Done()
	defer close(sub.deliveryCh)

	pollTicker := time.NewTicker(s.config.PollInterval)
	defer pollTicker.Stop()

	leaseTicker := time.NewTicker(s.config.LeaseRenewalInterval)
	defer leaseTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Release all leases on shutdown
			s.releaseAllLeases(ctx, sub.topic)
			return

		case <-leaseTicker.C:
			// Renew existing leases
			s.renewLeases(ctx, sub.topic)

		case <-pollTicker.C:
			// Fetch and deliver messages from leased partitions
			s.pollLeasedPartitions(ctx, sub)
		}
	}
}

// renewLeases renews leases for all partitions owned by this worker
func (s *subscriber) renewLeases(ctx context.Context, topic string) {
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, topic)
	if err != nil {
		s.logger.Errorw("failed to get leased partitions for renewal",
			"topic", topic,
			"error", err,
		)
		// Error suppressed: lease renewal is best-effort. If we can't get leases,
		// they will eventually expire and be reacquired by this or another worker.
		// Failing the entire renewal cycle would be worse than skipping one iteration.
		s.metrics.Tagged(map[string]string{"topic": topic}).Counter("lease_renewal.get_partitions_errors").Inc(1)
		return
	}

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.RenewLease(ctx, topic, partitionKey); err != nil {
			s.logger.Warnw("failed to renew lease",
				"topic", topic,
				"partition_key", partitionKey,
				"error", err,
			)
			// Error suppressed: Continue trying to renew other leases even if one fails.
			// The partition will eventually expire and be re-acquired by this or another worker.
			// Failing fast would prevent other partitions from being renewed.
			s.metrics.Tagged(map[string]string{
				"topic":         topic,
				"partition_key": partitionKey,
			}).Counter("lease_renewal.renew_errors").Inc(1)
		}
	}
}

// releaseAllLeases releases all leases for a topic
func (s *subscriber) releaseAllLeases(ctx context.Context, topic string) {
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, topic)
	if err != nil {
		s.logger.Errorw("failed to get leased partitions for release",
			"topic", topic,
			"error", err,
		)
		return
	}

	for _, partitionKey := range leasedPartitions {
		if err := s.leaseStore.ReleaseLease(ctx, topic, partitionKey); err != nil {
			s.logger.Warnw("failed to release lease",
				"topic", topic,
				"partition_key", partitionKey,
				"error", err,
			)
			// Continue trying to release other leases even if one fails
		}
	}
}

// pollLeasedPartitions fetches and delivers messages from all leased partitions
func (s *subscriber) pollLeasedPartitions(ctx context.Context, sub *subscription) {
	// Discover and try to acquire leases for new partitions
	acquiredCount, err := s.leaseStore.DiscoverAndAcquirePartitions(ctx, sub.topic)
	if err == nil && acquiredCount > 0 {
		s.metrics.Tagged(map[string]string{"topic": sub.topic}).Counter("leases_acquired").Inc(int64(acquiredCount))
	}

	// Get currently leased partitions
	leasedPartitions, err := s.leaseStore.GetLeasedPartitions(ctx, sub.topic)
	if err != nil {
		s.logger.Errorw("failed to get leased partitions", "topic", sub.topic, "error", err)
		return
	}

	// Poll each leased partition
	for _, partitionKey := range leasedPartitions {
		// Check if context was cancelled before processing next partition
		select {
		case <-ctx.Done():
			return
		default:
			s.fetchAndDeliverPartition(ctx, sub, partitionKey)
		}
	}
}

// fetchAndDeliverPartition fetches messages from a specific partition and delivers them
func (s *subscriber) fetchAndDeliverPartition(ctx context.Context, sub *subscription, partitionKey string) {
	start := time.Now()

	// Initialize offset for this partition if needed
	if err := s.offsetStore.Initialize(ctx, sub.topic, partitionKey); err != nil {
		s.logger.Errorw("offset initialization failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
		return
	}

	// Get current offset for this partition
	currentOffset, err := s.offsetStore.GetAckedOffset(ctx, sub.topic, partitionKey)
	if err != nil {
		s.logger.Errorw("get current offset failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
		return
	}

	// Fetch messages for this partition
	rows, err := s.messageStore.FetchByOffset(ctx, sub.topic, partitionKey, currentOffset, s.config.BatchSize)
	if err != nil {
		s.logger.Errorw("fetch messages failure", "topic", sub.topic, "partition_key", partitionKey, "error", err)
		return
	}

	messageCount := 0
	for _, row := range rows {
		// Check if message has exceeded retry limit (persistent retry_count from DB)
		if row.RetryCount >= s.config.Retry.MaxAttempts {
			s.logger.Warnw("message exceeded retry limit",
				"topic", sub.topic,
				"partition_key", partitionKey,
				"message_id", row.ID,
				"retry_count", row.RetryCount,
			)

			// Move to DLQ if enabled
			if s.config.DLQ.Enabled {
				if err := s.messageStore.MoveToDLQ(ctx, sub.topic, row.ID, row.RetryCount, "exceeded retry limit"); err != nil {
					s.logger.Errorw("failed to move message to DLQ",
						"topic", sub.topic,
						"message_id", row.ID,
						"error", err,
					)
				} else {
					s.logger.Infow("moved message to DLQ",
						"topic", sub.topic,
						"message_id", row.ID,
						"retry_count", row.RetryCount,
					)
					s.metrics.Tagged(map[string]string{
						"topic":         sub.topic,
						"partition_key": partitionKey,
					}).Counter("messages_moved_to_dlq").Inc(1)

					// Update offset since message is now processed (moved to DLQ)
					if err := s.offsetStore.UpdateAckedOffset(ctx, sub.topic, partitionKey, row.Offset); err != nil {
						s.logger.Errorw("failed to update offset after DLQ move",
							"topic", sub.topic,
							"partition_key", partitionKey,
							"offset", row.Offset,
							"error", err,
						)
					}
				}
			}
			continue
		}

		// Create message (value type)
		msg := queue.NewMessage(row.ID, row.Payload)
		msg.Metadata = row.Metadata
		msg.PartitionKey = row.PartitionKey
		msg.PublishedAt = row.PublishedAt

		// Calculate message age for metrics
		messageAge := time.Duration(time.Now().UnixMilli()-row.PublishedAt) * time.Millisecond
		s.metrics.Tagged(map[string]string{
			"topic":         sub.topic,
			"partition_key": partitionKey,
		}).Timer("message_age").Record(messageAge)

		// Create delivery ID from offset
		deliveryID := strconv.FormatInt(row.Offset, 10)

		// Create delivery metadata
		deliveryMetadata := map[string]string{
			"topic":         sub.topic,
			"partition_key": partitionKey,
			"offset":        deliveryID,
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
		)

		// Deliver message
		select {
		case sub.deliveryCh <- delivery:
			messageCount++
		case <-ctx.Done():
			return
		}
	}

	// Record metrics
	if messageCount > 0 {
		elapsed := time.Since(start)
		partitionTags := map[string]string{
			"topic":         sub.topic,
			"partition_key": partitionKey,
		}
		s.metrics.Tagged(partitionTags).Counter("messages_received").Inc(int64(messageCount))
		s.metrics.Tagged(partitionTags).Timer("poll_latency").Record(elapsed)

		s.logger.Debugw("delivered messages",
			"topic", sub.topic,
			"partition_key", partitionKey,
			"count", messageCount,
			"duration_ms", elapsed.Milliseconds(),
		)
	}
}

// Close gracefully shuts down the subscriber
func (s *subscriber) Close() error {
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

		// Wait for goroutine to finish with timeout
		done := make(chan struct{})
		go func() {
			sub.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Graceful shutdown completed
		case <-time.After(30 * time.Second):
			s.logger.Warnw("subscription shutdown timeout", "topic", topic)
		}

		// Update metrics
		s.metrics.Tagged(map[string]string{"topic": topic}).Gauge("active_subscriptions").Update(0)
	}

	s.subscriptions = make(map[string]*subscription)

	s.closed = true

	s.logger.Info("subscriber closed")
	return nil
}
