package speculate

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
// It consumes batches, performs speculation, and publishes to both build and merge stages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new speculate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a speculate delivery from the queue.
// Deserializes the batch, performs speculation, and publishes to both build and merge topics.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	// Deserialize batch entity
	batch, err := entity.BatchFromBytes(msg.Payload)
	if err != nil {
		c.logger.Errorw("failed to deserialize batch",
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
			"error", err,
		)
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize batch: %w", err)
	}

	c.logger.Infow("received speculate event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Add speculation logic
	// - Speculative merge/rebase
	// - Conflict detection
	// - Publish to build only if speculation is in progress (needs CI verification)
	// - Publish to merge only if speculation is complete and successful (ready to land)

	// Publish to build topic
	if err := c.publish(ctx, consumer.TopicKeyBuild, batch); err != nil {
		c.logger.Errorw("failed to publish to build",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyBuild,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to build: %w", err))
	}

	c.logger.Infow("published batch to build",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeyBuild,
	)

	// Publish to merge topic
	if err := c.publish(ctx, consumer.TopicKeyMerge, batch); err != nil {
		c.logger.Errorw("failed to publish to merge",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyMerge,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to merge: %w", err))
	}

	c.logger.Infow("published batch to merge",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeyMerge,
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// publish publishes a batch to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batch entity.Batch) error {
	payload, err := batch.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch: %w", err)
	}

	msg := entityqueue.NewMessage(batch.ID, payload, batch.Queue, nil)

	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}

	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "speculate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
