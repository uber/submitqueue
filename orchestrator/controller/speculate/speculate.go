package speculate

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/speculation"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
// It consumes batches, performs speculation with scoring, and publishes to both build and merge stages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	strategy      speculation.Strategy
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
	strategy speculation.Strategy,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		registry:      registry,
		strategy:      strategy,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a speculate delivery from the queue.
// Deserializes the batch, performs speculation with scoring, and publishes to both build and merge topics.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	// Deserialize batch entity (batch controller publishes Batch messages).
	batch, err := entity.BatchFromBytes(msg.Payload)
	if err != nil {
		c.logger.Errorw("failed to deserialize batch",
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
			"error", err,
		)
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		return consumer.NewNonRetryableError(fmt.Errorf("failed to deserialize batch: %w", err))
	}

	c.logger.Infow("received speculate event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Extract dependency batch IDs.
	dependencyIDs := dependencyBatchIDs(batch.Dependencies)

	// Generate speculation tree using the configured strategy.
	tree, err := c.strategy.Generate(ctx, batch.ID, dependencyIDs)
	if err != nil {
		c.logger.Errorw("failed to generate speculation tree",
			"batch_id", batch.ID,
			"error", err,
		)
		c.metricsScope.Counter("speculation_errors").Inc(1)
		return consumer.NewNonRetryableError(fmt.Errorf("failed to generate speculation tree: %w", err))
	}

	c.logger.Infow("generated speculation tree",
		"batch_id", batch.ID,
		"path_count", len(tree.Speculations),
	)

	// Publish to build topic
	if err := c.publish(ctx, consumer.TopicKeyBuild, batch); err != nil {
		c.logger.Errorw("failed to publish to build",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyBuild,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to build: %w", err)
	}

	// Publish to merge topic
	if err := c.publish(ctx, consumer.TopicKeyToMerge, batch); err != nil {
		c.logger.Errorw("failed to publish to merge",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyToMerge,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to merge: %w", err)
	}

	c.logger.Infow("published batch to next stages",
		"batch_id", batch.ID,
		"topic_keys", []string{consumer.TopicKeyBuild.String(), consumer.TopicKeyToMerge.String()},
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

// dependencyBatchIDs extracts batch IDs from the Dependencies field.
func dependencyBatchIDs(deps []map[string]interface{}) []string {
	ids := make([]string, 0, len(deps))
	for _, dep := range deps {
		if id, ok := dep["ID"].(string); ok {
			ids = append(ids, id)
		}
	}
	return ids
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
