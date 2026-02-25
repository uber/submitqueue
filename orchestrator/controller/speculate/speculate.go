package speculate

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
// It consumes batched requests, performs speculation, and publishes to both build and merge stages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topic         consumer.Topic
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new speculate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topic consumer.Topic,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		registry:      registry,
		topic:         topic,
		consumerGroup: consumerGroup,
	}
}

// Process processes a speculate delivery from the queue.
// Deserializes the request, performs speculation, and publishes to both build and merge topics.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	// Deserialize request entity
	request, err := entity.RequestFromBytes(msg.Payload)
	if err != nil {
		c.logger.Errorw("failed to deserialize request",
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
			"error", err,
		)
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return consumer.NewNonRetryableError(fmt.Errorf("failed to deserialize request: %w", err))
	}

	c.logger.Infow("received speculate event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Add speculation logic
	// - Speculative merge/rebase
	// - Conflict detection

	// Publish to build topic
	if err := c.publish(ctx, consumer.TopicBuild, request); err != nil {
		c.logger.Errorw("failed to publish to build",
			"request_id", request.ID,
			"topic", consumer.TopicBuild,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to build: %w", err)
	}

	// Publish to merge topic
	if err := c.publish(ctx, consumer.TopicToMerge, request); err != nil {
		c.logger.Errorw("failed to publish to merge",
			"request_id", request.ID,
			"topic", consumer.TopicToMerge,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to merge: %w", err)
	}

	c.logger.Infow("published request to next stages",
		"request_id", request.ID,
		"topics", []string{consumer.TopicBuild.String(), consumer.TopicToMerge.String()},
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// publish publishes a request to the specified topic.
func (c *Controller) publish(ctx context.Context, topic consumer.Topic, request entity.Request) error {
	payload, err := request.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request: %w", err)
	}

	msg := entityqueue.NewMessage(request.ID, payload, request.Queue, nil)

	q, ok := c.registry.Queue(topic)
	if !ok {
		return fmt.Errorf("no queue registered for topic %s", topic)
	}

	if err := q.Publisher().Publish(ctx, topic.String(), msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "speculate"
}

// Topic returns the topic this controller subscribes to.
func (c *Controller) Topic() consumer.Topic {
	return c.topic
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
