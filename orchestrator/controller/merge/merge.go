package merge

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"go.uber.org/zap"
)

// Controller handles merge queue messages.
// It consumes merge requests, performs merges, and publishes results to the merge signal stage.
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

// NewController creates a new merge controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topic consumer.Topic,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("merge_controller"),
		metricsScope:  scope.SubScope("merge_controller"),
		registry:      registry,
		topic:         topic,
		consumerGroup: consumerGroup,
	}
}

// Process processes a merge delivery from the queue.
// Deserializes the request, performs the merge, and publishes to the merge signal topic.
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

	c.logger.Infow("received merge event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Add merge logic
	// - Perform source control merge operation
	// - Handle merge conflicts

	// Publish to merge signal topic
	if err := c.publish(ctx, consumer.TopicMergeSignal, request); err != nil {
		c.logger.Errorw("failed to publish output",
			"request_id", request.ID,
			"topic", consumer.TopicMergeSignal,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to merge-signal: %w", err)
	}

	c.logger.Infow("published request to next stage",
		"request_id", request.ID,
		"topic", consumer.TopicMergeSignal,
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
	return "merge"
}

// Topic returns the topic this controller subscribes to.
func (c *Controller) Topic() consumer.Topic {
	return c.topic
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
