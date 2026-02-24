package finalize

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	"go.uber.org/zap"
)

// Controller handles finalize queue messages.
// It consumes finalization requests and completes the pipeline.
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

// NewController creates a new finalize controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topic consumer.Topic,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("finalize_controller"),
		metricsScope:  scope.SubScope("finalize_controller"),
		registry:      registry,
		topic:         topic,
		consumerGroup: consumerGroup,
	}
}

// Process processes a finalize delivery from the queue.
// Deserializes the request and completes the pipeline processing.
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

	c.logger.Infow("received finalize event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Add finalization logic
	// - Mark request as landed or error
	// - Send notifications
	// - Clean up resources

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "finalize"
}

// Topic returns the topic this controller subscribes to.
func (c *Controller) Topic() consumer.Topic {
	return c.topic
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
