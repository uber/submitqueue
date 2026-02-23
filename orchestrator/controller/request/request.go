package request

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	extqueue "github.com/uber/submitqueue/extension/queue"
	"go.uber.org/zap"
)

// Controller handles request queue messages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	topic        string
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new request controller for the orchestrator.
func NewController(logger *zap.SugaredLogger, scope tally.Scope) *Controller {
	return &Controller{
		logger:       logger.Named("request_controller"),
		metricsScope: scope.SubScope("request_controller"),
		topic:         "request",
		consumerGroup: "orchestrator-request",
	}
}

// Process processes a request delivery from the queue.
// Deserializes the request, logs the event, and prepares for future state transitions.
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

	c.logger.Infow("received land request event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"land_strategy", string(request.LandStrategy),
		"change_source", request.Change.Source,
		"change_ids", request.Change.IDs,
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Update request state to processing
	// TODO: Perform validation checks
	// TODO: Publish to next queue (requests_for_batching)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "request"
}

// Topic returns the topic this controller subscribes to.
func (c *Controller) Topic() string {
	return c.topic
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}

// SubscriptionConfig returns the subscription config for the request controller.
// Uses default settings which work well for request processing (100ms poll, 60s visibility timeout).
func (c *Controller) SubscriptionConfig(subscriberName string) extqueue.SubscriptionConfig {
	config := extqueue.DefaultSubscriptionConfig(subscriberName, c.consumerGroup)

	// Request controller uses default settings:
	// - PollInterval: 100ms (fast polling for immediate request processing)
	// - BatchSize: 10
	// - VisibilityTimeout: 60s
	// - Retry: 3 attempts with exponential backoff
	// - DLQ: enabled

	// Can customize if needed:
	// config.PollInterval = 50 * time.Millisecond  // Even faster polling
	// config.BatchSize = 20                         // Process more requests at once

	return config
}
