package request

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/mergechecker"
	"go.uber.org/zap"
)

// Controller handles request queue messages.
// It consumes requests, validates them, and publishes to the next stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	mergeChecker  mergechecker.MergeChecker
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new request controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	mergeChecker mergechecker.MergeChecker,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("request_controller"),
		metricsScope:  scope.SubScope("request_controller"),
		registry:      registry,
		mergeChecker:  mergeChecker,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a request delivery from the queue.
// Deserializes the request, validates it, and publishes to the batch topic.
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
		return fmt.Errorf("failed to deserialize request: %w", err)
	}

	c.logger.Infow("received land request event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"land_strategy", string(request.LandStrategy),
		"change_uris", request.Change.URIs,
		"change_count", len(request.Change.URIs),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Merge conflict check
	mergeResult, err := c.mergeChecker.Check(ctx, request.Queue, request.Change)
	if err != nil {
		c.logger.Errorw("merge check failed",
			"request_id", request.ID,
			"error", err,
		)
		c.metricsScope.Counter("merge_check_errors").Inc(1)
		return fmt.Errorf("merge check failed: %w", err)
	}
	if !mergeResult.Mergeable {
		c.logger.Infow("request not mergeable",
			"request_id", request.ID,
			"queue", request.Queue,
			"reason", mergeResult.Reason,
		)
		c.metricsScope.Counter("not_mergeable").Inc(1)
		return errs.NewUserError(fmt.Errorf("request %s is not mergeable: %s", request.ID, mergeResult.Reason))
	}

	// Publish to batch topic
	if err := c.publish(ctx, consumer.TopicKeyToBatch, request); err != nil {
		c.logger.Errorw("failed to publish output",
			"request_id", request.ID,
			"topic_key", "to-batch",
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to batch: %w", err))
	}

	c.logger.Infow("published request to next stage",
		"request_id", request.ID,
		"topic_key", "to-batch",
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// publish publishes a request to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, request entity.Request) error {
	payload, err := request.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request: %w", err)
	}

	msg := entityqueue.NewMessage(request.ID, payload, request.Queue, nil)

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
	return "request"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
