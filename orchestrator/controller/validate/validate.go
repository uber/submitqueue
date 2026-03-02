package validate

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

// Controller handles validate queue messages.
// It consumes requests, performs validation checks (merge conflicts, duplicate requests, etc.),
// and publishes to the batch stage. Validation logic is extensible to support additional checks.
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

// NewController creates a new validate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	mergeChecker mergechecker.MergeChecker,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("validate_controller"),
		metricsScope:  scope.SubScope("validate_controller"),
		registry:      registry,
		mergeChecker:  mergeChecker,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a validate delivery from the queue.
// Deserializes the request and publishes to the batch topic.
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

	c.logger.Infow("received validate event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
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
	if err := c.publish(ctx, consumer.TopicKeyBatch, request); err != nil {
		c.logger.Errorw("failed to publish output",
			"request_id", request.ID,
			"topic_key", consumer.TopicKeyBatch,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to batch: %w", err))
	}

	c.logger.Infow("published request to batch",
		"request_id", request.ID,
		"topic_key", consumer.TopicKeyBatch,
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
	return "validate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
