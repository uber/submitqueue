package score

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles score queue messages.
// It consumes batches, scores them, and publishes to the speculate stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
	scorer        scorer.Scorer
	store         storage.Storage
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new score controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
	scorer scorer.Scorer,
	store storage.Storage,
) *Controller {
	return &Controller{
		logger:        logger.Named("score_controller"),
		metricsScope:  scope.SubScope("score_controller"),
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		scorer:        scorer,
		store:         store,
	}
}

// Process processes a score delivery from the queue.
// Deserializes the batch, scores it, and publishes to the speculate topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

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
		metrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize batch: %w", err)
	}

	c.logger.Infow("received score event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Validate batch contains exactly one request
	if len(batch.Contains) == 0 {
		metrics.NamedCounter(c.metricsScope, "process", "empty_batch_errors", 1)
		return fmt.Errorf("batch %s contains no requests", batch.ID)
	}
	if len(batch.Contains) > 1 {
		// TODO: multi-request batches will be supported later
		metrics.NamedCounter(c.metricsScope, "process", "multi_request_batch_errors", 1)
		return fmt.Errorf("batch %s contains %d requests, only single-request batches are supported", batch.ID, len(batch.Contains))
	}

	// Look up the request to get its Change
	request, err := c.store.GetRequestStore().Get(ctx, batch.Contains[0])
	if err != nil {
		if storage.IsNotFound(err) {
			c.logger.Errorw("request not found",
				"batch_id", batch.ID,
				"request_id", batch.Contains[0],
				"error", err,
			)
			metrics.NamedCounter(c.metricsScope, "process", "request_not_found_errors", 1)
			return fmt.Errorf("request %s not found: %w", batch.Contains[0], err)
		}
		c.logger.Errorw("failed to get request",
			"batch_id", batch.ID,
			"request_id", batch.Contains[0],
			"error", err,
		)
		metrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
		return errs.NewRetryableError(fmt.Errorf("failed to get request %s: %w", batch.Contains[0], err))
	}

	// Score the change
	score, err := c.scorer.Score(ctx, request.Change)
	if err != nil {
		c.logger.Errorw("failed to score change",
			"batch_id", batch.ID,
			"request_id", request.ID,
			"error", err,
		)
		metrics.NamedCounter(c.metricsScope, "process", "scorer_errors", 1)
		return errs.NewRetryableError(fmt.Errorf("failed to score change: %w", err))
	}

	batchScore := float32(score)

	c.logger.Infow("scored batch",
		"batch_id", batch.ID,
		"score", batchScore,
	)

	// Update batch store with score and transition state to speculating
	if err := c.store.GetBatchStore().UpdateScoreAndState(ctx, batch.ID, batch.Version, batchScore, entity.BatchStateSpeculating); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			c.logger.Errorw("version mismatch updating batch score",
				"batch_id", batch.ID,
				"version", batch.Version,
				"error", err,
			)
			metrics.NamedCounter(c.metricsScope, "process", "version_mismatch_errors", 1)
			return fmt.Errorf("version mismatch updating batch %s: %w", batch.ID, err)
		}
		c.logger.Errorw("failed to update batch score",
			"batch_id", batch.ID,
			"error", err,
		)
		metrics.NamedCounter(c.metricsScope, "process", "batch_store_errors", 1)
		return errs.NewRetryableError(fmt.Errorf("failed to update batch %s score: %w", batch.ID, err))
	}

	// Create new batch with updated state and version to reflect the store update
	scored := batch.WithScoreAndState(batchScore, entity.BatchStateSpeculating)

	// Publish to speculate topic
	if err := c.publish(ctx, consumer.TopicKeySpeculate, scored); err != nil {
		c.logger.Errorw("failed to publish output",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeySpeculate,
			"error", err,
		)
		metrics.NamedCounter(c.metricsScope, "process", "publish_errors", 1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to speculate: %w", err))
	}

	c.logger.Infow("published batch to speculate",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeySpeculate,
	)

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
	return "score"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
