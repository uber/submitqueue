package batch

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/counter"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles batch queue messages.
// It consumes validated requests, groups them into batches, and publishes to the score stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	counter       counter.Counter
	store         storage.Storage
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new batch controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	counter counter.Counter,
	store storage.Storage,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("batch_controller"),
		metricsScope:  scope.SubScope("batch_controller"),
		registry:      registry,
		counter:       counter,
		store:         store,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a batch delivery from the queue.
// Deserializes the request, groups into batch, and publishes to the score topic.
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

	c.logger.Infow("received batch event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Generate a globally unique batch ID.
	seq, err := c.counter.Next(ctx, "batch/"+request.Queue)
	if err != nil {
		c.logger.Errorw("failed to generate batch ID",
			"request_id", request.ID,
			"queue", request.Queue,
			"error", err,
		)
		c.metricsScope.Counter("counter_errors").Inc(1)
		return fmt.Errorf("failed to generate batch ID for queue=%s: %w", request.Queue, err)
	}

	batch := entity.Batch{
		ID:       fmt.Sprintf("%s/batch/%d", request.Queue, seq),
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	// Get active batches for this queue to set as dependencies.
	activeBatches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, []entity.BatchState{
		entity.BatchStateCreated,
		entity.BatchStateSpeculating,
		entity.BatchStateFinalizing,
	})
	if err != nil {
		c.logger.Errorw("failed to get active batches",
			"request_id", request.ID,
			"queue", request.Queue,
			"error", err,
		)
		c.metricsScope.Counter("batch_store_errors").Inc(1)
		return fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err)
	}

	for _, dep := range activeBatches {
		batch.Dependencies = append(batch.Dependencies, map[string]interface{}{
			"id":    dep.ID,
			"state": string(dep.State),
		})
	}

	// Create batch dependent entities (reverse relationship of batch.Dependencies).
	// For each dependency, record the new batch as a dependent.
	// If existing dependents are found in the store, append them.
	for _, dep := range activeBatches {
		bd := entity.BatchDependent{
			BatchID:    dep.ID,
			Dependents: []string{batch.ID},
		}

		existing, err := c.store.GetBatchDependentStore().Get(ctx, dep.ID)
		if err != nil && !storage.IsNotFound(err) {
			c.logger.Errorw("failed to get existing batch dependent",
				"batch_id", dep.ID,
				"error", err,
			)
			c.metricsScope.Counter("batch_dependent_store_errors").Inc(1)
			return fmt.Errorf("failed to get batch dependent for batchID=%s: %w", dep.ID, err)
		}
		if err == nil {
			bd.Dependents = append(existing.Dependents, bd.Dependents...)
		}
	}

	// TODO:
	// - Add batch to DB
	// - Add to batch dependent DB

	c.logger.Infow("batch created",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"dependency_count", len(batch.Dependencies),
	)

	// Publish to score topic
	if err := c.publish(ctx, consumer.TopicKeyScore, batch); err != nil {
		c.logger.Errorw("failed to publish output",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyScore,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to score: %w", err))
	}

	c.logger.Infow("published batch to score",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeyScore,
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
	return "batch"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
