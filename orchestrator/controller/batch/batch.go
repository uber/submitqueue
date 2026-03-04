package batch

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
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

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
	counterOp := metrics.Begin(c.metricsScope, "counter_next")
	seq, err := c.counter.Next(ctx, "batch/"+request.Queue)
	counterOp.Complete(err)
	if err != nil {
		c.logger.Errorw("failed to generate batch ID",
			"request_id", request.ID,
			"queue", request.Queue,
			"error", err,
		)
		return errs.NewRetryableError(fmt.Errorf("failed to generate batch ID for queue=%s: %w", request.Queue, err))
	}

	batch := entity.Batch{
		ID:       fmt.Sprintf("%s/batch/%d", request.Queue, seq),
		Queue:    request.Queue,
		Contains: []string{request.ID},
		State:    entity.BatchStateCreated,
		Version:  1,
	}

	// Persist batch to storage.
	// ErrAlreadyExists should never happen since batch IDs are generated from a unique counter.
	batchCreateOp := metrics.Begin(c.metricsScope, "batch_store_create")
	if err := c.store.GetBatchStore().Create(ctx, batch); err != nil {
		batchCreateOp.Complete(err)
		c.logger.Errorw("failed to create batch in storage",
			"batch_id", batch.ID,
			"error", err,
		)
		if errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("unexpected duplicate batch ID=%s: %w", batch.ID, err)
		}
		return errs.NewRetryableError(fmt.Errorf("failed to create batch: %w", err))
	}
	batchCreateOp.Complete(nil)

	// Get active batches for this queue to set as dependencies.
	getActiveOp := metrics.Begin(c.metricsScope, "batch_store_get_active")
	activeBatches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, []entity.BatchState{
		entity.BatchStateReady,
		entity.BatchStateSpeculating,
		entity.BatchStateFinalizing,
	})
	getActiveOp.Complete(err)
	if err != nil {
		c.logger.Errorw("failed to get active batches",
			"request_id", request.ID,
			"queue", request.Queue,
			"error", err,
		)
		return errs.NewRetryableError(fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err))
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
			Version:    1,
		}

		// Get existing dependents for this batch (already in store)
		getDepOp := metrics.Begin(c.metricsScope, "batch_dependent_get")
		existing, err := c.store.GetBatchDependentStore().Get(ctx, dep.ID)
		if err != nil && !storage.IsNotFound(err) {
			getDepOp.Complete(err)
			c.logger.Errorw("failed to get existing batch dependent",
				"batch_id", dep.ID,
				"error", err,
			)
			return errs.NewRetryableError(fmt.Errorf("failed to get batch dependent for batchID=%s: %w", dep.ID, err))
		}
		getDepOp.Complete(nil)

		if err == nil {
			// Existing record found — update with merged dependents list.
			// Note: existing.Dependents may have batches that are in "new" state
			// indicating errors in previous batch creation pipeline. "new"
			// should not be considered an active state for further processing. The
			// callers of the batch dependents store should check for this.
			bd.Dependents = append(existing.Dependents, bd.Dependents...)
			updateOp := metrics.Begin(c.metricsScope, "batch_dependent_update")
			if err := c.store.GetBatchDependentStore().UpdateDependents(ctx, dep.ID, existing.Version, bd.Dependents); err != nil {
				updateOp.Complete(err)
				c.logger.Errorw("failed to update batch dependent",
					"batch_id", dep.ID,
					"error", err,
				)
				return errs.NewRetryableError(fmt.Errorf("failed to update batch dependent for batchID=%s: %w", dep.ID, err))
			}
			updateOp.Complete(nil)
			c.logger.Debugw("updated batch dependent",
				"batch_id", dep.ID,
				"dependent_count", len(bd.Dependents),
			)
		} else {
			// No existing record — create new batch dependent entry.
			createDepOp := metrics.Begin(c.metricsScope, "batch_dependent_create")
			createErr := c.store.GetBatchDependentStore().Create(ctx, bd)
			if createErr != nil && !errors.Is(createErr, storage.ErrAlreadyExists) {
				createDepOp.Complete(createErr)
				c.logger.Errorw("failed to create batch dependent",
					"batch_id", dep.ID,
					"error", createErr,
				)
				return errs.NewRetryableError(fmt.Errorf("failed to create batch dependent for batchID=%s: %w", dep.ID, createErr))
			}
			createDepOp.Complete(nil)
			c.logger.Debugw("created batch dependent",
				"batch_id", dep.ID,
				"dependent_batch_id", batch.ID,
			)
		}
	}

	// Transition batch state from created to ready.
	updateStateOp := metrics.Begin(c.metricsScope, "batch_store_update_state")
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, entity.BatchStateReady); err != nil {
		updateStateOp.Complete(err)
		c.logger.Errorw("failed to update batch state to ready",
			"batch_id", batch.ID,
			"error", err,
		)
		return errs.NewRetryableError(fmt.Errorf("failed to update batch state to ready: %w", err))
	}
	updateStateOp.Complete(nil)

	c.logger.Infow("batch ready",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"dependency_count", len(batch.Dependencies),
	)

	// Publish to score topic
	publishOp := metrics.Begin(c.metricsScope, "publish")
	if err := c.publish(ctx, consumer.TopicKeyScore, batch); err != nil {
		publishOp.Complete(err)
		c.logger.Errorw("failed to publish output",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyScore,
			"error", err,
		)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to score: %w", err))
	}
	publishOp.Complete(nil)

	c.logger.Infow("published batch to score",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeyScore,
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
