package merge

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/landprovider"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles merge queue messages.
// It consumes batches, fetches the contained requests from storage,
// lands the changes via the LandProvider, and publishes results to the merge signal stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	landProvider  landprovider.LandProvider
	storage       storage.Storage
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new merge controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	landProvider landprovider.LandProvider,
	storage storage.Storage,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("merge_controller"),
		metricsScope:  scope.SubScope("merge_controller"),
		registry:      registry,
		landProvider:  landProvider,
		storage:       storage,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a merge delivery from the queue.
// Extracts the batch ID from the message, fetches the batch from storage,
// lands the changes, and publishes the batch ID to the merge signal topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()
	batchID := string(msg.Payload)

	c.logger.Infow("received merge event",
		"batch_id", batchID,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	if batchID == "" {
		c.logger.Errorw("empty batch ID in message",
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
		)
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		return fmt.Errorf("empty batch ID in message %s", msg.ID)
	}

	// Fetch the batch from storage — this is the single source of truth.
	// On retries (e.g., land succeeded but publish failed), reading current
	// state prevents calling Land again for changes that were already merged.
	batch, err := c.storage.GetBatchStore().Get(ctx, batchID)
	if err != nil {
		c.logger.Errorw("failed to fetch batch",
			"batch_id", batchID,
			"error", err,
		)
		c.metricsScope.Counter("batch_fetch_errors").Inc(1)
		return fmt.Errorf("failed to fetch batch %s: %w", batchID, err)
	}

	switch batch.State {
	case entity.BatchStateLandSucceeded, entity.BatchStateLandFailed:
		c.logger.Infow("batch already landed, skipping to publish",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		c.metricsScope.Counter("idempotent_skip").Inc(1)
	case entity.BatchStateLanding:
		newState, err := c.land(ctx, batch)
		if err != nil {
			return err
		}

		if err := c.storage.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newState); err != nil {
			c.logger.Errorw("failed to update batch state",
				"batch_id", batch.ID,
				"target_state", string(newState),
				"error", err,
			)
			c.metricsScope.Counter("batch_update_errors").Inc(1)
			return fmt.Errorf("failed to update batch state: %w", err)
		}

		batch.State = newState
		batch.Version++
	default:
		c.logger.Errorw("unexpected batch state for merge",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		c.metricsScope.Counter("unexpected_state").Inc(1)
		return fmt.Errorf("unexpected batch state %s for batch %s", batch.State, batch.ID)
	}

	// Publish batch ID to merge signal topic
	if err := c.publish(ctx, consumer.TopicKeyMergeSignal, batch.ID, batch.Queue); err != nil {
		c.logger.Errorw("failed to publish output",
			"batch_id", batch.ID,
			"topic_key", consumer.TopicKeyMergeSignal,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to merge-signal: %w", err)
	}

	c.logger.Infow("published batch to next stage",
		"batch_id", batch.ID,
		"state", string(batch.State),
		"topic_key", consumer.TopicKeyMergeSignal,
	)

	c.metricsScope.Counter("processed").Inc(1)
	return nil
}

// land fetches the requests in the batch, lands them via the LandProvider,
// and classifies the outcome into a batch state.
func (c *Controller) land(ctx context.Context, batch entity.Batch) (entity.BatchState, error) {
	requestStore := c.storage.GetRequestStore()
	entries := make([]entity.LandEntry, 0, len(batch.Contains))

	for _, requestID := range batch.Contains {
		request, err := requestStore.Get(ctx, requestID)
		if err != nil {
			c.logger.Errorw("failed to fetch request",
				"batch_id", batch.ID,
				"request_id", requestID,
				"error", err,
			)
			c.metricsScope.Counter("request_fetch_errors").Inc(1)
			return "", fmt.Errorf("failed to fetch request %s: %w", requestID, err)
		}

		entries = append(entries, entity.LandEntry{
			Strategy: request.LandStrategy,
			Change:   request.Change,
		})
	}

	err := c.landProvider.Land(ctx, batch.Queue, entries)

	switch {
	case err == nil:
		c.logger.Infow("land succeeded",
			"batch_id", batch.ID,
		)
		return entity.BatchStateLandSucceeded, nil
	case landprovider.IsLandRejected(err):
		c.logger.Errorw("land rejected",
			"batch_id", batch.ID,
			"error", err,
		)
		c.metricsScope.Counter("land_rejected").Inc(1)
		return entity.BatchStateLandFailed, nil
	default:
		c.logger.Errorw("land failed",
			"batch_id", batch.ID,
			"error", err,
		)
		c.metricsScope.Counter("land_errors").Inc(1)
		return "", fmt.Errorf("land failed: %w", err)
	}
}

// publish publishes a batch ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, id string, partitionKey string) error {
	msg := entityqueue.NewMessage(id, []byte(id), partitionKey, nil)

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
	return "merge"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
