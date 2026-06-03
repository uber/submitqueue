// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package score

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	corerequest "github.com/uber/submitqueue/core/request"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/scorer"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles score queue messages.
// It consumes batches, scores them using the provided scorer, persists the score,
// and publishes to the speculate stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	scorer        scorer.Scorer
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new score controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	scorer scorer.Scorer,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("score_controller"),
		metricsScope:  scope.SubScope("score_controller"),
		store:         store,
		scorer:        scorer,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a score delivery from the queue.
// Deserializes the batch, scores each request's change using the scorer,
// persists the minimum score, publishes request log entries,
// and publishes to the speculate topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize batch ID from payload
	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	// Fetch batch from storage
	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	c.logger.Infow("received score event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit when the batch is in BatchStateCancelling — the cancel
	// controller has handed the batch off to speculate, which owns the terminal
	// write to Cancelled and the downstream dependent / conclude publishes. We
	// must not race it to conclude (conclude requires terminal). Silently ack.
	if batch.State == entity.BatchStateCancelling {
		c.metricsScope.Counter("skipped_cancelling").Inc(1)
		c.logger.Infow("skipping score for cancelling batch",
			"batch_id", batch.ID,
		)
		return nil
	}

	// Short-circuit if the batch is already terminal. Score never writes a
	// terminal state, so it owns no recovery here: whichever controller wrote
	// the terminal state (speculate.cancelBatch / failOnDependency, or merge)
	// already published to conclude, and speculate's terminal self-heal
	// republishes conclude on every redelivery of a terminal batch. Silently
	// ack — same pattern as build / buildsignal on halted.
	if batch.State.IsTerminal() {
		c.metricsScope.Counter("skipped_terminal").Inc(1)
		c.logger.Infow("skipping score for terminal batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// Score each request's change and take the minimum (worst-case) as the batch score
	batchScore, err := c.scoreBatch(ctx, batch)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "scorer_errors", 1)
		return fmt.Errorf("failed to score batch %s: %w", batch.ID, err)
	}

	// Atomically update score and state to "scored" in the database
	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateScoreAndState(ctx, batch.ID, batch.Version, newVersion, batchScore, entity.BatchStateScored); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update score for batch %s: %w", batch.ID, err)
	}
	batch.Version = newVersion

	c.logger.Infow("scored batch",
		"batch_id", batch.ID,
		"score", batchScore,
	)

	// Publish request log entries for all requests in the batch
	if err := corerequest.PublishBatchLogs(ctx, c.registry, batch.Contains, entity.RequestStatusScored, map[string]string{
		"batch_id": batch.ID,
		"score":    fmt.Sprintf("%.4f", batchScore),
	}); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request logs for batch %s: %w", batch.ID, err)
	}

	// Publish to speculate topic
	if err := c.publish(ctx, consumer.TopicKeySpeculate, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to speculate: %w", err)
	}

	c.logger.Infow("published batch to speculate",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeySpeculate,
	)

	return nil // Success - message will be acked
}

// scoreBatch scores each request's change in the batch and returns the combined probability.
// Uses multiplicative probability: if any single request fails, the entire batch fails,
// so the batch score is the product of individual request scores.
func (c *Controller) scoreBatch(ctx context.Context, batch entity.Batch) (float64, error) {
	score := 1.0
	for _, requestID := range batch.Contains {
		request, err := c.store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return 0, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		s, err := c.scorer.Score(ctx, request.Change)
		if err != nil {
			return 0, fmt.Errorf("failed to score request %s: %w", requestID, err)
		}
		score *= s
	}
	return score, nil
}

// publish publishes a batch ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
	bid := entity.BatchID{ID: batchID}
	payload, err := bid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(batchID, payload, partitionKey, nil)

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
