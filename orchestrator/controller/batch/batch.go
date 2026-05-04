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

package batch

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
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

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

	// Fetch request from storage
	request, err := c.store.GetRequestStore().Get(ctx, rid.ID)
	if err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to get request %s: %w", rid.ID, err)
	}

	c.logger.Infow("received batch event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: if capacity is full, wait here for other requests to accumulate to batch them together, or include a request into an existing batch if it's not too late.

	// Generate a globally unique batch ID.
	seq, err := c.counter.Next(ctx, "batch/"+request.Queue)
	if err != nil {
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

	// TODO: run Target Analyzer to understand new batch's dependency graph, run it against other active batches to understand the conflicts.
	// So far we'll just assume that the new batch conflicts with all active batches, which results in a serial non-parallelized queue.

	// Get active batches for this queue to set as dependencies.
	activeBatches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, []entity.BatchState{
		entity.BatchStateCreated,
		entity.BatchStateSpeculating,
		entity.BatchStateFinalizing,
	})
	if err != nil {
		c.metricsScope.Counter("batch_store_errors").Inc(1)
		return fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err)
	}

	for _, dep := range activeBatches {
		batch.Dependencies = append(batch.Dependencies, dep.ID)
	}

	// Create batch dependent entities (reverse relationship of batch.Dependencies).
	// For each dependency, record the new batch as a dependent.
	// If existing dependents are found in the store, append them.
	for _, dep := range activeBatches {
		// Get existing reverse index entry for the dependency.
		existing, err := c.store.GetBatchDependentStore().Get(ctx, dep.ID)
		if err != nil {
			c.metricsScope.Counter("batch_dependent_store_errors").Inc(1)
			return fmt.Errorf("failed to get batch dependent for batchID=%s: %w", dep.ID, err)
		}

		dependents := append(existing.Dependents, batch.ID)

		newVersion := existing.Version + 1
		if err := c.store.GetBatchDependentStore().UpdateDependents(ctx, dep.ID, existing.Version, newVersion, dependents); err != nil {
			c.metricsScope.Counter("batch_dependent_store_errors").Inc(1)
			return fmt.Errorf("failed to update batch dependent index for existing batchID=%s and new batchID=%s: %w", dep.ID, batch.ID, err)
		}
		existing.Version = newVersion
	}

	// Create new reverse index entry for the new batch. It would be empty for now, but will be updated as new batches are created that conflict with this batch.
	bd := entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{},
		Version:    1,
	}

	if err := c.store.GetBatchDependentStore().Create(ctx, bd); err != nil {
		c.metricsScope.Counter("batch_dependent_store_errors").Inc(1)
		return fmt.Errorf("failed to create batch dependent index for new batchID=%s: %w", batch.ID, err)
	}

	// Persist batch to storage.
	// This is the final operation that concludes the batch creation process. If it fails, BatchDependents will be pointing to a batch id that does not exist.
	// We do not reuse batch ids, a retry of this operation will create a new batch with a new ID. The downstream logic that operates on BatchDependent should be able to handle stale entries.
	if err := c.store.GetBatchStore().Create(ctx, batch); err != nil {
		c.metricsScope.Counter("batch_store_errors").Inc(1)
		return fmt.Errorf("failed to create batch in batch store: %w", err)
	}

	c.logger.Infow("batch created",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"dependency_count", len(batch.Dependencies),
	)

	// Publish to score topic for further processing.
	// If it fails and the controller retries, a new batch will be created with the new batch ID but the same request ID.
	// The downstream logic should be able to handle stale entries by looking at the state of the batch.
	if err := c.publish(ctx, consumer.TopicKeyScore, batch.ID, batch.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish batch ID to score topic: %w", err)
	}

	c.logger.Infow("published batch to score topic",
		"batch_id", batch.ID,
		"topic_key", consumer.TopicKeyScore,
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
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
