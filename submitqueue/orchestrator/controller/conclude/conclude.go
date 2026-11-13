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

package conclude

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	corebatch "github.com/uber/submitqueue/submitqueue/orchestrator/core/batch"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

// Controller handles conclude queue messages.
// It consumes batches and completes the pipeline.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new conclude controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores orchstorage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("conclude_controller"),
		metricsScope:  scope.SubScope("conclude_controller"),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a conclude delivery from the queue.
// Deserializes the batch and completes the pipeline processing.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	bid, err := sqmq.UnmarshalBatchID(c.topicKey, msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, bid.Queue); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	// Fetch batch from storage
	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, "process", "queue_mismatch", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}

	c.logger.Infow("received conclude event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	return corebatch.ConcludeBatchRequests(ctx, store, c.registry, c.logger, c.metricsScope, batch, msg.Metadata[topickey.MetadataKeyFailureReason])
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "conclude"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
