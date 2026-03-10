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

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles conclude queue messages.
// It consumes batches and completes the pipeline.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
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
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("conclude_controller"),
		metricsScope:  scope.SubScope("conclude_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a conclude delivery from the queue.
// Deserializes the batch and completes the pipeline processing.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize batch entity
	batch, err := entity.BatchFromBytes(msg.Payload)
	if err != nil {
		// Non-retryable: malformed messages will never succeed regardless of retry count
		metrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch: %w", err)
	}

	c.logger.Infow("received conclude event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// TODO: Handle cancellation

	// Map batch terminal state to request state.
	// We expect the batch to be in a terminal state
	// as updated by the merge controller.
	requestState, err := batchStateToRequestState(batch.State)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "unexpected_state_errors", 1)
		return fmt.Errorf("unexpected batch state %q for batch %s: %w", batch.State, batch.ID, err)
	}

	// Update each request's state to reflect the batch outcome.
	for _, requestID := range batch.Contains {
		request, err := c.store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "request_store_errors", 1)
			return fmt.Errorf("failed to get request %s: %w", requestID, err)
		}

		if err := c.store.GetRequestStore().UpdateState(ctx, requestID, request.Version, requestState); err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "request_update_errors", 1)
			return fmt.Errorf("failed to update request %s state to %s: %w", requestID, requestState, err)
		}

		c.logger.Infow("updated request state",
			"batch_id", batch.ID,
			"request_id", requestID,
			"new_state", string(requestState),
		)
	}

	return nil // Success - message will be acked
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

// batchStateToRequestState maps a terminal batch state to the corresponding request state.
func batchStateToRequestState(state entity.BatchState) (entity.RequestState, error) {
	switch state {
	case entity.BatchStateSucceeded:
		return entity.RequestStateLanded, nil
	case entity.BatchStateFailed:
		return entity.RequestStateError, nil
	default:
		return entity.RequestStateUnknown, fmt.Errorf("non-terminal batch state: %s", state)
	}
}
