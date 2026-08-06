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
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
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
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	// Deserialize batch ID from payload
	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	// Fetch batch from storage
	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, "process", "queue_mismatch", 1)
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

	// Map batch terminal state to request state.
	// We expect the batch to be in a terminal state as written by the merge
	// controller (Succeeded) or the speculate controller (Failed via
	// failOnDependency, Cancelled via cancelBatch).
	requestState, err := batchStateToRequestState(batch.State)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "unexpected_state_errors", 1)
		return fmt.Errorf("unexpected batch state %q for batch %s: %w", batch.State, batch.ID, err)
	}

	// Reconcile each request to the batch's terminal state and emit a terminal
	// log entry. TerminateRequest is idempotent under at-least-once delivery: a
	// prior attempt may have completed the CAS but failed before publishing the
	// log, so the log publish still runs when the request is already in the
	// target terminal state, and a request that a concurrent path already drove
	// to a different terminal state is left untouched. A request referenced by
	// the batch but missing from the store is a hard error — the whole batch is
	// retried (and eventually dead-lettered) rather than silently skipped. We
	// translate the result into per-outcome logs and metrics.
	for _, requestID := range batch.Contains {
		res, err := corerequest.TerminateRequest(ctx, c.store, c.registry, requestID, requestState, "", map[string]string{
			"batch_id": batch.ID,
		})
		if err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "terminate_errors", 1)
			return fmt.Errorf("failed to terminate request %s for batch %s: %w", requestID, batch.ID, err)
		}

		switch res.Outcome {
		case corerequest.TerminationOutcomeSuccess:
			c.logger.Infow("updated request state",
				"batch_id", batch.ID,
				"request_id", requestID,
				"new_state", string(res.AfterState),
			)
		case corerequest.TerminationOutcomeAlreadyInTargetState:
			metrics.NamedCounter(c.metricsScope, "process", "already_reconciled", 1)
		case corerequest.TerminationOutcomeDiverged:
			c.logger.Warnw("request already in different terminal state, skipping reconcile",
				"batch_id", batch.ID,
				"request_id", requestID,
				"expected_state", string(requestState),
				"actual_state", string(res.BeforeState),
			)
			metrics.NamedCounter(c.metricsScope, "process", "terminal_state_divergence", 1)
		case corerequest.TerminationOutcomeNotFound:
			metrics.NamedCounter(c.metricsScope, "process", "request_store_errors", 1)
			return fmt.Errorf("failed to get request %s for batch %s: %w", requestID, batch.ID, storage.ErrNotFound)
		}
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
	case entity.BatchStateCancelled:
		return entity.RequestStateCancelled, nil
	default:
		return entity.RequestStateUnknown, fmt.Errorf("non-terminal batch state: %s", state)
	}
}
