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
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process", metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

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
	requestStatus, err := requestStateToStatus(requestState)
	if err != nil {
		// Unreachable: batchStateToRequestState only returns terminal request states.
		return fmt.Errorf("failed to map request state %s to status: %w", requestState, err)
	}

	// Reconcile each request to the batch's terminal state and emit a terminal
	// log entry. The flow is idempotent under at-least-once delivery: a prior
	// attempt may have completed the CAS but failed before publishing the log,
	// so the log publish must still run when the request is already in the
	// target terminal state.
	for _, requestID := range batch.Contains {
		request, err := c.store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "request_store_errors", 1)
			return fmt.Errorf("failed to get request %s: %w", requestID, err)
		}

		switch {
		case request.State == requestState:
			// Idempotent retry: a prior delivery already wrote the terminal
			// state. Skip the CAS and fall through to the log publish.
			metrics.NamedCounter(c.metricsScope, "process", "already_reconciled", 1)
		case entity.IsRequestStateTerminal(request.State):
			// Divergent terminal state — a concurrent path (e.g. a racing
			// cancel-not-yet-batched transition) reached terminal first. Skip
			// the reconcile and the log publish; the other writer owns the
			// terminal log entry for the state it actually wrote.
			c.logger.Warnw("request already in different terminal state, skipping reconcile",
				"batch_id", batch.ID,
				"request_id", requestID,
				"actual_state", string(request.State),
				"expected_state", string(requestState),
			)
			metrics.NamedCounter(c.metricsScope, "process", "terminal_state_divergence", 1)
			continue
		default:
			newVersion := request.Version + 1
			if err := c.store.GetRequestStore().UpdateState(ctx, requestID, request.Version, newVersion, requestState); err != nil {
				metrics.NamedCounter(c.metricsScope, "process", "request_update_errors", 1)
				return fmt.Errorf("failed to update request %s state to %s: %w", requestID, requestState, err)
			}
			request.Version = newVersion
			request.State = requestState

			c.logger.Infow("updated request state",
				"batch_id", batch.ID,
				"request_id", requestID,
				"new_state", string(requestState),
			)
		}

		logEntry := entity.NewRequestLog(requestID, requestStatus, request.Version, "", map[string]string{
			"batch_id": batch.ID,
		})
		if err := corerequest.PublishLog(ctx, c.registry, logEntry, requestID); err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "log_publish_errors", 1)
			return fmt.Errorf("failed to publish request log for %s: %w", requestID, err)
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

// requestStateToStatus maps a terminal request state to the corresponding log status.
func requestStateToStatus(state entity.RequestState) (entity.RequestStatus, error) {
	switch state {
	case entity.RequestStateLanded:
		return entity.RequestStatusLanded, nil
	case entity.RequestStateError:
		return entity.RequestStatusError, nil
	case entity.RequestStateCancelled:
		return entity.RequestStatusCancelled, nil
	default:
		return entity.RequestStatusUnknown, fmt.Errorf("non-terminal request state: %s", state)
	}
}
