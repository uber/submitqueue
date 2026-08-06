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

// Package cancel implements the orchestrator-side cancel controller.
//
// The controller consumes CancelRequest messages from the cancel topic and
// records the user's cancellation intent. Two distinct paths exist after the
// request-level intent (RequestStateCancelling) is recorded:
//
//   - The request has not yet been enrolled into a batch — the controller
//     transitions Cancelling → RequestStateCancelled directly and emits a
//     RequestStatusCancelled log entry. This path is fully owned by the cancel
//     controller.
//
//   - The request is associated with one or more batch attempts — the controller
//     records cancellation intent on every cancellable attempt and hands each one
//     to speculate. Creating attempts are ignored because their reverse indexes
//     may be incomplete, while Merging and terminal attempts retain their existing
//     outcome for conclude to reconcile.
//
// The split exists so that the terminal write and the work that must precede
// it (cancelling builds, respeculating dependents) live in the same controller
// — speculate is the single writer of every non-Cancelling batch state and is
// already wired with the build/dependent stores. Forward-progress controllers
// (build, buildsignal, merge) observe BatchStateCancelling via
// IsBatchStateHalted and short-circuit while speculate drives the batch to
// its terminal state.
//
// The controller is idempotent: re-delivery of the same CancelRequest after
// the terminal request transition is a no-op. Re-delivery after a Cancelling
// write skips that CAS, finds every batch ID containing the request again, and
// republishes matching attempts already in BatchStateCancelling.
//
// Concurrent producers surface as the intrinsically retryable
// storage.ErrVersionMismatch; the controller returns the wrapped error as-is
// so the next attempt sees the new state and takes the other branch.
// storage.ErrNotFound on the initial Get (the start
// controller has not yet persisted the request) is returned as-is for the
// same reason.
package cancel

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles cancel queue messages. Implements consumer.Controller.
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

const opName = "process"

// NewController creates a new cancel controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("cancel_controller"),
		metricsScope:  scope.SubScope("cancel_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a cancel delivery from the queue.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	cancelReq, err := entity.CancelRequestFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize cancel request: %w", err)
	}

	request, err := c.store.GetRequestStore().Get(ctx, cancelReq.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", cancelReq.ID, err)
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if cancelReq.Queue != "" && cancelReq.Queue != request.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", cancelReq.Queue, request.Queue, request.ID)
	}

	c.logger.Infow("received cancel event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"reason", cancelReq.Reason,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	if entity.IsRequestStateTerminal(request.State) {
		metrics.NamedCounter(c.metricsScope, opName, "already_terminal", 1)
		return nil
	}

	// Step 1: record the cancellation intent on the request itself by transitioning
	// to RequestStateCancelling. This is non-terminal; forward-progress controllers
	// (validate, batch) treat it as halted, but conclude may still write a different
	// terminal state if a concurrent merge or failure wins the race.
	request, err = c.markCancelling(ctx, request)
	if err != nil {
		return err
	}

	// Find every batch associated with this request. Retries may create multiple batch IDs, and each persisted attempt must be handled.
	batches, err := c.findBatches(ctx, request)
	if err != nil {
		return err
	}

	// Return the first failure so the existing linear error-classification chain remains intact.
	// Matches are sorted by ID below, making the selected error deterministic while every failure is still logged and counted.
	var firstErr error
	foundApplicableBatch := false
	for _, batch := range batches {
		switch {
		case batch.State.IsCancellable():
			foundApplicableBatch = true
			if err := c.cancelBatch(ctx, batch); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "batch_cancel_errors", 1)
				c.logger.Errorw("failed to cancel batch",
					"batch_id", batch.ID,
					"batch_state", string(batch.State),
					"error", err,
				)
				if firstErr == nil {
					firstErr = err
				}
			}
		case batch.State == entity.BatchStateMerging:
			// Merge owns the outcome once it has started. Conclude will reconcile the request with that outcome.
			foundApplicableBatch = true
			metrics.NamedCounter(c.metricsScope, opName, "batch_merging", 1)
		case batch.State.IsTerminal():
			// The terminal batch outcome wins; conclude may not have reconciled the request yet.
			foundApplicableBatch = true
			metrics.NamedCounter(c.metricsScope, opName, "batch_already_terminal", 1)
		}
	}

	if !foundApplicableBatch {
		return c.cancelRequest(ctx, request, cancelReq.Reason)
	}
	return firstErr
}

// markCancelling transitions the request to RequestStateCancelling (intent) if it
// isn't already in that state. Returns the updated request (with the post-CAS
// version and state) on success, or the original request on the idempotent path
// where the prior delivery already wrote Cancelling.
//
// storage.ErrVersionMismatch (a concurrent writer — most likely conclude
// observing a batch transition) is returned as-is; its declaration makes it
// retryable, and the next attempt re-fetches and re-evaluates (it may now
// be terminal, in which case the top-level terminal-check acks).
func (c *Controller) markCancelling(ctx context.Context, request entity.Request) (entity.Request, error) {
	if request.State == entity.RequestStateCancelling {
		// Idempotent re-delivery: prior pass already recorded intent.
		metrics.NamedCounter(c.metricsScope, opName, "already_cancelling", 1)
		return request, nil
	}
	newVersion := request.Version + 1
	request.State = entity.RequestStateCancelling
	if err := c.store.GetRequestStore().Update(ctx, request, request.Version, newVersion); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_update_errors", 1)
		return entity.Request{}, fmt.Errorf("failed to mark request %s as cancelling: %w", request.ID, err)
	}
	request.Version = newVersion
	metrics.NamedCounter(c.metricsScope, opName, "request_cancelling", 1)
	return request, nil
}

// findBatches resolves every batch attempt associated with the request.
// Associations whose batch was never persisted are stale retry artifacts and are ignored.
func (c *Controller) findBatches(ctx context.Context, request entity.Request) ([]entity.Batch, error) {
	associations, err := c.store.GetRequestBatchStore().GetByRequestID(ctx, request.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_batch_store_errors", 1)
		return nil, fmt.Errorf("failed to get batch associations for request %s: %w", request.ID, err)
	}

	var batches []entity.Batch
	for _, association := range associations {
		batch, err := c.store.GetBatchStore().Get(ctx, association.BatchID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// The association may precede batch persistence or may outlive a failed attempt.
				// If the batch is later persisted and published, speculate re-checks the contained request state before starting work.
				metrics.NamedCounter(c.metricsScope, opName, "stale_batch_associations", 1)
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
			return nil, fmt.Errorf("failed to get associated batch %s for request %s: %w", association.BatchID, request.ID, err)
		}
		batches = append(batches, batch)
	}

	// The batches are independent, but deterministic order stabilizes logs, tests, and first-error selection.
	sort.Slice(batches, func(i, j int) bool {
		return batches[i].ID < batches[j].ID
	})
	return batches, nil
}

// cancelRequest drives the terminal transition (Cancelling → Cancelled) for a
// request that is not part of any active batch, and emits the RequestStatusCancelled log entry.
// It delegates the CAS-plus-log to the shared TerminateRequest helper,
// which re-reads the request before the terminal write.
//
// A storage.ErrVersionMismatch surfaced by the helper means a concurrent writer
// (typically conclude after a racing batch terminal transition) advanced the
// request between our mark-cancelling CAS and this terminal CAS; it is returned
// as-is because the sentinel is intrinsically retryable, and the next pass will
// observe the new state and ack via the top-level terminal-check. If that
// concurrent writer already reached a *different* terminal state, the helper
// reports TerminationDiverged and we simply ack — the other writer owns the
// terminal log for the state it wrote.
func (c *Controller) cancelRequest(ctx context.Context, request entity.Request, reason string) error {
	metadata := map[string]string{}
	if reason != "" {
		metadata["reason"] = reason
	}
	res, err := corerequest.TerminateRequest(ctx, c.store, c.registry, request.ID, entity.RequestStateCancelled, "", metadata)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_terminate_errors", 1)
		return fmt.Errorf("failed to cancel request %s: %w", request.ID, err)
	}

	switch res.Outcome {
	case corerequest.TerminationOutcomeSuccess, corerequest.TerminationOutcomeAlreadyInTargetState:
		c.logger.Infow("request cancelled (not batched)",
			"request_id", request.ID,
			"queue", request.Queue,
		)
		metrics.NamedCounter(c.metricsScope, opName, "request_cancelled", 1)
	case corerequest.TerminationOutcomeDiverged:
		c.logger.Infow("request reached a different terminal state before cancel, skipping",
			"request_id", request.ID,
			"queue", request.Queue,
			"actual_state", string(res.BeforeState),
		)
		metrics.NamedCounter(c.metricsScope, opName, "request_terminal_divergence", 1)
	case corerequest.TerminationOutcomeNotFound:
		metrics.NamedCounter(c.metricsScope, opName, "request_store_errors", 1)
		return fmt.Errorf("request %s not found during cancel: %w", request.ID, storage.ErrNotFound)
	}
	return nil
}

// cancelBatch records the cancellation intent on the batch and hands off to
// the speculate controller. It performs the intent CAS (Cancelling) if not
// already in that state, then publishes the batch ID to TopicKeySpeculate.
//
// The speculate controller owns everything from there: cancelling any
// in-flight Build entity for the batch, fanning out to dependents, the
// terminal CAS to BatchStateCancelled, and publishing to conclude. Cancel
// does not perform a terminal write and does not emit per-request logs on
// the batch path (the gateway already wrote RequestStatusCancelling with
// the reason at intent time; conclude writes the terminal log entries when
// it reconciles request state).
//
// On idempotent redelivery the batch may already be in BatchStateCancelling
// (a prior pass wrote the intent but the publish failed). In that case the
// intent CAS is skipped and we just re-publish — speculate absorbs the
// duplicate as a cheap no-op nudge.
func (c *Controller) cancelBatch(ctx context.Context, batch entity.Batch) error {
	c.logger.Infow("handing batch cancellation off to speculate",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"batch_state", string(batch.State),
	)

	if batch.State != entity.BatchStateCancelling {
		var err error
		batch, err = corebatch.Transition(ctx, c.store, batch, entity.BatchStateCancelling)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_update_errors", 1)
			// storage.ErrVersionMismatch here means the batch advanced concurrently
			// (e.g. speculate / merge progressed). Returned as-is because the
			// sentinel is intrinsically retryable; the re-fetch will see the new state
			// and either short-circuit (already terminal) or attempt the transition
			// again.
			return fmt.Errorf("failed to mark batch %s as cancelling: %w", batch.ID, err)
		}
		metrics.NamedCounter(c.metricsScope, opName, "batch_cancelling", 1)
	} else {
		metrics.NamedCounter(c.metricsScope, opName, "batch_already_cancelling", 1)
		// A prior pass wrote the intent but may have crashed before completing
		// the membership record move; repair before re-publishing.
		if err := corebatch.EnsureRecord(ctx, c.store, batch); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_update_errors", 1)
			return err
		}
	}

	if err := c.publishBatchID(ctx, topickey.TopicKeySpeculate, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to hand off cancelled batch %s to speculate: %w", batch.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "batch_handed_off", 1)
	return nil
}

// publishBatchID publishes a BatchID-payload message to the specified topic
// key, stamped with and partitioned by the batch's queue.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, batchID string, queue string) error {
	bid := entity.BatchID{ID: batchID, Queue: queue}
	payload, err := bid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(batchID, payload, queue, nil)

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
	return "cancel"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
