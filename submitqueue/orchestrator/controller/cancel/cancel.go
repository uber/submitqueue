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
//   - The request is already part of an active batch — the controller performs
//     a single intent CAS on the batch (advancing it to BatchStateCancelling)
//     and hands off to the speculate controller by publishing the batch ID to
//     TopicKeySpeculate. The speculate controller then owns: cancelling any
//     in-flight Build entity for the batch, fanning out to dependents, the
//     terminal CAS to BatchStateCancelled, and publishing to conclude. Cancel
//     does no terminal write and no downstream fan-out on the batch path.
//
// The split exists so that the terminal write and the work that must precede
// it (cancelling builds, respeculating dependents) live in the same controller
// — speculate is the single writer of every non-Cancelling batch state and is
// already wired with the build/dependent stores. Forward-progress controllers
// (score, build, buildsignal, merge) observe BatchStateCancelling via
// IsBatchStateHalted and short-circuit while speculate drives the batch to
// its terminal state.
//
// The controller is idempotent: re-delivery of the same CancelRequest after
// the terminal request transition is a no-op; re-delivery after the
// Cancelling write skips the mark-cancelling step and proceeds straight to
// the batch lookup. On the batch path, re-delivery against an already
// Cancelling batch re-publishes to TopicKeySpeculate (a cheap no-op nudge
// the speculate controller absorbs).
//
// Concurrent producers surface as storage.ErrVersionMismatch; the controller
// returns the wrapped error as-is and relies on the base controller layer to
// classify it as retryable so the next attempt sees the new state and takes
// the other branch. storage.ErrNotFound on the initial Get (the start
// controller has not yet persisted the request) is returned as-is for the
// same reason.
package cancel

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
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

// NewController creates a new cancel controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	// TODO(queue-aware): make this controller queue-aware during Process — derive the
	// queue from the loaded entity and use it for structured logging, metrics scoping,
	// and per-queue storage resolution. Today it uses the default store because the
	// queue is only known after the by-ID load.
	store, _ := stores.For("")
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
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	cancelReq, err := entity.CancelRequestFromBytes(msg.Payload)
	if err != nil {
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		return fmt.Errorf("failed to deserialize cancel request: %w", err)
	}

	request, err := c.store.GetRequestStore().Get(ctx, cancelReq.ID)
	if err != nil {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to get request %s: %w", cancelReq.ID, err)
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
		c.metricsScope.Counter("already_terminal").Inc(1)
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

	// Look for an active batch that already contains this request.
	batch, found, err := c.findActiveBatch(ctx, request)
	if err != nil {
		return err
	}

	if !found {
		return c.cancelRequest(ctx, request, cancelReq.Reason)
	}
	return c.cancelBatch(ctx, batch)
}

// markCancelling transitions the request to RequestStateCancelling (intent) if it
// isn't already in that state. Returns the updated request (with the post-CAS
// version and state) on success, or the original request on the idempotent path
// where the prior delivery already wrote Cancelling.
//
// storage.ErrVersionMismatch (a concurrent writer — most likely conclude
// observing a batch transition) is returned as-is for the base controller to
// classify and retry; the next attempt re-fetches and re-evaluates (it may now
// be terminal, in which case the top-level terminal-check acks).
func (c *Controller) markCancelling(ctx context.Context, request entity.Request) (entity.Request, error) {
	if request.State == entity.RequestStateCancelling {
		// Idempotent re-delivery: prior pass already recorded intent.
		c.metricsScope.Counter("already_cancelling").Inc(1)
		return request, nil
	}
	newVersion := request.Version + 1
	if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newVersion, entity.RequestStateCancelling); err != nil {
		c.metricsScope.Counter("request_update_errors").Inc(1)
		return entity.Request{}, fmt.Errorf("failed to mark request %s as cancelling: %w", request.ID, err)
	}
	request.Version = newVersion
	request.State = entity.RequestStateCancelling
	c.metricsScope.Counter("request_cancelling").Inc(1)
	return request, nil
}

// findActiveBatch scans all active batches in the request's queue for one whose
// Contains list includes the request. Returns (batch, true, nil) on a hit,
// (zero, false, nil) when the request is not yet batched, and any storage
// error otherwise.
//
// BatchStateCancelling is included in the active-state list so an idempotent
// redelivery of the cancel message (the prior pass wrote the intent but the
// speculate hand-off publish failed) still resolves the batch and re-attempts
// the publish.
func (c *Controller) findActiveBatch(ctx context.Context, request entity.Request) (entity.Batch, bool, error) {
	// TODO: Scans all the batches in flight - make it more efficient?
	active, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, request.Queue, []entity.BatchState{
		entity.BatchStateCreated,
		entity.BatchStateScored,
		entity.BatchStateSpeculating,
		entity.BatchStateMerging,
		entity.BatchStateCancelling,
	})
	if err != nil {
		c.metricsScope.Counter("batch_store_errors").Inc(1)
		return entity.Batch{}, false, fmt.Errorf("failed to get active batches for queue=%s: %w", request.Queue, err)
	}

	for _, b := range active {
		for _, rid := range b.Contains {
			if rid == request.ID {
				return b, true, nil
			}
		}
	}
	return entity.Batch{}, false, nil
}

// cancelRequest performs the terminal CAS (Cancelling → Cancelled) for a request
// that is not part of any active batch, and emits the RequestStatusCancelled log
// entry. storage.ErrVersionMismatch here means a concurrent writer (typically
// conclude after a racing batch terminal transition) advanced the request between
// our mark-cancelling CAS and this terminal CAS — returned as-is for the base
// controller to classify and retry; the next pass will observe the new state
// (likely terminal) and ack via the top-level terminal-check.
func (c *Controller) cancelRequest(ctx context.Context, request entity.Request, reason string) error {
	newVersion := request.Version + 1
	if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newVersion, entity.RequestStateCancelled); err != nil {
		c.metricsScope.Counter("request_update_errors").Inc(1)
		return fmt.Errorf("failed to cancel request %s: %w", request.ID, err)
	}

	metadata := map[string]string{}
	if reason != "" {
		metadata["reason"] = reason
	}
	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusCancelled, newVersion, "", metadata)
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		c.metricsScope.Counter("log_publish_errors").Inc(1)
		return fmt.Errorf("failed to publish cancel log for request %s: %w", request.ID, err)
	}

	c.logger.Infow("request cancelled (not batched)",
		"request_id", request.ID,
		"queue", request.Queue,
	)
	c.metricsScope.Counter("request_cancelled").Inc(1)
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
		newVersion := batch.Version + 1
		if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateCancelling); err != nil {
			c.metricsScope.Counter("batch_update_errors").Inc(1)
			// storage.ErrVersionMismatch here means the batch advanced concurrently
			// (e.g. speculate / merge progressed). Returned as-is for the base
			// controller to classify and retry; the re-fetch will see the new state
			// and either short-circuit (already terminal) or attempt the transition
			// again.
			return fmt.Errorf("failed to mark batch %s as cancelling: %w", batch.ID, err)
		}
		batch.Version = newVersion
		batch.State = entity.BatchStateCancelling
		c.metricsScope.Counter("batch_cancelling").Inc(1)
	} else {
		c.metricsScope.Counter("batch_already_cancelling").Inc(1)
	}

	if err := c.publishBatchID(ctx, consumer.TopicKeySpeculate, batch.ID, batch.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to hand off cancelled batch %s to speculate: %w", batch.ID, err)
	}

	c.metricsScope.Counter("batch_handed_off").Inc(1)
	return nil
}

// publishBatchID publishes a BatchID-payload message to the specified topic key.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
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
