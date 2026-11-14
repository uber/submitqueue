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

package dlq

import (
	"context"
	"errors"
	"fmt"

	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corebatch "github.com/uber/submitqueue/submitqueue/orchestrator/core/batch"
	requestcore "github.com/uber/submitqueue/submitqueue/orchestrator/core/request"
	"go.uber.org/zap"
)

// RequestIDDecoder extracts a request's ID and queue from a request-scoped
// DLQ payload. Returning an empty ID is treated as a decode failure.
type RequestIDDecoder func(payload []byte) (entity.RequestID, error)

// DecodeLandRequestID extracts the request ID from a LandRequest payload
// (the shape used by the start topic).
func DecodeLandRequestID(payload []byte) (entity.RequestID, error) {
	lr, err := sqmq.UnmarshalLandRequest(payload)
	if err != nil {
		return entity.RequestID{}, err
	}
	return entity.RequestID{ID: lr.ID, Queue: lr.Queue}, nil
}

// DecodeCancelRequestID extracts the request identity from a cancellation payload.
func DecodeCancelRequestID(payload []byte) (entity.RequestID, error) {
	request, err := sqmq.UnmarshalCancelRequest(payload)
	if err != nil {
		return entity.RequestID{}, err
	}
	return entity.RequestID{ID: request.ID, Queue: request.Queue}, nil
}

// DecodeRequestID extracts the request ID from a validate or batch payload.
// primary is the originating topic (TopicKeyValidate or TopicKeyBatch), not the DLQ key.
func DecodeRequestID(primary consumer.TopicKey) RequestIDDecoder {
	return func(payload []byte) (entity.RequestID, error) {
		return sqmq.UnmarshalRequestID(primary, payload)
	}
}

// requestController is the DLQ reconciler for request-scoped pipeline stages.
// It uses the matching decoder for cancel, validate, or batch and fails the
// request unless a batch already owns its outcome.
type requestController struct {
	logger               *zap.SugaredLogger
	metricsScope         tally.Scope
	stores               orchstorage.Factory
	registry             consumer.TopicRegistry
	decode               RequestIDDecoder
	topicKey             consumer.TopicKey
	consumerGroup        string
	retainMissingRequest bool
}

// Verify requestController implements consumer.Controller at compile time.
var _ consumer.Controller = (*requestController)(nil)

// NewDLQRequestController builds a DLQ controller for a request-scoped topic.
// topicKey must be the DLQ topic key (typically TopicKey(primary)); decode
// must match the payload shape of the primary topic this DLQ drains.
func NewDLQRequestController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores orchstorage.Factory,
	registry consumer.TopicRegistry,
	decode RequestIDDecoder,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	return newRequestController(logger, scope, stores, registry, decode, topicKey, consumerGroup)
}

// NewDLQCancelController reconciles only Error; missing Requests remain durable until they exist.
func NewDLQCancelController(logger *zap.SugaredLogger, scope tally.Scope, stores orchstorage.Factory, registry consumer.TopicRegistry, topicKey consumer.TopicKey, consumerGroup string) consumer.Controller {
	controller := newRequestController(logger, scope, stores, registry, DecodeCancelRequestID, topicKey, consumerGroup)
	controller.retainMissingRequest = true
	return controller
}

func newRequestController(logger *zap.SugaredLogger, scope tally.Scope, stores orchstorage.Factory, registry consumer.TopicRegistry, decode RequestIDDecoder, topicKey consumer.TopicKey, consumerGroup string) *requestController {
	name := string(topicKey) + "_controller"
	return &requestController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		registry:      registry,
		decode:        decode,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for a request-scoped topic.
func (c *requestController) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	rid, err := c.decode(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		// Malformed DLQ payload is non-retryable: a re-delivery will decode the
		// same bytes and fail the same way. The framework will route this DLQ
		// message to its own DLQ if one is configured; otherwise the message is
		// acked and dropped after the error is logged.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rid.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		return nil
	}
	if rid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: rid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rid.Queue, err)
	}

	lastError, failureMeta := failureContext(delivery)
	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", rid.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if c.retainMissingRequest {
		err := c.reconcileCancelRequest(ctx, delivery, store, rid.ID, lastError, failureMeta)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1, metrics.TagsFromContext(ctx)...)
		}
		return err
	}
	owned, err := c.requestHasOwningBatch(ctx, store, rid.ID)
	if err != nil || owned {
		return err
	}
	if err := failRequest(ctx, store, c.registry, c.logger, rid.ID, lastError, failureMeta); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	return nil
}

func (c *requestController) reconcileCancelRequest(ctx context.Context, delivery consumer.Delivery, store orchstorage.Storage, requestID, lastError string, metadata map[string]string) error {
	request, err := store.GetRequestStore().Get(ctx, requestID)
	if errors.Is(err, storage.ErrNotFound) {
		metrics.NamedCounter(c.metricsScope, "process", "waiting_for_request", 1, metrics.TagsFromContext(ctx)...)
		delivery.Hold(waitForRequestDelayMs)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to load request for cancel reconciliation: %w", err)
	}
	if request.State == entity.RequestStateError {
		return requestcore.ResendTerminalRequestLog(ctx, c.registry, request)
	}
	if entity.IsRequestStateTerminal(request.State) {
		return nil
	}
	owned, err := c.requestHasOwningBatch(ctx, store, requestID)
	if err != nil || owned {
		return err
	}
	request.State = entity.RequestStateError
	newVersion := request.Version + 1
	// The version observed before owner lookup fences a later batch claim.
	if err := store.GetRequestStore().Update(ctx, request, request.Version, newVersion); err != nil {
		return fmt.Errorf("failed to fail cancelled request %s: %w", requestID, err)
	}
	request.Version = newVersion
	entry := entity.NewRequestStatusLog(request.Queue, request.ID, entity.RequestStatusError, request.Version, lastError, metadata)
	return requestcore.PublishLog(ctx, c.registry, entry, request.ID, "")
}

func (c *requestController) requestHasOwningBatch(ctx context.Context, store orchstorage.Storage, requestID string) (bool, error) {
	owner, err := owningBatch(ctx, store, requestID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "batch_lookup_errors", 1, metrics.TagsFromContext(ctx)...)
		return false, err
	}
	if owner == "" {
		return false, nil
	}
	metrics.NamedCounter(c.metricsScope, "process", "skipped_owned_by_batch", 1, metrics.TagsFromContext(ctx)...)
	c.logger.Infow("dlq reconcile: batch owns the request outcome, leaving completion to conclude", "request_id", requestID, "batch_id", owner)
	return true, nil
}

const waitForRequestDelayMs int64 = 1000

// owningBatch returns a batch that is still running or whose result has not been recorded on the request yet.
// Creating batches must have claimed the request; failed batches do not stop it from being marked Error.
func owningBatch(ctx context.Context, store orchstorage.Storage, requestID string) (string, error) {
	batches, _, err := corebatch.FindByRequestID(ctx, store, requestID)
	if err != nil {
		return "", err
	}

	owner := ""
	needsClaim := false
	for _, batch := range batches {
		switch {
		case batch.State == entity.BatchStateFailed:
		case batch.State == entity.BatchStateSucceeded || batch.State == entity.BatchStateCancelled:
			owner, needsClaim = batch.ID, false
		case batch.State != entity.BatchStateCreating:
			return batch.ID, nil
		case owner == "":
			owner, needsClaim = batch.ID, true
		}
	}
	if owner == "" {
		return "", nil
	}

	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) && needsClaim {
			return "", nil
		}
		if errors.Is(err, storage.ErrNotFound) {
			return owner, nil
		}
		return "", err
	}
	if entity.IsRequestStateTerminal(request.State) || needsClaim && request.State != entity.RequestStateBatched {
		return "", nil
	}
	return owner, nil
}

// Name returns the controller name for logging and metrics.
func (c *requestController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *requestController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *requestController) ConsumerGroup() string {
	return c.consumerGroup
}
