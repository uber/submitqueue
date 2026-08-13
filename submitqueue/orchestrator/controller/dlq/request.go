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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// RequestIDDecoder extracts the affected request's identity — its ID and its
// queue — from the raw payload bytes of a DLQ message. Different primary
// topics carry different payload shapes (LandRequest on start, CancelRequest
// on cancel, RequestID on validate / batch), so the caller injects the right
// decoder for the topic being reconciled. Returning an empty ID is treated as
// a decode failure.
type RequestIDDecoder func(payload []byte) (entity.RequestID, error)

// DecodeLandRequestID extracts the request ID from a LandRequest payload
// (the shape used by the start topic).
func DecodeLandRequestID(payload []byte) (entity.RequestID, error) {
	lr, err := entity.LandRequestFromBytes(payload)
	if err != nil {
		return entity.RequestID{}, err
	}
	return entity.RequestID{ID: lr.ID, Queue: lr.Queue}, nil
}

// DecodeCancelRequestID extracts the request ID from a CancelRequest payload
// (the shape used by the cancel topic).
func DecodeCancelRequestID(payload []byte) (entity.RequestID, error) {
	cr, err := entity.CancelRequestFromBytes(payload)
	if err != nil {
		return entity.RequestID{}, err
	}
	return entity.RequestID{ID: cr.ID, Queue: cr.Queue}, nil
}

// DecodeRequestID extracts the request ID from a RequestID payload (the shape
// used by the validate and batch topics).
func DecodeRequestID(payload []byte) (entity.RequestID, error) {
	return entity.RequestIDFromBytes(payload)
}

// requestController is the DLQ reconciler for request-scoped pipeline stages.
// It is registered once per primary request-scoped topic (start, cancel,
// validate, batch) with the matching decoder. On each delivery it decodes the
// request ID and transitions the request to RequestStateError, unless a live
// batch already owns the request's outcome.
type requestController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	registry      consumer.TopicRegistry
	decode        RequestIDDecoder
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify requestController implements consumer.Controller at compile time.
var _ consumer.Controller = (*requestController)(nil)

// NewDLQRequestController builds a DLQ controller for a request-scoped topic.
// topicKey must be the DLQ topic key (typically TopicKey(primary)); decode
// must match the payload shape of the primary topic this DLQ drains.
func NewDLQRequestController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	decode RequestIDDecoder,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
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
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Malformed DLQ payload is non-retryable: a re-delivery will decode the
		// same bytes and fail the same way. The framework will route this DLQ
		// message to its own DLQ if one is configured; otherwise the message is
		// acked and dropped after the error is logged.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if rid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(storage.Config{QueueName: rid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
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

	// A batch that got as far as enrolling the request owns its outcome, and
	// conclude writes that outcome when the batch finishes. This dead letter can
	// be a redundant attempt whose predecessor is already speculating — the batch
	// stage mints one batch per delivery — and TerminateRequest does not guard
	// against Batched, so failing the request here would overwrite a live batch's
	// claim and leave it building for a request reported as failed.
	owner, err := owningBatch(ctx, store, rid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_lookup_errors", 1)
		return err
	}
	if owner != "" {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_owned_by_batch", 1)
		c.logger.Infow("dlq reconcile: request is carried by a live batch, leaving the outcome to conclude",
			"request_id", rid.ID,
			"batch_id", owner,
		)
		return nil
	}

	if err := failRequest(ctx, store, c.registry, c.logger, rid.ID, lastError, failureMeta); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
		return err
	}

	return nil
}

// owningBatch returns a batch that owns the request's outcome, or the empty
// string if none does.
//
// A non-terminal batch past Creating owns the request outright: conclude writes
// its outcome when the batch finishes. Creating is the ambiguous case, because
// the association is written before the claim that enrols the request, so it
// also survives an abandoned attempt — one whose claim lost to cancel, or found
// the request already halted. Such a batch is inert: nothing promotes it and no
// conclude will ever run for it, so letting it answer "owned" would suppress
// the reconcile forever and strand the request.
//
// The request's own state separates the two, because the claim is the only
// writer of Batched: a Creating batch alongside a Batched request is
// mid-promotion and owns it, and any other request state means the claim never
// landed. A request that has since disappeared is left to failRequest, which
// reports the missing row.
func owningBatch(ctx context.Context, store storage.Storage, requestID string) (string, error) {
	batches, _, err := corebatch.FindByRequestID(ctx, store, requestID)
	if err != nil {
		return "", err
	}

	creating := ""
	for _, batch := range batches {
		switch {
		case batch.State.IsTerminal():
		case batch.State != entity.BatchStateCreating:
			return batch.ID, nil
		case creating == "":
			creating = batch.ID
		}
	}
	if creating == "" {
		return "", nil
	}

	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	if request.State != entity.RequestStateBatched {
		return "", nil
	}
	return creating, nil
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
