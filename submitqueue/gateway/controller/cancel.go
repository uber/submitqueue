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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/errs"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// CancelController handles cancel business logic for the gateway. It validates the request,
// records a RequestStatusCancelling log entry (intent-only — cancellation is best-effort
// and may still race a successful merge), publishes a CancelRequest to the cancel topic,
// and returns a response. The orchestrator-side cancel controller performs the actual
// state transitions and emits the terminal RequestStatusCancelled log entry.
type CancelController struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	requestLogStore storage.RequestLogStore
	registry        consumer.TopicRegistry
}

// NewCancelController creates a new instance of the gateway cancel controller.
// The controller writes a RequestStatusCancelling log entry through requestLogStore and
// publishes cancel requests to the topic registered under consumer.TopicKeyCancel.
func NewCancelController(logger *zap.SugaredLogger, scope tally.Scope, requestLogStore storage.RequestLogStore, registry consumer.TopicRegistry) *CancelController {
	return &CancelController{
		logger:          logger,
		metricsScope:    scope,
		requestLogStore: requestLogStore,
		registry:        registry,
	}
}

// Cancel handles a cancel request and returns an empty response. The actual cancellation
// is performed asynchronously by the orchestrator cancel controller. Cancel is idempotent:
// the orchestrator treats already-terminal requests as a no-op.
//
// Cancellation is best-effort: a request that has already merged or that races to
// completion before the cancel propagates may still land. The RequestStatusCancelling
// entry written here records the user's intent; the terminal outcome is reflected by a
// later RequestStatusCancelled (orchestrator side) or RequestStatusLanded entry.
func (c *CancelController) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("cancel_request_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("cancel_request_count").Inc(1)

	if req.Sqid == "" {
		return nil, fmt.Errorf("CancelController requires the request to have a sqid specified: %w", ErrInvalidRequest)
	}

	cancelRequest := entity.CancelRequest{
		ID:     req.Sqid,
		Reason: req.Reason,
	}

	c.logger.Debugw("cancel request received",
		"sqid", cancelRequest.ID,
		"reason", cancelRequest.Reason,
	)

	// Verify the sqid exists before recording intent or publishing. Cancel is opt-in
	// by sqid; an unknown sqid is a user error and must never leave a cancelling log
	// row or a queue message behind for a request that never existed. The Land
	// controller writes its "accepted" log entry synchronously to the same store, so
	// a NotFound here reliably means "this sqid was never accepted by the gateway"
	// rather than "in flight" — there is no false-negative race window.
	if _, err := c.requestLogStore.List(ctx, cancelRequest.ID); err != nil {
		if storage.IsNotFound(err) {
			c.metricsScope.Counter("cancel_request_not_found").Inc(1)
			return nil, errs.NewUserError(&RequestNotFoundError{Sqid: cancelRequest.ID})
		}
		return nil, fmt.Errorf("CancelController failed to look up request log for sqid=%s: %w", cancelRequest.ID, err)
	}

	// Record the user's intent in the request log before publishing. Writing direct to the
	// store (rather than via the log topic) keeps the gateway-emitted entry consistent with
	// the Land "accepted" entry and guarantees the entry is visible the moment Cancel returns.
	metadata := map[string]string{}
	if cancelRequest.Reason != "" {
		metadata["reason"] = cancelRequest.Reason
	}
	logEntry := entity.NewRequestLog(cancelRequest.ID, entity.RequestStatusCancelling, 0, "", metadata)
	if err := c.requestLogStore.Insert(ctx, logEntry); err != nil {
		return nil, fmt.Errorf("CancelController failed to insert cancelling log for sqid=%s: %w", cancelRequest.ID, err)
	}

	if err := c.publishToQueue(ctx, cancelRequest); err != nil {
		return nil, fmt.Errorf("CancelController failed to publish cancel request to queue: %w", err)
	}

	c.logger.Infow("cancel request published to queue",
		"sqid", cancelRequest.ID,
		"topic_key", consumer.TopicKeyCancel,
	)
	c.metricsScope.Counter("cancel_publish_success").Inc(1)

	return &pb.CancelResponse{}, nil
}

// publishToQueue publishes a cancel request to the cancel queue for async processing.
func (c *CancelController) publishToQueue(ctx context.Context, cancelRequest entity.CancelRequest) error {
	payload, err := cancelRequest.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize cancel request: %w", err)
	}

	// Partition by the sqid so retries and reorderings on the same request are serialised.
	// TODO: figure best way to ID and partition the message according to new guidelines on queue usage
	msg := entityqueue.NewMessage(cancelRequest.ID, payload, cancelRequest.ID, nil)

	q, ok := c.registry.Queue(consumer.TopicKeyCancel)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", consumer.TopicKeyCancel)
	}

	topicName, ok := c.registry.TopicName(consumer.TopicKeyCancel)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", consumer.TopicKeyCancel)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish cancel request message: %w", err)
	}

	return nil
}
