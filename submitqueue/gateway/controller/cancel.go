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

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	requestcore "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// CancelController handles cancel business logic for the gateway. It validates the request,
// records a RequestStatusCancelling log entry (intent-only — cancellation is best-effort
// and may still race a successful merge), publishes a CancelRequest to the cancel topic,
// and returns a response. The orchestrator-side cancel controller performs the actual
// state transitions and emits the terminal RequestStatusCancelled log entry.
type CancelController struct {
	logger              *zap.SugaredLogger
	metricsScope        tally.Scope
	requestSummaryStore storage.RequestSummaryStore
	materializer        *requestcore.Materializer
	registry            consumer.TopicRegistry
}

// NewCancelController creates a new instance of the gateway cancel controller.
// The controller writes a RequestStatusCancelling log entry through the shared materializer and
// publishes cancel requests to the topic registered under topickey.TopicKeyCancel.
func NewCancelController(logger *zap.SugaredLogger, scope tally.Scope, store storage.Storage, registry consumer.TopicRegistry) *CancelController {
	return &CancelController{
		logger:              logger,
		metricsScope:        scope,
		requestSummaryStore: store.GetRequestSummaryStore(),
		materializer:        requestcore.NewMaterializer(store),
		registry:            registry,
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
func (c *CancelController) Cancel(ctx context.Context, req entity.CancelRequest) (retErr error) {
	const opName = "cancel"

	op := metrics.Begin(c.metricsScope, opName, metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if req.ID == "" {
		return fmt.Errorf("requires the request to have a sqid specified: %w", ErrInvalidRequest)
	}

	c.logger.Debugw("cancel request received",
		"sqid", req.ID,
		"reason", req.Reason,
	)

	// Verify the sqid exists before recording intent or publishing.
	if _, err := c.requestSummaryStore.Get(ctx, req.ID); err != nil {
		if storage.IsNotFound(err) {
			metrics.NamedCounter(c.metricsScope, opName, "not_found", 1)
			return errs.NewUserError(&RequestNotFoundError{Sqid: req.ID})
		}
		return fmt.Errorf("failed to look up request summary for sqid=%s: %w", req.ID, err)
	}

	// Record the user's intent in the request log before publishing. Writing direct to the
	// store (rather than via the log topic) keeps the gateway-emitted entry consistent with
	// the Land "accepted" entry and guarantees the entry is visible the moment Cancel returns.
	metadata := map[string]string{}
	if req.Reason != "" {
		metadata["reason"] = req.Reason
	}
	logEntry := entity.NewRequestLog(req.ID, entity.RequestStatusCancelling, 0, "", metadata)
	if err := c.materializer.PersistLog(ctx, logEntry); err != nil {
		return fmt.Errorf("failed to insert cancelling log for sqid=%s: %w", req.ID, err)
	}

	if err := c.publishToQueue(ctx, req); err != nil {
		return fmt.Errorf("failed to publish cancel request to queue: %w", err)
	}

	c.logger.Infow("cancel request published to queue",
		"sqid", req.ID,
		"topic_key", topickey.TopicKeyCancel,
	)

	return nil
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

	q, ok := c.registry.Queue(topickey.TopicKeyCancel)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyCancel)
	}

	topicName, ok := c.registry.TopicName(topickey.TopicKeyCancel)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyCancel)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish cancel request message: %w", err)
	}

	return nil
}
