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
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	requestcore "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// ErrInvalidRequest is returned when the request fails validation.
// This error should be mapped to codes.InvalidArgument at the gRPC layer.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// UnrecognizedQueueError indicates the request named a queue that is not
// present in the queue configuration store.
type UnrecognizedQueueError struct {
	Queue string
}

// Error implements the error interface.
func (e *UnrecognizedQueueError) Error() string {
	return fmt.Sprintf("unrecognized queue %q", e.Queue)
}

// IsUnrecognizedQueue returns true if any error in the chain is an
// *UnrecognizedQueueError.
func IsUnrecognizedQueue(err error) bool {
	var target *UnrecognizedQueueError
	return errors.As(err, &target)
}

// LandController handles land business logic for the gateway
type LandController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	counter      counter.Counter
	store        storage.Storage
	materializer *requestcore.Materializer
	queueConfigs queueconfig.Store
	registry     consumer.TopicRegistry
}

// NewLandController creates a new instance of the gateway land controller.
// The controller publishes land requests to the topic registered under
// topickey.TopicKeyStart in the registry.
func NewLandController(logger *zap.SugaredLogger, scope tally.Scope, counter counter.Counter, store storage.Storage, queueConfigs queueconfig.Store, registry consumer.TopicRegistry) *LandController {
	return &LandController{
		logger:       logger,
		metricsScope: scope.SubScope("land_controller"),
		counter:      counter,
		store:        store,
		materializer: requestcore.NewMaterializer(store),
		queueConfigs: queueConfigs,
		registry:     registry,
	}
}

// Land handles the land request and returns the ID assigned to the accepted request.
func (c *LandController) Land(ctx context.Context, req entity.LandRequest) (result entity.LandResult, retErr error) {
	const opName = "land"

	op := metrics.Begin(c.metricsScope, opName, metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	// Validate provider-agnostic request constraints before allocating an sqid.
	if err := validateQueueIdentifier(req.Queue); err != nil {
		return entity.LandResult{}, fmt.Errorf("invalid queue: %w", err)
	}
	if err := validateChangeURIs(req.Change.URIs); err != nil {
		return entity.LandResult{}, fmt.Errorf("invalid change URIs: %w", err)
	}

	queue := req.Queue
	if _, err := c.queueConfigs.Get(ctx, queue); err != nil {
		if errors.Is(err, queueconfig.ErrNotFound) {
			return entity.LandResult{}, errs.NewUserError(&UnrecognizedQueueError{Queue: queue})
		}
		return entity.LandResult{}, fmt.Errorf("failed to look up queue %q: %w", queue, err)
	}

	// Generate a globally unique request ID for the land request.
	// The inbound entity arrives with an empty ID; the controller owns minting it.
	seq, err := c.counter.Next(ctx, "request/"+queue)
	if err != nil {
		return entity.LandResult{}, fmt.Errorf("failed to generate request ID for queue=%s: %w", queue, err)
	}
	req.ID = fmt.Sprintf("%s/%d", queue, seq)
	if err := validateStoredIdentifier("generated sqid", req.ID); err != nil {
		return entity.LandResult{}, fmt.Errorf("generated invalid request ID for queue=%s: %w", queue, err)
	}

	receivedAtMs := time.Now().UnixMilli()
	summary := entity.RequestSummary{
		RequestID:         req.ID,
		Queue:             req.Queue,
		ChangeURIs:        append([]string{}, req.Change.URIs...),
		ReceivedAtMs:      receivedAtMs,
		Status:            entity.RequestStatusAccepting,
		StatusTimestampMs: receivedAtMs,
		Version:           1,
		Metadata:          map[string]string{},
	}
	if err := c.store.GetRequestSummaryStore().Create(ctx, summary); err != nil {
		return entity.LandResult{}, fmt.Errorf("failed to create request receipt sqid=%s: %w", req.ID, err)
	}

	// Publish before exposing the request as accepted. A failed publish leaves an
	// internal accepting receipt that public read APIs do not expose.
	if err := c.publishToQueue(ctx, req); err != nil {
		return entity.LandResult{}, fmt.Errorf("failed to publish request to queue: %w", err)
	}

	logEntry := entity.RequestLog{
		RequestID:   req.ID,
		TimestampMs: receivedAtMs,
		Status:      entity.RequestStatusAccepted,
		Metadata:    map[string]string{},
	}
	if err := c.materializer.PersistLog(ctx, logEntry); err != nil {
		// Publication is the Land success boundary. Later pipeline events repair
		// the accepting projection even if this accepted log is not persisted. If
		// the client retries after losing the response, the orchestrator rejects
		// the new sqid as a duplicate while the original request continues.
		c.logger.Errorw("failed to record accepted status after publishing request",
			"queue", req.Queue,
			"sqid", req.ID,
			"error", err,
		)
		metrics.NamedCounter(c.metricsScope, opName, "accepted_log_failure", 1)
	}

	c.logger.Debugw("land request created",
		"queue", req.Queue,
		"sqid", req.ID,
		"change_uris", req.Change.URIs,
		"change_count", len(req.Change.URIs),
		"strategy", string(req.LandStrategy),
	)

	c.logger.Infow("request published to queue",
		"queue", req.Queue,
		"sqid", req.ID,
		"topic_key", topickey.TopicKeyStart,
	)
	metrics.NamedCounter(c.metricsScope, opName, "publish_success", 1)

	return entity.LandResult{ID: req.ID}, nil
}

// publishToQueue publishes a land request to the request queue for async processing.
func (c *LandController) publishToQueue(ctx context.Context, landRequest entity.LandRequest) error {
	// Serialize land request entity to JSON
	payload, err := landRequest.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize land request: %w", err)
	}

	// Create queue message
	// - Message ID: landRequest.ID for idempotency
	// - Payload: serialized LandRequest entity
	// - Partition key: landRequest.Queue (ensures ordering per queue)
	msg := entityqueue.NewMessage(landRequest.ID, payload, landRequest.Queue, nil)

	q, ok := c.registry.Queue(topickey.TopicKeyStart)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyStart)
	}

	topicName, ok := c.registry.TopicName(topickey.TopicKeyStart)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyStart)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish land request message: %w", err)
	}

	return nil
}
