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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
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
		queueConfigs: queueConfigs,
		registry:     registry,
	}
}

// Land handles the land request and returns a response
func (c *LandController) Land(ctx context.Context, req *pb.LandRequest) (resp *pb.LandResponse, retErr error) {
	const opName = "land"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	// Validate required fields.
	if req.Queue == "" {
		return nil, fmt.Errorf("LandController requires the request to have a queue name specified: %w", ErrInvalidRequest)
	}
	if req.Change == nil || len(req.Change.Uris) == 0 {
		return nil, fmt.Errorf("LandController requires the request to have at least one change URI specified: %w", ErrInvalidRequest)
	}

	change := change.Change{
		URIs: req.Change.GetUris(),
	}

	queue := req.Queue
	if _, err := c.queueConfigs.Get(ctx, queue); err != nil {
		if errors.Is(err, queueconfig.ErrNotFound) {
			return nil, errs.NewUserError(&UnrecognizedQueueError{Queue: queue})
		}
		return nil, fmt.Errorf("LandController failed to look up queue %q: %w", queue, err)
	}

	// TODO: pass default queue land strategy to resolver function to process a default.
	strategy, err := resolveMergeStrategy(req.Strategy)
	if err != nil {
		return nil, fmt.Errorf("LandController failed to map strategy for queue=%s: %w", req.Queue, err)
	}

	// Generate a globally unique request ID for the land request.
	seq, err := c.counter.Next(ctx, "request/"+queue)
	if err != nil {
		return nil, fmt.Errorf("LandController failed to generate request ID for queue=%s: %w", queue, err)
	}

	landRequest := entity.LandRequest{
		ID:           fmt.Sprintf("%s/%d", queue, seq),
		Queue:        queue,
		Change:       change,
		LandStrategy: strategy,
	}

	// Record the accepted status in the request log for reconciliation. Once the request materializes as a Request entity, the status might be updated to "new".
	// It is important to record the status before publishing to the queue for processing. It is important to publish straight to the database and not via a entityqueue.
	// Gateway has to stay consistent with the request log.
	logEntry := entity.NewRequestLog(landRequest.ID, entity.RequestStatusAccepted, 0, "", nil)
	if err := c.store.GetRequestLogStore().Insert(ctx, logEntry); err != nil {
		return nil, fmt.Errorf("LandController failed to insert request log for sqid=%s: %w", landRequest.ID, err)
	}

	c.logger.Debugw("land request created",
		"queue", req.Queue,
		"sqid", landRequest.ID,
		"change_uris", change.URIs,
		"change_count", len(change.URIs),
		"strategy", string(strategy),
	)

	// Publish to queue for async processing
	if err := c.publishToQueue(ctx, landRequest); err != nil {
		return nil, fmt.Errorf("LandController failed to publish request to queue: %w", err)
	}

	c.logger.Infow("request published to queue",
		"queue", req.Queue,
		"sqid", landRequest.ID,
		"topic_key", topickey.TopicKeyStart,
	)
	metrics.NamedCounter(c.metricsScope, opName, "publish_success", 1)

	return &pb.LandResponse{
		Sqid: landRequest.ID,
	}, nil
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

// resolveMergeStrategy maps a proto Strategy enum to the shared mergestrategy.MergeStrategy.
func resolveMergeStrategy(s pb.Strategy) (mergestrategy.MergeStrategy, error) {
	switch s {
	case pb.Strategy_DEFAULT:
		// TODO: resolve default strategy based on queue configuration
		return mergestrategy.MergeStrategyRebase, nil
	case pb.Strategy_REBASE:
		return mergestrategy.MergeStrategyRebase, nil
	case pb.Strategy_SQUASH_REBASE:
		return mergestrategy.MergeStrategySquashRebase, nil
	case pb.Strategy_MERGE:
		return mergestrategy.MergeStrategyMerge, nil
	default:
		return mergestrategy.MergeStrategyUnknown, fmt.Errorf("unknown land strategy in proto message: %v", s)
	}
}
