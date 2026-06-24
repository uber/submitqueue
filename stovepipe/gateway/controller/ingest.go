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
	pb "github.com/uber/submitqueue/api/stovepipe/gateway/protopb"
	"github.com/uber/submitqueue/platform/base/change"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// ErrInvalidRequest is returned when the request fails validation.
// This error should be mapped to codes.InvalidArgument at the gRPC layer.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// IngestController handles ingest business logic for the stovepipe gateway.
type IngestController struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	counter         counter.Counter
	requestLogStore storage.RequestLogStore
	registry        consumer.TopicRegistry
}

// NewIngestController creates a new instance of the stovepipe ingest controller.
// The controller records an accepted entry in the request log before publishing
// ingest requests to the topic registered under topickey.TopicKeyStart in the registry.
func NewIngestController(logger *zap.SugaredLogger, scope tally.Scope, counter counter.Counter, requestLogStore storage.RequestLogStore, registry consumer.TopicRegistry) *IngestController {
	return &IngestController{
		logger:          logger,
		metricsScope:    scope.SubScope("ingest_controller"),
		counter:         counter,
		requestLogStore: requestLogStore,
		registry:        registry,
	}
}

// Ingest validates the request, generates a SPID, publishes the ingest request
// to the pipeline queue, and returns the SPID for tracking.
func (c *IngestController) Ingest(ctx context.Context, req *pb.IngestRequest) (resp *pb.IngestResponse, retErr error) {
	const opName = "ingest"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" {
		return nil, fmt.Errorf("IngestController requires the request to have a queue name specified: %w", ErrInvalidRequest)
	}
	if req.Change == nil || len(req.Change.Uris) == 0 {
		return nil, fmt.Errorf("IngestController requires the request to have at least one change URI specified: %w", ErrInvalidRequest)
	}

	queue := req.Queue

	seq, err := c.counter.Next(ctx, "ingest/"+queue)
	if err != nil {
		return nil, fmt.Errorf("IngestController failed to generate SPID for queue=%s: %w", queue, err)
	}

	ingestRequest := entity.IngestRequest{
		ID:     fmt.Sprintf("%s/%d", queue, seq),
		Queue:  queue,
		Change: change.Change{URIs: req.Change.GetUris()},
	}

	c.logger.Debugw("ingest request created",
		"queue", queue,
		"spid", ingestRequest.ID,
		"change_uris", ingestRequest.Change.URIs,
	)

	// Record the accepted status in the request log for reconciliation before publishing to the
	// queue for processing. The gateway is the sole owner of the request log and must record the
	// status synchronously (written straight to storage, not via the queue) so it stays consistent
	// with what callers observe the moment Ingest returns.
	logEntry := entity.NewRequestLog(ingestRequest.ID, entity.RequestStatusAccepted, 0, "", nil)
	if err := c.requestLogStore.Insert(ctx, logEntry); err != nil {
		return nil, fmt.Errorf("IngestController failed to insert request log for spid=%s: %w", ingestRequest.ID, err)
	}

	if err := c.publishToQueue(ctx, ingestRequest); err != nil {
		return nil, fmt.Errorf("IngestController failed to publish request to queue: %w", err)
	}

	c.logger.Infow("ingest request published to queue",
		"queue", queue,
		"spid", ingestRequest.ID,
		"topic_key", topickey.TopicKeyStart,
	)
	metrics.NamedCounter(c.metricsScope, opName, "publish_success", 1)

	return &pb.IngestResponse{
		Spid: ingestRequest.ID,
	}, nil
}

// publishToQueue serializes the ingest request and publishes it to the start topic.
func (c *IngestController) publishToQueue(ctx context.Context, ingestRequest entity.IngestRequest) error {
	payload, err := ingestRequest.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize ingest request: %w", err)
	}

	msg := entityqueue.NewMessage(ingestRequest.ID, payload, ingestRequest.Queue, nil)

	q, ok := c.registry.Queue(topickey.TopicKeyStart)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyStart)
	}

	topicName, ok := c.registry.TopicName(topickey.TopicKeyStart)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyStart)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish ingest request message: %w", err)
	}

	return nil
}
