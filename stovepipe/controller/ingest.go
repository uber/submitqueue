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
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap"
)

// ErrInvalidRequest is returned when the request fails validation.
// This error should be mapped to codes.InvalidArgument at the gRPC layer.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest returns true if any error in the error chain is ErrInvalidRequest.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
}

// IngestController handles ingest business logic for stovepipe: it admits a queue's newly
// observed commit into the validation pipeline.
//
// This is the thin entry point. It mints a request ID namespaced by the queue and records the
// resulting Request. Resolving the commit URI via the SourceControl extension, persisting the
// Request, and publishing it onto the pipeline are deliberately out of scope for now.
type IngestController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	counter      counter.Counter
}

// NewIngestController creates a new instance of the stovepipe ingest controller.
func NewIngestController(logger *zap.SugaredLogger, scope tally.Scope, counter counter.Counter) *IngestController {
	return &IngestController{
		logger:       logger,
		metricsScope: scope.SubScope("ingest_controller"),
		counter:      counter,
	}
}

// Ingest admits a queue's newly observed commit into the validation pipeline and returns the minted request ID.
func (c *IngestController) Ingest(ctx context.Context, req *pb.IngestRequest) (resp *pb.IngestResponse, retErr error) {
	const opName = "ingest"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" {
		return nil, fmt.Errorf("IngestController requires the request to have a queue name specified: %w", ErrInvalidRequest)
	}

	queue := req.Queue

	// Generate a globally unique request ID namespaced by the queue. The counter domain
	// ("request/<queue>") doubles as the ID prefix, so the ID is "<domain>/<counter>".
	domain := "request/" + queue
	seq, err := c.counter.Next(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("IngestController failed to generate request ID for queue=%s: %w", queue, err)
	}

	request := entity.Request{
		ID:      fmt.Sprintf("%s/%d", domain, seq),
		Queue:   queue,
		State:   entity.RequestStateAccepted,
		Version: 1,
	}

	c.logger.Infow("accepted request",
		"id", request.ID,
		"queue", request.Queue,
		"state", request.State,
	)

	return &pb.IngestResponse{Id: request.ID}, nil
}
