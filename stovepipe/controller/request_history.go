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
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// RequestHistoryController handles retained request-history lookups.
type RequestHistoryController interface {
	GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error)
}

var _ RequestHistoryController = (*requestHistoryController)(nil)

type requestHistoryController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	stores       storage.Factory
}

// NewRequestHistoryController creates a request-history controller.
func NewRequestHistoryController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory) RequestHistoryController {
	return &requestHistoryController{
		logger:       logger,
		metricsScope: scope.SubScope("request_history_controller"),
		stores:       stores,
	}
}

// GetRequestHistoryByID returns every retained log event for one request ID.
func (c *requestHistoryController) GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) (logs []entity.RequestLog, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_id", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	logs, retErr = c.readHistoryByID(ctx, req)
	if retErr != nil {
		return nil, retErr
	}
	c.logger.Debugw("request history retrieved",
		"request_id", req.ID,
		"queue", req.Queue,
		"event_count", len(logs),
	)
	return logs, nil
}

func (c *requestHistoryController) readHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) ([]entity.RequestLog, error) {
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByID invalid queue: %w", err)
	}
	if err := validateHistoryIdentifier("request ID", req.ID); err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByID invalid request: %w", err)
	}

	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByID failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	logs, err := stores.GetRequestLogStore().List(ctx, req.ID)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, errs.NewUserError(&RequestHistoryNotFoundError{RequestID: req.ID})
		}
		return nil, fmt.Errorf("GetRequestHistoryByID failed to list request logs request_id=%s: %w", req.ID, err)
	}

	return logs, nil
}
