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

//go:generate mockgen -source=request_history.go -destination=mock/request_history_mock.go -package=mock

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
	GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) (entity.RequestHistory, error)
	GetRequestHistoryByURI(ctx context.Context, req entity.GetRequestHistoryByURIRequest) ([]entity.RequestHistory, error)
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

// GetRequestHistoryByID returns retained events and the current summary for one request ID.
func (c *requestHistoryController) GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) (history entity.RequestHistory, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_id", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	history, retErr = c.readHistoryByID(ctx, req)
	if retErr != nil {
		return entity.RequestHistory{}, retErr
	}
	c.logger.Debugw("request history retrieved",
		"request_id", req.ID,
		"queue", req.Queue,
		"event_count", len(history.Events),
	)
	return history, nil
}

func (c *requestHistoryController) readHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) (entity.RequestHistory, error) {
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByID invalid queue=%q: %w", req.Queue, err)
	}
	if err := validateRequestID(req.ID); err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByID invalid request_id=%q queue=%q: %w", req.ID, req.Queue, err)
	}

	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByID failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	history, err := readRequestHistory(ctx, stores, req.Queue, req.ID)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.RequestHistory{}, errs.NewUserError(&RequestHistoryByIDNotFoundError{RequestID: req.ID})
		}
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByID failed to read history request_id=%s: %w", req.ID, err)
	}

	return history, nil
}

// GetRequestHistoryByURI returns the retained history mapped to an exact commit URI.
func (c *requestHistoryController) GetRequestHistoryByURI(ctx context.Context, req entity.GetRequestHistoryByURIRequest) (histories []entity.RequestHistory, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_uri", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	history, retErr := c.readHistoryByURI(ctx, req)
	if retErr != nil {
		return nil, retErr
	}
	c.logger.Debugw("request history retrieved by URI",
		"uri", req.URI,
		"request_id", history.RequestID,
		"queue", req.Queue,
		"event_count", len(history.Events),
	)
	return []entity.RequestHistory{history}, nil
}

func (c *requestHistoryController) readHistoryByURI(ctx context.Context, req entity.GetRequestHistoryByURIRequest) (entity.RequestHistory, error) {
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI invalid queue=%q: %w", req.Queue, err)
	}
	if err := validateHistoryIdentifier("URI", req.URI); err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI invalid uri=%q queue=%q: %w", req.URI, req.Queue, err)
	}

	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	requestID, err := stores.GetRequestURIStore().GetIDByURI(ctx, req.URI)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.RequestHistory{}, errs.NewUserError(&RequestHistoryByURINotFoundError{URI: req.URI})
		}
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI failed to resolve request URI %s: %w", req.URI, err)
	}

	history, err := readRequestHistory(ctx, stores, req.Queue, requestID)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI found URI mapping without retained request logs uri=%q request_id=%q: %w", req.URI, requestID, err)
		}
		return entity.RequestHistory{}, fmt.Errorf("GetRequestHistoryByURI failed to read history uri=%s request_id=%s: %w", req.URI, requestID, err)
	}

	if history.RequestSummary.URI != req.URI {
		return entity.RequestHistory{}, &RequestHistoryConsistencyError{Queue: req.Queue, RequestID: requestID, Reason: "URI mapping disagrees with request summary"}
	}
	return history, nil
}

func readRequestHistory(ctx context.Context, stores storage.Storage, queue, requestID string) (entity.RequestHistory, error) {
	logs, err := stores.GetRequestLogStore().List(ctx, requestID)
	if err != nil {
		return entity.RequestHistory{}, fmt.Errorf("failed to list request logs: %w", err)
	}
	summary, err := stores.GetRequestSummaryStore().Get(ctx, requestID)
	if err != nil {
		if storage.IsNotFound(err) {
			// The initial log is persisted before its summary; this visibility gap is retryable.
			return entity.RequestHistory{}, errs.NewRetryableError(fmt.Errorf("request summary is not visible queue=%q request_id=%q", queue, requestID))
		}
		return entity.RequestHistory{}, fmt.Errorf("failed to read request summary: %w", err)
	}
	if summary.RequestID != requestID || summary.Queue != queue {
		return entity.RequestHistory{}, &RequestHistoryConsistencyError{Queue: queue, RequestID: requestID, Reason: "request summary disagrees with history selector"}
	}
	return entity.RequestHistory{RequestID: requestID, Events: logs, RequestSummary: summary}, nil
}
