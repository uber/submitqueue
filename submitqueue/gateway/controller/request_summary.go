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
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// RequestSummaryController handles materialized request-summary lookups for the gateway.
type RequestSummaryController struct {
	logger              *zap.SugaredLogger
	metricsScope        tally.Scope
	requestSummaryStore storage.RequestSummaryStore
	requestURIStore     storage.RequestURIStore
}

// NewRequestSummaryController creates a gateway request-summary controller.
func NewRequestSummaryController(logger *zap.SugaredLogger, scope tally.Scope, requestSummaryStore storage.RequestSummaryStore, requestURIStore storage.RequestURIStore) *RequestSummaryController {
	return &RequestSummaryController{
		logger:              logger,
		metricsScope:        scope.SubScope("request_summary_controller"),
		requestSummaryStore: requestSummaryStore,
		requestURIStore:     requestURIStore,
	}
}

// GetRequestSummaryByID returns the current materialized view of one request.
func (c *RequestSummaryController) GetRequestSummaryByID(ctx context.Context, req entity.GetRequestSummaryByIDRequest) (summary entity.RequestSummary, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_id", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("sqid", req.ID); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("GetRequestSummaryByID invalid request: %w", err)
	}

	summary, err := c.requestSummaryStore.Get(ctx, req.ID)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.RequestSummary{}, errs.NewUserError(&RequestNotFoundError{Sqid: req.ID})
		}
		return entity.RequestSummary{}, fmt.Errorf("GetRequestSummaryByID failed to get request summary sqid=%s: %w", req.ID, err)
	}
	if summary.Status == entity.RequestStatusAccepting {
		return entity.RequestSummary{}, errs.NewUserError(&RequestNotFoundError{Sqid: req.ID})
	}

	c.logger.Debugw("request status retrieved",
		"sqid", req.ID,
		"status", string(summary.Status),
	)
	return summary, nil
}

// GetRequestSummaryByChangeURI returns current materialized views for an exact pinned change URI.
func (c *RequestSummaryController) GetRequestSummaryByChangeURI(ctx context.Context, req entity.GetRequestSummaryByChangeURIRequest) (summaries []entity.RequestSummary, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_change_uri", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("change URI", req.ChangeURI); err != nil {
		return nil, fmt.Errorf("GetRequestSummaryByChangeURI invalid request: %w", err)
	}

	mappings, err := c.requestURIStore.ListByURI(ctx, req.ChangeURI, maxChangeRequestResults+1)
	if err != nil {
		return nil, fmt.Errorf("GetRequestSummaryByChangeURI failed to list request mappings change_uri=%s: %w", req.ChangeURI, err)
	}
	if len(mappings) == 0 {
		return nil, errs.NewUserError(&RequestNotFoundError{ChangeURI: req.ChangeURI})
	}
	if len(mappings) > maxChangeRequestResults {
		return nil, errs.NewUserError(&TooManyChangeRequestsError{ChangeURI: req.ChangeURI, Limit: maxChangeRequestResults})
	}

	requests := make([]entity.RequestSummary, 0, len(mappings))
	for _, mapping := range mappings {
		summary, err := c.requestSummaryStore.Get(ctx, mapping.RequestID)
		if err != nil {
			if storage.IsNotFound(err) {
				return nil, &InternalConsistencyError{Message: fmt.Sprintf("request summary missing for mapped change URI %q and sqid %q", req.ChangeURI, mapping.RequestID)}
			}
			return nil, fmt.Errorf("GetRequestSummaryByChangeURI failed to get request summary change_uri=%s sqid=%s: %w", req.ChangeURI, mapping.RequestID, err)
		}
		requests = append(requests, summary)
	}

	c.logger.Debugw("request statuses retrieved",
		"change_uri", req.ChangeURI,
		"request_count", len(requests),
	)
	return requests, nil
}
