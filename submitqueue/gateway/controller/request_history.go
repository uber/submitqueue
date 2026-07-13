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
	"sort"
	"strconv"
	"strings"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// RequestHistoryController handles retained request-log history lookups.
type RequestHistoryController struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	requestLogStore storage.RequestLogStore
	requestURIStore storage.RequestURIStore
}

// NewRequestHistoryController creates a gateway request-history controller.
func NewRequestHistoryController(logger *zap.SugaredLogger, scope tally.Scope, requestLogStore storage.RequestLogStore, requestURIStore storage.RequestURIStore) *RequestHistoryController {
	return &RequestHistoryController{
		logger:          logger,
		metricsScope:    scope.SubScope("request_history_controller"),
		requestLogStore: requestLogStore,
		requestURIStore: requestURIStore,
	}
}

// GetRequestHistoryByID returns every retained request-log event for one sqid.
func (c *RequestHistoryController) GetRequestHistoryByID(ctx context.Context, req entity.GetRequestHistoryByIDRequest) (logs []entity.RequestLog, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_id")
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("sqid", req.ID); err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByID invalid request: %w", err)
	}

	logs, err := c.requestLogStore.List(ctx, req.ID)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, errs.NewUserError(&RequestNotFoundError{Sqid: req.ID})
		}
		return nil, fmt.Errorf("GetRequestHistoryByID failed to list request logs sqid=%s: %w", req.ID, err)
	}

	c.logger.Debugw("request history retrieved",
		"sqid", req.ID,
		"event_count", len(logs),
	)
	return logs, nil
}

// GetRequestHistoryByChangeURI returns retained histories for an exact pinned change URI.
func (c *RequestHistoryController) GetRequestHistoryByChangeURI(ctx context.Context, req entity.GetRequestHistoryByChangeURIRequest) (result []entity.RequestHistory, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_by_change_uri")
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("change URI", req.ChangeURI); err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByChangeURI invalid request: %w", err)
	}

	mappings, err := c.requestURIStore.ListByURI(ctx, req.ChangeURI, maxChangeRequestResults+1)
	if err != nil {
		return nil, fmt.Errorf("GetRequestHistoryByChangeURI failed to list request mappings change_uri=%s: %w", req.ChangeURI, err)
	}
	if len(mappings) == 0 {
		return nil, errs.NewUserError(&RequestNotFoundError{ChangeURI: req.ChangeURI})
	}
	if len(mappings) > maxChangeRequestResults {
		return nil, errs.NewUserError(&TooManyChangeRequestsError{ChangeURI: req.ChangeURI, Limit: maxChangeRequestResults})
	}

	histories := make([]requestHistoryWithCounter, 0, len(mappings))
	for _, mapping := range mappings {
		logs, err := c.requestLogStore.List(ctx, mapping.RequestID)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("GetRequestHistoryByChangeURI failed to list request logs change_uri=%s sqid=%s: %w", req.ChangeURI, mapping.RequestID, err)
		}
		counter, err := sqidCounter(mapping.RequestID)
		if err != nil {
			return nil, &InternalConsistencyError{Message: fmt.Sprintf("invalid mapped sqid %q: %v", mapping.RequestID, err)}
		}
		histories = append(histories, requestHistoryWithCounter{
			counter: counter,
			history: entity.RequestHistory{
				RequestID: mapping.RequestID,
				Events:    append([]entity.RequestLog{}, logs...),
			},
		})
	}
	if len(histories) == 0 {
		return nil, errs.NewUserError(&RequestNotFoundError{ChangeURI: req.ChangeURI})
	}

	sort.Slice(histories, func(i, j int) bool {
		if histories[i].counter != histories[j].counter {
			return histories[i].counter < histories[j].counter
		}
		return histories[i].history.RequestID < histories[j].history.RequestID
	})

	result = make([]entity.RequestHistory, len(histories))
	for i, history := range histories {
		result[i] = history.history
	}
	c.logger.Debugw("request histories retrieved",
		"change_uri", req.ChangeURI,
		"request_count", len(result),
	)
	return result, nil
}

type requestHistoryWithCounter struct {
	counter int64
	history entity.RequestHistory
}

func sqidCounter(sqid string) (int64, error) {
	separator := strings.LastIndexByte(sqid, '/')
	if separator < 0 || separator == len(sqid)-1 {
		return 0, fmt.Errorf("missing numeric counter")
	}
	counter, err := strconv.ParseInt(sqid[separator+1:], 10, 64)
	if err != nil || counter <= 0 {
		return 0, fmt.Errorf("invalid numeric counter")
	}
	return counter, nil
}
