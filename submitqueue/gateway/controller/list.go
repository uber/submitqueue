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
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

const (
	defaultListPageSize = 50
	maxListPageSize     = 200
)

type listPageToken struct {
	Queue               string
	ReceivedAtOrAfterMs int64
	ReceivedBeforeMs    int64
	LastReceivedAtMs    int64
	LastRequestID       string
}

// ListController handles bounded queue receipt-history queries.
type ListController struct {
	logger                   *zap.SugaredLogger
	metricsScope             tally.Scope
	requestQueueSummaryStore storage.RequestQueueSummaryStore
	queueConfigs             queueconfig.Store
}

// NewListController creates a gateway list controller.
func NewListController(logger *zap.SugaredLogger, scope tally.Scope, requestQueueSummaryStore storage.RequestQueueSummaryStore, queueConfigs queueconfig.Store) *ListController {
	return &ListController{
		logger:                   logger,
		metricsScope:             scope.SubScope("list_controller"),
		requestQueueSummaryStore: requestQueueSummaryStore,
		queueConfigs:             queueConfigs,
	}
}

// List returns one page of requests received for a queue in the supplied half-open time range.
func (c *ListController) List(ctx context.Context, req entity.ListRequest) (result entity.ListResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "list")
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("queue", req.Queue); err != nil {
		return entity.ListResult{}, fmt.Errorf("ListController invalid queue: %w", err)
	}
	if _, err := c.queueConfigs.Get(ctx, req.Queue); err != nil {
		if errors.Is(err, queueconfig.ErrNotFound) {
			return entity.ListResult{}, errs.NewUserError(&UnrecognizedQueueError{Queue: req.Queue})
		}
		return entity.ListResult{}, fmt.Errorf("ListController failed to look up queue %q: %w", req.Queue, err)
	}
	if req.ReceivedAtOrAfterMs >= req.ReceivedBeforeMs {
		return entity.ListResult{}, fmt.Errorf("ListController requires received_at_or_after_ms < received_before_ms: %w", ErrInvalidRequest)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if pageSize < 0 || pageSize > maxListPageSize {
		return entity.ListResult{}, fmt.Errorf("ListController page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}

	query := storage.RequestQueueSummaryQuery{
		Queue:               req.Queue,
		ReceivedAtOrAfterMs: req.ReceivedAtOrAfterMs,
		ReceivedBeforeMs:    req.ReceivedBeforeMs,
		Limit:               pageSize + 1,
	}
	if req.PageToken != "" {
		token, err := decodeListPageToken(req.PageToken)
		if err != nil {
			return entity.ListResult{}, fmt.Errorf("ListController invalid page token: %w", ErrInvalidRequest)
		}
		if token.Queue != req.Queue || token.ReceivedAtOrAfterMs != req.ReceivedAtOrAfterMs || token.ReceivedBeforeMs != req.ReceivedBeforeMs {
			return entity.ListResult{}, fmt.Errorf("ListController page token does not match query: %w", ErrInvalidRequest)
		}
		query.HasCursor = true
		query.Cursor = storage.RequestQueueSummaryCursor{ReceivedAtMs: token.LastReceivedAtMs, RequestID: token.LastRequestID}
	}

	summaries, err := c.requestQueueSummaryStore.List(ctx, query)
	if err != nil {
		return entity.ListResult{}, fmt.Errorf("ListController failed to list queue=%s: %w", req.Queue, err)
	}

	visible := summaries
	result = entity.ListResult{Requests: make([]entity.RequestQueueSummary, 0, min(len(visible), pageSize))}
	if len(visible) > pageSize {
		visible = visible[:pageSize]
		last := visible[len(visible)-1]
		result.NextPageToken = encodeListPageToken(listPageToken{
			Queue:               req.Queue,
			ReceivedAtOrAfterMs: req.ReceivedAtOrAfterMs,
			ReceivedBeforeMs:    req.ReceivedBeforeMs,
			LastReceivedAtMs:    last.ReceivedAtMs,
			LastRequestID:       last.RequestID,
		})
	}
	result.Requests = append(result.Requests, visible...)
	c.logger.Debugw("queue requests listed", "queue", req.Queue, "request_count", len(result.Requests), "has_next_page", result.NextPageToken != "")
	return result, nil
}

func encodeListPageToken(token listPageToken) string {
	values := url.Values{
		"queue":                   {token.Queue},
		"received_at_or_after_ms": {strconv.FormatInt(token.ReceivedAtOrAfterMs, 10)},
		"received_before_ms":      {strconv.FormatInt(token.ReceivedBeforeMs, 10)},
		"last_received_at_ms":     {strconv.FormatInt(token.LastReceivedAtMs, 10)},
		"last_request_id":         {token.LastRequestID},
	}
	return base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
}

func decodeListPageToken(encoded string) (listPageToken, error) {
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return listPageToken{}, err
	}
	values, err := url.ParseQuery(string(data))
	if err != nil {
		return listPageToken{}, err
	}
	receivedAtOrAfterMs, err := strconv.ParseInt(values.Get("received_at_or_after_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	receivedBeforeMs, err := strconv.ParseInt(values.Get("received_before_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	lastReceivedAtMs, err := strconv.ParseInt(values.Get("last_received_at_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	token := listPageToken{
		Queue:               values.Get("queue"),
		ReceivedAtOrAfterMs: receivedAtOrAfterMs,
		ReceivedBeforeMs:    receivedBeforeMs,
		LastReceivedAtMs:    lastReceivedAtMs,
		LastRequestID:       values.Get("last_request_id"),
	}
	if token.Queue == "" || token.LastRequestID == "" || token.ReceivedAtOrAfterMs >= token.ReceivedBeforeMs {
		return listPageToken{}, fmt.Errorf("invalid token fields")
	}
	return token, nil
}
