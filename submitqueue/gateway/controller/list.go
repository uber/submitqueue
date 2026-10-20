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
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
	storage "github.com/uber/submitqueue/submitqueue/gateway/extension/storage"
	"go.uber.org/zap"
)

const (
	defaultListPageSize = 50
	maxListPageSize     = 200
)

// ListController handles bounded queue receipt-history queries.
type ListController interface {
	List(ctx context.Context, req entity.ListRequest) (entity.ListResult, error)
}

var _ ListController = (*listController)(nil)

type listController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	stores       storage.Factory
	queueConfigs queueconfig.Store
}

// NewListController creates a gateway list controller.
func NewListController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, queueConfigs queueconfig.Store) ListController {
	return &listController{
		logger:       logger,
		metricsScope: scope.SubScope("list_controller"),
		stores:       stores,
		queueConfigs: queueConfigs,
	}
}

// List returns one page of requests received for a queue in a half-open time range, taken from the request on the
// first page and from the page token afterwards.
func (c *listController) List(ctx context.Context, req entity.ListRequest) (result entity.ListResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateStoredIdentifier("queue", req.Queue); err != nil {
		return entity.ListResult{}, fmt.Errorf("invalid queue: %w", err)
	}
	if _, err := c.queueConfigs.Get(ctx, req.Queue); err != nil {
		if errors.Is(err, queueconfig.ErrNotFound) {
			return entity.ListResult{}, errs.NewUserError(&UnrecognizedQueueError{Queue: req.Queue})
		}
		return entity.ListResult{}, fmt.Errorf("failed to look up queue %q: %w", req.Queue, err)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if pageSize < 0 || pageSize > maxListPageSize {
		return entity.ListResult{}, fmt.Errorf("page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	query, err := resolveListRange(req, pageSize)
	if err != nil {
		return entity.ListResult{}, err
	}

	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.ListResult{}, fmt.Errorf("failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	receipts, err := store.GetRequestReceiptStore().List(ctx, query)
	if err != nil {
		return entity.ListResult{}, fmt.Errorf("failed to list request receipts queue=%q: %w", req.Queue, err)
	}

	visible := receipts[:min(len(receipts), pageSize)]
	result.ReceivedAtOrAfterMs = query.ReceivedAtOrAfterMs
	result.ReceivedBeforeMs = query.ReceivedBeforeMs
	result.Requests = make([]entity.RequestSummary, 0, len(visible))
	if len(visible) > 0 {
		summaries := store.GetRequestSummaryStore()
		for _, receipt := range visible {
			summary, err := readListSummary(ctx, summaries, req.Queue, receipt)
			if err != nil {
				return entity.ListResult{}, err
			}
			result.Requests = append(result.Requests, summary)
		}
	}
	if len(receipts) > pageSize {
		last := visible[len(visible)-1]
		token, err := encodeListPageToken(listPageToken{
			Version:             listPageTokenVersion,
			Queue:               req.Queue,
			ReceivedAtOrAfterMs: query.ReceivedAtOrAfterMs,
			ReceivedBeforeMs:    query.ReceivedBeforeMs,
			LastReceivedAtMs:    last.ReceivedAtMs,
			LastRequestID:       last.RequestID,
		})
		if err != nil {
			return entity.ListResult{}, fmt.Errorf("failed to encode page token queue=%q: %w", req.Queue, err)
		}
		result.NextPageToken = token
	}
	c.logger.Debugw("queue requests listed", "queue", req.Queue, "request_count", len(result.Requests), "has_next_page", result.NextPageToken != "")
	return result, nil
}

func readListSummary(ctx context.Context, summaries basestorage.RequestSummaryStore, queue string, receipt entity.RequestReceipt) (entity.RequestSummary, error) {
	if receipt.Queue != queue || receipt.ReceivedAtMs <= 0 || receipt.RequestID == "" {
		return entity.RequestSummary{}, &InternalConsistencyError{Message: fmt.Sprintf("invalid request receipt queue=%q request_id=%q", queue, receipt.RequestID)}
	}
	summary, err := summaries.Get(ctx, receipt.RequestID)
	if err != nil {
		if basestorage.IsNotFound(err) {
			return entity.RequestSummary{}, &InternalConsistencyError{Message: fmt.Sprintf("request summary missing for receipt queue=%q request_id=%q", queue, receipt.RequestID)}
		}
		return entity.RequestSummary{}, fmt.Errorf("failed to read request summary queue=%q request_id=%q: %w", queue, receipt.RequestID, err)
	}
	if summary.Queue != queue || summary.RequestID != receipt.RequestID || summary.ReceivedAtMs != receipt.ReceivedAtMs || summary.Status == entity.RequestStatusAccepting {
		return entity.RequestSummary{}, &InternalConsistencyError{Message: fmt.Sprintf("request receipt disagrees with public summary queue=%q request_id=%q", queue, receipt.RequestID)}
	}
	return summary, nil
}
