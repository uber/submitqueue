// Copyright (c) 2026 Uber Technologies, Inc.
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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	defaultListPageSize = 50
	maxListPageSize     = 200
)

// ListController handles queue-scoped acceptance-time listing.
type ListController interface {
	List(ctx context.Context, req entity.ListRequest) (entity.ListResult, error)
}

type listController struct {
	logger           *zap.SugaredLogger
	metricsScope     tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
	now              func() time.Time
}

// NewListController creates a controller restricted to the supplied configured queues.
func NewListController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, configuredQueues []string) ListController {
	queues := make(map[string]struct{}, len(configuredQueues))
	for _, queue := range configuredQueues {
		queues[queue] = struct{}{}
	}
	return &listController{
		logger: logger, metricsScope: scope.SubScope("list_controller"), stores: stores,
		configuredQueues: queues, now: time.Now,
	}
}

// List returns current summaries in descending acceptance-time order within a half-open time window.
// Omitted bounds resolve to [0, server now) on the first page and to the token's window on continuations.
func (c *listController) List(ctx context.Context, req entity.ListRequest) (result entity.ListResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" {
		return entity.ListResult{}, fmt.Errorf("List requires a queue: %w", ErrInvalidRequest)
	}
	if _, ok := c.configuredQueues[req.Queue]; !ok {
		return entity.ListResult{}, fmt.Errorf("List queue %q is not configured: %w", req.Queue, ErrInvalidRequest)
	}
	query, err := c.resolveListRange(req)
	if err != nil {
		return entity.ListResult{}, err
	}
	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.ListResult{}, fmt.Errorf("List failed to resolve storage for queue %q: %w", req.Queue, err)
	}
	mappings, err := stores.GetRequestAcceptanceStore().List(ctx, query)
	if err != nil {
		return entity.ListResult{}, fmt.Errorf("List failed to read acceptance mappings for queue %q: %w", req.Queue, err)
	}
	pageSize := query.Limit - 1
	visible := mappings[:min(len(mappings), pageSize)]
	result.Requests = make([]entity.RequestSummary, 0, len(visible))
	if len(visible) > 0 {
		summaries := stores.GetRequestSummaryStore()
		for _, mapping := range visible {
			summary, err := readListSummary(ctx, summaries, req.Queue, mapping)
			if err != nil {
				return entity.ListResult{}, err
			}
			result.Requests = append(result.Requests, summary)
		}
	}
	if len(mappings) > pageSize {
		last := visible[len(visible)-1]
		nextToken, err := encodeListPageToken(listPageToken{
			Version: listPageTokenVersion, Queue: req.Queue,
			AcceptedAtOrAfterMs: query.AcceptedAtOrAfterMs, AcceptedBeforeMs: query.AcceptedBeforeMs,
			LastAcceptedAtMs: last.AcceptedAtMs, LastRequestID: last.RequestID,
		})
		if err != nil {
			return entity.ListResult{}, fmt.Errorf("List failed to encode continuation: %w", err)
		}
		result.NextPageToken = nextToken
	}
	c.logger.Debugw("queue requests listed", "queue", req.Queue, "request_count", len(result.Requests), "has_next_page", result.NextPageToken != "")
	return result, nil
}

func readListSummary(ctx context.Context, summaries storage.RequestSummaryStore, queue string, mapping entity.RequestAcceptance) (entity.RequestSummary, error) {
	if mapping.Queue != queue || mapping.AcceptedAtMs <= 0 || mapping.RequestID == "" {
		return entity.RequestSummary{}, &ListConsistencyError{
			Queue: queue, RequestID: mapping.RequestID, Reason: "invalid acceptance mapping",
		}
	}
	summary, err := summaries.Get(ctx, mapping.RequestID)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.RequestSummary{}, &ListConsistencyError{
				Queue: queue, RequestID: mapping.RequestID, Reason: "acceptance mapping has no summary", Err: err,
			}
		}
		return entity.RequestSummary{}, fmt.Errorf("List failed to read summary queue=%q request_id=%q: %w", queue, mapping.RequestID, err)
	}
	if summary.Queue != queue || summary.RequestID != mapping.RequestID || summary.AcceptedAtMs != mapping.AcceptedAtMs {
		return entity.RequestSummary{}, &ListConsistencyError{
			Queue: queue, RequestID: mapping.RequestID, Reason: "acceptance mapping disagrees with summary",
		}
	}
	return summary, nil
}
