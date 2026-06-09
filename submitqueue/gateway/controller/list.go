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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

const (
	defaultListPageSize = 50
	maxListPageSize     = 200
)

// ListController handles queue request listing for the gateway.
type ListController struct {
	logger              *zap.SugaredLogger
	metricsScope        tally.Scope
	requestSummaryStore storage.RequestSummaryStore
	queueConfigs        queueconfig.Store
}

// NewListController creates a new instance of the gateway list controller.
func NewListController(logger *zap.SugaredLogger, scope tally.Scope, requestSummaryStore storage.RequestSummaryStore, queueConfigs queueconfig.Store) *ListController {
	return &ListController{
		logger:              logger,
		metricsScope:        scope.SubScope("list_controller"),
		requestSummaryStore: requestSummaryStore,
		queueConfigs:        queueConfigs,
	}
}

// List returns request summaries for one queue whose lifecycles overlap a time window.
func (c *ListController) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("list_latency").Record(time.Since(start))
	}()
	c.metricsScope.Counter("list_count").Inc(1)

	if req.Queue == "" {
		return nil, fmt.Errorf("ListController requires the request to have a queue name specified: %w", ErrInvalidRequest)
	}
	if req.StartTimeMs <= 0 || req.EndTimeMs <= 0 || req.StartTimeMs >= req.EndTimeMs {
		return nil, fmt.Errorf("ListController requires a valid non-empty time window: %w", ErrInvalidRequest)
	}
	if req.PageSize < 0 {
		return nil, fmt.Errorf("ListController page_size must be non-negative: %w", ErrInvalidRequest)
	}

	statuses, err := canonicalStatuses(req.Statuses)
	if err != nil {
		return nil, fmt.Errorf("ListController invalid status filter: %w", err)
	}

	if _, err := c.queueConfigs.Get(ctx, req.Queue); err != nil {
		if errors.Is(err, queueconfig.ErrNotFound) {
			return nil, errs.NewUserError(&UnrecognizedQueueError{Queue: req.Queue})
		}
		return nil, fmt.Errorf("ListController failed to look up queue %q: %w", req.Queue, err)
	}

	limit := int(req.PageSize)
	if limit == 0 {
		limit = defaultListPageSize
	}
	if limit > maxListPageSize {
		limit = maxListPageSize
	}

	cursor, err := decodeListPageToken(req.PageToken)
	if err != nil {
		return nil, fmt.Errorf("ListController invalid page token: %w", err)
	}
	if cursor != nil && !cursor.matches(req.Queue, req.StartTimeMs, req.EndTimeMs, statuses) {
		return nil, fmt.Errorf("ListController page token does not match query: %w", ErrInvalidRequest)
	}

	result, err := c.requestSummaryStore.List(ctx, storage.RequestSummaryListOptions{
		Queue:       req.Queue,
		StartTimeMs: req.StartTimeMs,
		EndTimeMs:   req.EndTimeMs,
		Statuses:    statuses,
		Cursor:      cursorStorage(cursor),
		Limit:       limit,
	})
	if err != nil {
		return nil, fmt.Errorf("ListController failed to list request summaries for queue=%s: %w", req.Queue, err)
	}

	resp := &pb.ListResponse{
		Requests: make([]*pb.RequestSummary, 0, len(result.Requests)),
	}
	for _, summary := range result.Requests {
		resp.Requests = append(resp.Requests, protoRequestSummary(summary))
	}
	if result.NextCursor != nil {
		token, err := encodeListPageToken(listPageToken{
			Queue:       req.Queue,
			StartTimeMs: req.StartTimeMs,
			EndTimeMs:   req.EndTimeMs,
			Statuses:    statusesToStrings(statuses),
			StartedAtMs: result.NextCursor.StartedAtMs,
			RequestID:   result.NextCursor.RequestID,
		})
		if err != nil {
			return nil, fmt.Errorf("ListController failed to encode next page token: %w", err)
		}
		resp.NextPageToken = token
	}

	c.logger.Debugw("request summaries listed",
		"queue", req.Queue,
		"count", len(resp.Requests),
		"has_next_page", resp.NextPageToken != "",
	)

	return resp, nil
}

type listPageToken struct {
	Queue       string   `json:"queue"`
	StartTimeMs int64    `json:"start_time_ms"`
	EndTimeMs   int64    `json:"end_time_ms"`
	Statuses    []string `json:"statuses"`
	StartedAtMs int64    `json:"started_at_ms"`
	RequestID   string   `json:"request_id"`
}

func (t listPageToken) matches(queue string, startTimeMs, endTimeMs int64, statuses []entity.RequestStatus) bool {
	return t.Queue == queue &&
		t.StartTimeMs == startTimeMs &&
		t.EndTimeMs == endTimeMs &&
		equalStrings(t.Statuses, statusesToStrings(statuses))
}

func encodeListPageToken(token listPageToken) (string, error) {
	data, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeListPageToken(raw string) (*listPageToken, error) {
	if raw == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	var token listPageToken
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return &token, nil
}

func cursorStorage(token *listPageToken) *storage.RequestSummaryCursor {
	if token == nil {
		return nil
	}
	return &storage.RequestSummaryCursor{StartedAtMs: token.StartedAtMs, RequestID: token.RequestID}
}

func canonicalStatuses(raw []string) ([]entity.RequestStatus, error) {
	seen := make(map[entity.RequestStatus]struct{}, len(raw))
	var statuses []entity.RequestStatus
	for _, statusRaw := range raw {
		status := entity.RequestStatus(statusRaw)
		if !entity.IsKnownRequestStatus(status) {
			return nil, fmt.Errorf("unknown status %q: %w", statusRaw, ErrInvalidRequest)
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i] < statuses[j] })
	return statuses, nil
}

func statusesToStrings(statuses []entity.RequestStatus) []string {
	out := make([]string, len(statuses))
	for i, status := range statuses {
		out[i] = string(status)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func protoRequestSummary(summary entity.RequestSummary) *pb.RequestSummary {
	return &pb.RequestSummary{
		Sqid:          summary.RequestID,
		Queue:         summary.Queue,
		ChangeUris:    summary.ChangeURIs,
		Status:        string(summary.Status),
		LastError:     summary.LastError,
		Metadata:      summary.Metadata,
		StartedAtMs:   summary.StartedAtMs,
		UpdatedAtMs:   summary.UpdatedAtMs,
		CompletedAtMs: summary.CompletedAtMs,
		Terminal:      summary.Terminal,
	}
}
