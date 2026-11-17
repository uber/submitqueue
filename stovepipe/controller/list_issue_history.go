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

//go:generate mockgen -source=list_issue_history.go -destination=mock/list_issue_history_mock.go -package=mock

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// ListIssueHistoryController reads retained issue occurrences across requests.
type ListIssueHistoryController interface {
	ListIssueHistory(context.Context, entity.ListIssueHistoryRequest) (entity.ListIssueHistoryResult, error)
}

type listIssueHistoryController struct {
	logger           *zap.SugaredLogger
	scope            tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
	now              func() time.Time
}

const (
	issueHistoryPageTokenVersion = 1
	maxIssueHistoryRequests      = 200
	maxIssueHistorySummaries     = 200
)

type issueHistoryPageToken struct {
	Version             int                               `json:"version"`
	Queue               string                            `json:"queue"`
	TaskID              string                            `json:"task_id"`
	UnitID              string                            `json:"unit_id"`
	AcceptedAtOrAfterMs int64                             `json:"accepted_at_or_after_ms"`
	AcceptedBeforeMs    int64                             `json:"accepted_before_ms"`
	Before              storage.RequestAcceptanceCursor   `json:"before"`
	AfterIssue          storage.RequestIssueSummaryCursor `json:"after_issue"`
}

// NewListIssueHistoryController restricts reads to configured queues.
func NewListIssueHistoryController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, configuredQueues []string) ListIssueHistoryController {
	queues := make(map[string]struct{}, len(configuredQueues))
	for _, queue := range configuredQueues {
		queues[queue] = struct{}{}
	}

	return &listIssueHistoryController{
		logger:           logger,
		scope:            scope.SubScope("list_issue_history_controller"),
		stores:           stores,
		configuredQueues: queues,
		now:              time.Now,
	}
}

func (c *listIssueHistoryController) ListIssueHistory(ctx context.Context, req entity.ListIssueHistoryRequest) (result entity.ListIssueHistoryResult, retErr error) {
	op := metrics.Begin(c.scope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" || len(req.Queue) > maxHistoryIdentifierBytes {
		return result, fmt.Errorf("queue must be nonempty and at most %d bytes: %w", maxHistoryIdentifierBytes, ErrInvalidRequest)
	}
	if _, ok := c.configuredQueues[req.Queue]; !ok {
		return result, fmt.Errorf("queue %q is not configured: %w", req.Queue, ErrInvalidRequest)
	}
	if len(req.TaskID) > maxHistoryIdentifierBytes {
		return result, fmt.Errorf("task ID must be at most %d bytes: %w", maxHistoryIdentifierBytes, ErrInvalidRequest)
	}
	if len(req.UnitID) > 1024 {
		return result, fmt.Errorf("unit ID must be at most 1024 bytes: %w", ErrInvalidRequest)
	}
	if req.TaskID == "" && req.UnitID != "" {
		return result, fmt.Errorf("unit ID requires a task ID: %w", ErrInvalidRequest)
	}

	token, err := resolveIssueHistoryPageToken(req, c.now().UnixMilli())
	if err != nil {
		return result, err
	}

	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve issue history storage: %w", err)
	}

	bounds := storage.RequestAcceptanceRange{
		AcceptedAtOrAfterMs: token.AcceptedAtOrAfterMs,
		AcceptedBeforeMs:    token.AcceptedBeforeMs,
		Before:              token.Before,
	}

	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}

	scan, err := scanIssueHistoryPage(ctx, store, req.TaskID, req.UnitID, bounds, token.AfterIssue, pageSize)
	if err != nil {
		return result, err
	}

	result.AcceptedAtOrAfterMs = token.AcceptedAtOrAfterMs
	result.AcceptedBeforeMs = token.AcceptedBeforeMs
	result.Events = scan.events

	if scan.hasMore {
		token.Before = storage.RequestAcceptanceCursor{AcceptedAtMs: scan.last.AcceptedAtMs, RequestID: scan.last.RequestID}
		token.AfterIssue = scan.afterIssue
		data, err := json.Marshal(token)
		if err != nil {
			return entity.ListIssueHistoryResult{}, fmt.Errorf("encode issue history continuation: %w", err)
		}

		result.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}

	c.logger.Debugw("issue history listed",
		"queue", req.Queue,
		"task_id", req.TaskID,
		"unit_id", req.UnitID,
		"event_count", len(result.Events),
		"has_next_page", result.NextPageToken != "",
	)
	return result, nil
}

type issueHistoryPageScan struct {
	events     []entity.IssueHistoryEntry
	last       entity.RequestAcceptance // Last examined request, even if skipped.
	afterIssue storage.RequestIssueSummaryCursor
	hasMore    bool
}

func scanIssueHistoryPage(ctx context.Context, store storage.Storage, taskID, unitID string, bounds storage.RequestAcceptanceRange, afterIssue storage.RequestIssueSummaryCursor, pageSize int) (issueHistoryPageScan, error) {
	scan := issueHistoryPageScan{
		events:     make([]entity.IssueHistoryEntry, 0, pageSize),
		afterIssue: afterIssue,
	}
	requests := store.GetRequestAcceptanceStore()
	inspected := 0
	inspectedIssues := 0

	for {
		if err := ctx.Err(); err != nil {
			return issueHistoryPageScan{}, err
		}

		want := min(pageSize-len(scan.events), maxIssueHistoryRequests-inspected)
		bounds.Limit = want + 1
		batch, err := requests.List(ctx, bounds)
		if err != nil {
			return issueHistoryPageScan{}, fmt.Errorf("read issue history requests: %w", err)
		}
		if scan.afterIssue.TaskID != "" {
			// Before normally excludes this request; a partial issue page must resume within it first.
			batch = append([]entity.RequestAcceptance{{RequestID: bounds.Before.RequestID, AcceptedAtMs: bounds.Before.AcceptedAtMs}}, batch...)
		}

		for i, request := range batch[:min(len(batch), want)] {
			if err := ctx.Err(); err != nil {
				return issueHistoryPageScan{}, err
			}

			scan.last = request
			inspected++

			requestScan, err := scanRequestIssueHistory(ctx, store, request.RequestID, taskID, unitID,
				scan.afterIssue, pageSize-len(scan.events), maxIssueHistorySummaries-inspectedIssues)
			if err != nil {
				return issueHistoryPageScan{}, err
			}

			scan.events = append(scan.events, requestScan.events...)
			inspectedIssues += requestScan.inspected
			scan.afterIssue = requestScan.resumeAfter

			if scan.afterIssue.TaskID != "" {
				scan.hasMore = true
				return scan, nil
			}
			if len(scan.events) == pageSize || inspectedIssues == maxIssueHistorySummaries {
				scan.hasMore = len(batch) > i+1
				return scan, nil
			}
		}

		if err := ctx.Err(); err != nil {
			return issueHistoryPageScan{}, err
		}
		if len(batch) <= want {
			return scan, nil
		}
		if inspected == maxIssueHistoryRequests {
			scan.hasMore = true
			return scan, nil
		}

		bounds.Before = storage.RequestAcceptanceCursor{AcceptedAtMs: scan.last.AcceptedAtMs, RequestID: scan.last.RequestID}
	}
}

type requestIssueHistoryScan struct {
	events      []entity.IssueHistoryEntry
	inspected   int
	resumeAfter storage.RequestIssueSummaryCursor
}

func scanRequestIssueHistory(ctx context.Context, store storage.Storage, requestID, taskID, unitID string, afterIssue storage.RequestIssueSummaryCursor, pageSize, budget int) (requestIssueHistoryScan, error) {
	var scan requestIssueHistoryScan
	summaries := store.GetRequestIssueSummaryStore()
	logs := store.GetIssueLogStore()
	position := afterIssue

	for {
		if err := ctx.Err(); err != nil {
			return requestIssueHistoryScan{}, err
		}

		limit := min(pageSize-len(scan.events), budget-scan.inspected)
		var rows []entity.RequestIssueSummary
		if taskID == "" {
			var err error
			rows, err = summaries.List(ctx, requestID, storage.RequestIssueSummaryRange{After: position, Limit: limit + 1})
			if err != nil {
				return requestIssueHistoryScan{}, fmt.Errorf("read request issue summaries: %w", err)
			}
		} else {
			summary, err := summaries.Get(ctx, requestID, taskID, unitID)
			if storage.IsNotFound(err) {
				return scan, nil
			}
			if err != nil {
				return requestIssueHistoryScan{}, fmt.Errorf("read request issue summary: %w", err)
			}
			rows = []entity.RequestIssueSummary{summary}
		}

		for _, summary := range rows[:min(len(rows), limit)] {
			if err := ctx.Err(); err != nil {
				return requestIssueHistoryScan{}, err
			}

			scan.inspected++
			position = storage.RequestIssueSummaryCursor{TaskID: summary.TaskID, UnitID: summary.UnitID}

			switch summary.Kind {
			case entity.RequestIssueKindOpened, entity.RequestIssueKindReported, entity.RequestIssueKindClosed:
			default:
				continue
			}

			logTask, logUnit := taskID, unitID
			if taskID == "" {
				logTask, logUnit = summary.TaskID, summary.UnitID
			}
			log, err := logs.Get(ctx, logTask, logUnit, summary.BuildID, entity.IssueEvent(summary.Kind))
			if storage.IsNotFound(err) {
				continue
			}
			if err != nil {
				return requestIssueHistoryScan{}, fmt.Errorf("read issue history: %w", err)
			}

			scan.events = append(scan.events, entity.IssueHistoryEntry{Log: log, RequestID: requestID})
		}

		if err := ctx.Err(); err != nil {
			return requestIssueHistoryScan{}, err
		}
		if len(rows) <= limit {
			return scan, nil
		}
		if len(scan.events) == pageSize || scan.inspected == budget {
			scan.resumeAfter = position
			return scan, nil
		}
	}
}

func resolveIssueHistoryPageToken(req entity.ListIssueHistoryRequest, nowMs int64) (issueHistoryPageToken, error) {
	token := issueHistoryPageToken{
		Version:          issueHistoryPageTokenVersion,
		Queue:            req.Queue,
		TaskID:           req.TaskID,
		UnitID:           req.UnitID,
		AcceptedBeforeMs: nowMs,
	}
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return token, fmt.Errorf("invalid issue history page size: %w", ErrInvalidRequest)
	}

	if req.PageToken == "" {
		if req.HasAcceptedAtOrAfterMs {
			token.AcceptedAtOrAfterMs = req.AcceptedAtOrAfterMs
		}
		if req.HasAcceptedBeforeMs {
			token.AcceptedBeforeMs = req.AcceptedBeforeMs
		}

		if token.AcceptedAtOrAfterMs < 0 || token.AcceptedBeforeMs <= token.AcceptedAtOrAfterMs {
			return token, fmt.Errorf("invalid issue history request window: %w", ErrInvalidRequest)
		}

		return token, nil
	}

	data, err := base64.RawURLEncoding.Strict().DecodeString(req.PageToken)
	if err != nil {
		return token, fmt.Errorf("invalid issue history continuation: %w", ErrInvalidRequest)
	}

	token = issueHistoryPageToken{}
	if err := json.Unmarshal(data, &token); err != nil {
		return token, fmt.Errorf("invalid issue history continuation: %w", ErrInvalidRequest)
	}

	if token.Version != issueHistoryPageTokenVersion ||
		token.Queue != req.Queue ||
		token.TaskID != req.TaskID ||
		token.UnitID != req.UnitID ||
		(req.HasAcceptedAtOrAfterMs && req.AcceptedAtOrAfterMs != token.AcceptedAtOrAfterMs) ||
		(req.HasAcceptedBeforeMs && req.AcceptedBeforeMs != token.AcceptedBeforeMs) {
		return token, fmt.Errorf("issue history continuation does not match the issue and request window: %w", ErrInvalidRequest)
	}

	after := token.AfterIssue
	if token.TaskID != "" && after.TaskID != "" {
		return token, fmt.Errorf("exact-issue history cannot include an issue cursor: %w", ErrInvalidRequest)
	}
	if after.TaskID == "" && after.UnitID != "" {
		return token, fmt.Errorf("issue history cursor requires a task ID: %w", ErrInvalidRequest)
	}
	if len(after.TaskID) > maxHistoryIdentifierBytes || len(after.UnitID) > 1024 {
		return token, fmt.Errorf("issue history cursor exceeds identifier limits: %w", ErrInvalidRequest)
	}

	if token.AcceptedAtOrAfterMs < 0 ||
		token.AcceptedBeforeMs <= token.AcceptedAtOrAfterMs ||
		token.Before.AcceptedAtMs <= 0 ||
		token.Before.AcceptedAtMs < token.AcceptedAtOrAfterMs ||
		token.Before.AcceptedAtMs >= token.AcceptedBeforeMs ||
		id.Validate(token.Before.RequestID) != nil {
		return token, fmt.Errorf("invalid issue history request position: %w", ErrInvalidRequest)
	}

	return token, nil
}
