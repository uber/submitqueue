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

//go:generate mockgen -source=list_open_issues.go -destination=mock/list_open_issues_mock.go -package=mock

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	openIssuesPageTokenVersion = 1
	maxOpenIssueCandidates     = 500
)

type openIssuesPageToken struct {
	Version    int    `json:"version"`
	Queue      string `json:"queue"`
	LastTaskID string `json:"last_task_id"`
	LastUnitID string `json:"last_unit_id"`
}

// ListOpenIssuesController lists current open issues within a configured queue.
type ListOpenIssuesController interface {
	ListOpenIssues(context.Context, entity.ListOpenIssuesRequest) (entity.ListOpenIssuesResult, error)
}

type listOpenIssuesController struct {
	logger           *zap.SugaredLogger
	scope            tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
}

// NewListOpenIssuesController restricts open-issue listing to configured queues.
func NewListOpenIssuesController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, configuredQueues []string) ListOpenIssuesController {
	queues := make(map[string]struct{}, len(configuredQueues))
	for _, queue := range configuredQueues {
		queues[queue] = struct{}{}
	}
	return &listOpenIssuesController{
		logger:           logger,
		scope:            scope.SubScope("list_open_issues_controller"),
		stores:           stores,
		configuredQueues: queues,
	}
}

func (c *listOpenIssuesController) ListOpenIssues(ctx context.Context, req entity.ListOpenIssuesRequest) (result entity.ListOpenIssuesResult, retErr error) {
	op := metrics.Begin(c.scope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if req.Queue == "" {
		return result, fmt.Errorf("ListOpenIssues requires a queue: %w", ErrInvalidRequest)
	}
	if _, ok := c.configuredQueues[req.Queue]; !ok {
		return result, fmt.Errorf("ListOpenIssues queue %q is not configured: %w", req.Queue, ErrInvalidRequest)
	}

	bounds, err := resolveOpenIssueRange(req)
	if err != nil {
		return result, err
	}

	stores, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve open issue storage for queue %q: %w", req.Queue, err)
	}

	scan, err := scanOpenIssuesPage(ctx, stores, req.Queue, bounds)
	if err != nil {
		return result, err
	}
	result.Issues = scan.issues
	if scan.hasMore {
		token, err := encodeOpenIssuesPageToken(openIssuesPageToken{
			Version: openIssuesPageTokenVersion, Queue: req.Queue, LastTaskID: scan.last.TaskID, LastUnitID: scan.last.UnitID,
		})
		if err != nil {
			return entity.ListOpenIssuesResult{}, fmt.Errorf("encode open issue continuation: %w", err)
		}
		result.NextPageToken = token
	}

	c.logger.Debugw("queue open issues listed", "queue", req.Queue, "issue_count", len(result.Issues), "has_next_page", result.NextPageToken != "")
	return result, nil
}

type openIssuesPageScan struct {
	issues  []entity.IssueSummary
	last    entity.OpenIssue // Last inspected entry, even if skipped.
	hasMore bool
}

func scanOpenIssuesPage(ctx context.Context, stores storage.Storage, queue string, bounds storage.OpenIssueRange) (openIssuesPageScan, error) {
	pageSize := bounds.Limit - 1
	scan := openIssuesPageScan{issues: make([]entity.IssueSummary, 0, pageSize)}
	openIssues := stores.GetOpenIssueStore()
	summaries := stores.GetIssueSummaryStore()
	inspected := 0
	for {
		want := min(pageSize-len(scan.issues), maxOpenIssueCandidates-inspected)
		bounds.Limit = want + 1
		batch, err := openIssues.List(ctx, bounds)
		if err != nil {
			return openIssuesPageScan{}, fmt.Errorf("list open issues for queue %q: %w", queue, err)
		}
		for _, openIssue := range batch[:min(len(batch), want)] {
			summary, err := summaries.Get(ctx, openIssue.TaskID, openIssue.UnitID)
			if err != nil {
				return openIssuesPageScan{}, fmt.Errorf("read open issue summary queue=%q task=%q unit=%q: %w", queue, openIssue.TaskID, openIssue.UnitID, err)
			}
			if summary.TaskID != openIssue.TaskID || summary.UnitID != openIssue.UnitID {
				return openIssuesPageScan{}, fmt.Errorf("open issue disagrees with summary identity queue=%q task=%q unit=%q", queue, openIssue.TaskID, openIssue.UnitID)
			}
			if summary.Status == entity.IssueStatusOpen && summary.FirstSeenBuildID == openIssue.FirstSeenBuildID {
				scan.issues = append(scan.issues, summary)
			}
			scan.last = openIssue
			inspected++
		}
		if len(batch) <= want {
			return scan, nil
		}
		if len(scan.issues) == pageSize || inspected == maxOpenIssueCandidates {
			scan.hasMore = true
			return scan, nil
		}
		bounds.After = storage.OpenIssueCursor{TaskID: scan.last.TaskID, UnitID: scan.last.UnitID}
	}
}

func resolveOpenIssueRange(req entity.ListOpenIssuesRequest) (storage.OpenIssueRange, error) {
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return storage.OpenIssueRange{}, fmt.Errorf("ListOpenIssues page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	bounds := storage.OpenIssueRange{Limit: pageSize + 1}
	if req.PageToken != "" {
		token, err := decodeOpenIssuesPageToken(req.PageToken)
		if err != nil {
			return storage.OpenIssueRange{}, err
		}
		if token.Queue != req.Queue {
			return storage.OpenIssueRange{}, fmt.Errorf("ListOpenIssues page_token does not match the queue: %w", ErrInvalidRequest)
		}
		bounds.After = storage.OpenIssueCursor{TaskID: token.LastTaskID, UnitID: token.LastUnitID}
	}
	return bounds, nil
}

func encodeOpenIssuesPageToken(token openIssuesPageToken) (string, error) {
	contents, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(contents), nil
}

func decodeOpenIssuesPageToken(encoded string) (openIssuesPageToken, error) {
	contents, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return openIssuesPageToken{}, fmt.Errorf("ListOpenIssues invalid page_token encoding: %w", ErrInvalidRequest)
	}
	var token openIssuesPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return openIssuesPageToken{}, fmt.Errorf("ListOpenIssues invalid page_token contents: %w", ErrInvalidRequest)
	}
	if token.Version != openIssuesPageTokenVersion || token.Queue == "" || token.LastTaskID == "" {
		return openIssuesPageToken{}, fmt.Errorf("ListOpenIssues invalid page_token fields: %w", ErrInvalidRequest)
	}
	if len(token.LastTaskID) > 255 || len(token.LastUnitID) > 1024 {
		return openIssuesPageToken{}, fmt.Errorf("ListOpenIssues page_token key exceeds identifier limits: %w", ErrInvalidRequest)
	}
	return token, nil
}
