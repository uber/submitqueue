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

//go:generate mockgen -source=list_issues_by_request_id.go -destination=mock/list_issues_by_request_id_mock.go -package=mock

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const maxIssueBuildsPerPage = 100

type requestIssuesPageToken struct {
	Version   int    `json:"version"`
	Queue     string `json:"queue"`
	RequestID string `json:"request_id"`
	BuildID   string `json:"build_id"`
	TaskID    string `json:"task_id"`
	UnitID    string `json:"unit_id"`
}

// ListIssuesByRequestIDController lists observations for a request, including earlier attempts.
type ListIssuesByRequestIDController interface {
	ListIssuesByRequestID(context.Context, entity.ListIssuesByRequestIDRequest) (entity.ListIssuesByRequestIDResult, error)
}

type listIssuesByRequestIDController struct {
	scope            tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
}

// NewListIssuesByRequestIDController restricts reads to configured queues.
func NewListIssuesByRequestIDController(scope tally.Scope, stores storage.Factory, queues []string) ListIssuesByRequestIDController {
	configured := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		configured[queue] = struct{}{}
	}
	return &listIssuesByRequestIDController{scope: scope.SubScope("list_issues_by_request_id_controller"), stores: stores, configuredQueues: configured}
}

func (c *listIssuesByRequestIDController) ListIssuesByRequestID(ctx context.Context, req entity.ListIssuesByRequestIDRequest) (result entity.ListIssuesByRequestIDResult, retErr error) {
	op := metrics.Begin(c.scope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return result, err
	}
	if _, ok := c.configuredQueues[req.Queue]; !ok {
		return result, fmt.Errorf("queue %q is not configured: %w", req.Queue, ErrInvalidRequest)
	}
	if err := validateRequestID(req.RequestID); err != nil {
		return result, err
	}
	pageSize, cursor, err := resolveRequestIssuesPage(req)
	if err != nil {
		return result, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve request issue storage: %w", err)
	}
	history, err := readRequestHistory(ctx, store, req.Queue, req.RequestID)
	if err != nil {
		if storage.IsNotFound(err) && !errs.IsRetryable(err) {
			return result, &RequestHistoryByIDNotFoundError{RequestID: req.RequestID}
		}
		return result, err
	}
	result = entity.ListIssuesByRequestIDResult{Queue: history.RequestSummary.Queue, RequestID: history.RequestID, Issues: []entity.RequestIssue{}}
	buildIDs := requestIssueBuildIDs(history)
	start := 0
	if cursor.BuildID != "" {
		start = slices.Index(buildIDs, cursor.BuildID)
		if start < 0 {
			return entity.ListIssuesByRequestIDResult{}, fmt.Errorf("page_token build does not belong to request: %w", ErrInvalidRequest)
		}
	}
	if len(buildIDs) == 0 {
		return result, nil
	}
	observations := store.GetBuildIssueStore()
	for index := start; index < len(buildIDs); index++ {
		buildID := buildIDs[index]
		after := storage.BuildIssueCursor{}
		if index == start {
			after = storage.BuildIssueCursor{TaskID: cursor.TaskID, UnitID: cursor.UnitID}
		}
		remaining := pageSize - len(result.Issues)
		rows, err := observations.List(ctx, buildID, storage.BuildIssueRange{After: after, Limit: remaining + 1})
		if err != nil {
			return entity.ListIssuesByRequestIDResult{}, fmt.Errorf("list issues for build %q: %w", buildID, err)
		}
		for _, row := range rows[:min(remaining, len(rows))] {
			result.Issues = append(result.Issues, entity.RequestIssue{BuildID: buildID, TaskID: row.TaskID, UnitID: row.UnitID, FixID: row.FixID})
		}
		next := requestIssuesPageToken{Version: 1, Queue: req.Queue, RequestID: req.RequestID}
		switch {
		case len(rows) > remaining:
			last := result.Issues[len(result.Issues)-1]
			next.BuildID, next.TaskID, next.UnitID = buildID, last.TaskID, last.UnitID
		case index+1 < len(buildIDs) && (len(result.Issues) == pageSize || index-start+1 == maxIssueBuildsPerPage):
			next.BuildID = buildIDs[index+1]
		default:
			continue
		}
		contents, err := json.Marshal(next)
		if err != nil {
			return entity.ListIssuesByRequestIDResult{}, fmt.Errorf("encode request issue continuation: %w", err)
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(contents)
		break
	}
	return result, nil
}

func resolveRequestIssuesPage(req entity.ListIssuesByRequestIDRequest) (int, requestIssuesPageToken, error) {
	var cursor requestIssuesPageToken
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return 0, cursor, fmt.Errorf("page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if req.PageToken == "" {
		return pageSize, cursor, nil
	}
	contents, err := base64.RawURLEncoding.Strict().DecodeString(req.PageToken)
	if err != nil {
		return 0, cursor, fmt.Errorf("invalid page_token encoding: %w", ErrInvalidRequest)
	}
	if err := json.Unmarshal(contents, &cursor); err != nil {
		return 0, cursor, fmt.Errorf("invalid page_token contents: %w", ErrInvalidRequest)
	}
	if cursor.Version != 1 || cursor.Queue != req.Queue || cursor.RequestID != req.RequestID || cursor.BuildID == "" ||
		len(cursor.BuildID) > 1020 || len(cursor.TaskID) > 255 || len(cursor.UnitID) > 1024 || (cursor.TaskID == "" && cursor.UnitID != "") {
		return 0, cursor, fmt.Errorf("invalid page_token fields or request scope: %w", ErrInvalidRequest)
	}
	return pageSize, cursor, nil
}

func requestIssueBuildIDs(history entity.RequestHistory) []string {
	builds := make(map[string]struct{})
	for _, log := range history.Events {
		buildID := log.Metadata[requestlog.MetadataKeyBuildID]
		if buildID != "" {
			builds[buildID] = struct{}{}
		}
	}
	buildIDs := make([]string, 0, len(builds))
	for buildID := range builds {
		buildIDs = append(buildIDs, buildID)
	}
	slices.Sort(buildIDs)
	return buildIDs
}
