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

//go:generate mockgen -source=project_issues.go -destination=mock/project_issues_mock.go -package=mock

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

const projectIssuesPageTokenVersion = 1

type projectIssueScope struct {
	Queue     string `json:"queue"`
	RequestID string `json:"request_id"`
	TaskID    string `json:"task_id,omitempty"`
	UnitID    string `json:"unit_id,omitempty"`
	Project   string `json:"project,omitempty"`
}

type projectIssuesPageToken struct {
	Version int                            `json:"version"`
	Scope   projectIssueScope              `json:"scope"`
	After   entity.ProjectIssueAttribution `json:"after"`
}

type ProjectIssuesController interface {
	ListRequestProjectsByIssue(context.Context, entity.ListRequestProjectsByIssueRequest) (entity.ListRequestProjectIssuesResult, error)
	ListRequestIssuesByProject(context.Context, entity.ListRequestIssuesByProjectRequest) (entity.ListRequestProjectIssuesResult, error)
}

type projectIssuesController struct {
	logger           *zap.SugaredLogger
	scope            tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
}

func NewProjectIssuesController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, configuredQueues []string) ProjectIssuesController {
	queues := make(map[string]struct{}, len(configuredQueues))
	for _, queue := range configuredQueues {
		queues[queue] = struct{}{}
	}
	return &projectIssuesController{logger: logger, scope: scope.SubScope("project_issues_controller"), stores: stores, configuredQueues: queues}
}

func (c *projectIssuesController) ListRequestProjectsByIssue(ctx context.Context, req entity.ListRequestProjectsByIssueRequest) (entity.ListRequestProjectIssuesResult, error) {
	if req.TaskID == "" || len(req.TaskID) > 255 {
		return entity.ListRequestProjectIssuesResult{}, fmt.Errorf("task ID must be nonempty and at most 255 bytes: %w", ErrInvalidRequest)
	}
	if req.UnitID == "" || len(req.UnitID) > 1024 {
		return entity.ListRequestProjectIssuesResult{}, fmt.Errorf("unit ID must be nonempty and at most 1024 bytes: %w", ErrInvalidRequest)
	}
	return c.list(ctx, projectIssueScope{Queue: req.Queue, RequestID: req.RequestID, TaskID: req.TaskID, UnitID: req.UnitID}, req.PageSize, req.PageToken)
}

func (c *projectIssuesController) ListRequestIssuesByProject(ctx context.Context, req entity.ListRequestIssuesByProjectRequest) (entity.ListRequestProjectIssuesResult, error) {
	if req.Project == "" || len(req.Project) > 1020 {
		return entity.ListRequestProjectIssuesResult{}, fmt.Errorf("project must be nonempty and at most 1020 bytes: %w", ErrInvalidRequest)
	}
	return c.list(ctx, projectIssueScope{Queue: req.Queue, RequestID: req.RequestID, Project: req.Project}, req.PageSize, req.PageToken)
}

func (c *projectIssuesController) list(ctx context.Context, scope projectIssueScope, pageSize int32, pageToken string) (result entity.ListRequestProjectIssuesResult, retErr error) {
	op := metrics.Begin(c.scope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if scope.Queue == "" || len(scope.Queue) > 255 {
		return result, fmt.Errorf("queue must be nonempty and at most 255 bytes: %w", ErrInvalidRequest)
	}
	if _, ok := c.configuredQueues[scope.Queue]; !ok {
		return result, fmt.Errorf("queue %q is not configured: %w", scope.Queue, ErrInvalidRequest)
	}
	if err := validateRequestID(scope.RequestID); err != nil {
		return result, err
	}
	after, limit, err := resolveProjectIssuePage(scope, pageSize, pageToken)
	if err != nil {
		return result, err
	}
	store, err := c.stores.For(storage.Config{QueueName: scope.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve request attribution storage: %w", err)
	}
	history, err := store.GetRequestLogStore().List(ctx, scope.RequestID)
	if err != nil {
		return result, fmt.Errorf("read request attribution history: %w", err)
	}
	for _, log := range history {
		if log.Event == entity.RequestEventProjectIssuesRecorded {
			result.AttributionComplete = true
			break
		}
	}
	// Do not expose a changing set while recording is still adding associations.
	if !result.AttributionComplete {
		return result, nil
	}

	var entries []entity.ProjectIssueAttribution
	if scope.Project != "" {
		entries, err = store.GetRequestIssuesByProjectStore().List(ctx, scope.RequestID, scope.Project, after, limit)
	} else {
		entries, err = store.GetRequestProjectsByIssueStore().ListByIssue(ctx, scope.RequestID, scope.TaskID, scope.UnitID, after.Project, limit)
	}
	if err != nil {
		return entity.ListRequestProjectIssuesResult{}, fmt.Errorf("list request project issue associations: %w", err)
	}
	result.Attributions = entries[:min(len(entries), limit-1)]
	if len(entries) > limit-1 {
		last := result.Attributions[len(result.Attributions)-1]
		token, err := json.Marshal(projectIssuesPageToken{Version: projectIssuesPageTokenVersion, Scope: scope, After: last})
		if err != nil {
			return entity.ListRequestProjectIssuesResult{}, fmt.Errorf("encode request attribution continuation: %w", err)
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(token)
	}
	c.logger.Debugw("request attribution listed", "queue", scope.Queue, "request_id", scope.RequestID, "count", len(result.Attributions))
	return result, nil
}

func resolveProjectIssuePage(scope projectIssueScope, pageSize int32, encoded string) (entity.ProjectIssueAttribution, int, error) {
	var after entity.ProjectIssueAttribution
	if pageSize < 0 || pageSize > maxListPageSize {
		return after, 0, fmt.Errorf("page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	if encoded == "" {
		return after, int(pageSize) + 1, nil
	}
	contents, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return after, 0, fmt.Errorf("invalid page_token encoding: %w", ErrInvalidRequest)
	}
	var token projectIssuesPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return after, 0, fmt.Errorf("invalid page_token contents: %w", ErrInvalidRequest)
	}
	if token.Version != projectIssuesPageTokenVersion || token.Scope != scope {
		return after, 0, fmt.Errorf("page_token does not match the lookup: %w", ErrInvalidRequest)
	}
	after = token.After
	if err := after.Validate(); err != nil {
		return after, 0, fmt.Errorf("invalid page_token cursor: %w", ErrInvalidRequest)
	}
	if len(after.TaskID) > 255 || len(after.UnitID) > 1024 || len(after.Project) > 1020 {
		return after, 0, fmt.Errorf("page_token cursor exceeds identifier limits: %w", ErrInvalidRequest)
	}
	if scope.Project != "" {
		if after.Project != scope.Project {
			return after, 0, fmt.Errorf("page_token cursor does not match the project: %w", ErrInvalidRequest)
		}
	} else if after.TaskID != scope.TaskID || after.UnitID != scope.UnitID {
		return after, 0, fmt.Errorf("page_token cursor does not match the issue: %w", ErrInvalidRequest)
	}
	return after, int(pageSize) + 1, nil
}
