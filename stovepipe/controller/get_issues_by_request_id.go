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

//go:generate mockgen -source=get_issues_by_request_id.go -destination=mock/get_issues_by_request_id_mock.go -package=mock

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const requestIssuesPageTokenVersion = 1

type requestIssuesPageToken struct {
	Version    int    `json:"version"`
	Queue      string `json:"queue"`
	RequestID  string `json:"request_id"`
	LastTaskID string `json:"last_task_id"`
	LastUnitID string `json:"last_unit_id"`
}

// GetIssuesByRequestIDController reads accepted issue results for configured queues.
type GetIssuesByRequestIDController interface {
	GetIssuesByRequestID(context.Context, entity.GetIssuesByRequestIDRequest) (entity.GetIssuesByRequestIDResult, error)
}

type getIssuesByRequestIDController struct {
	logger           *zap.SugaredLogger
	scope            tally.Scope
	stores           storage.Factory
	configuredQueues map[string]struct{}
}

// NewGetIssuesByRequestIDController restricts request issue reads to configured queues.
func NewGetIssuesByRequestIDController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, configuredQueues []string) GetIssuesByRequestIDController {
	queues := make(map[string]struct{}, len(configuredQueues))
	for _, queue := range configuredQueues {
		queues[queue] = struct{}{}
	}
	return &getIssuesByRequestIDController{logger: logger, scope: scope.SubScope("get_issues_by_request_id_controller"), stores: stores, configuredQueues: queues}
}

func (c *getIssuesByRequestIDController) GetIssuesByRequestID(ctx context.Context, req entity.GetIssuesByRequestIDRequest) (result entity.GetIssuesByRequestIDResult, retErr error) {
	op := metrics.Begin(c.scope, "get", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	if req.Queue == "" {
		return result, fmt.Errorf("GetIssuesByRequestID requires a queue: %w", ErrInvalidRequest)
	}
	if _, ok := c.configuredQueues[req.Queue]; !ok {
		return result, fmt.Errorf("GetIssuesByRequestID queue %q is not configured: %w", req.Queue, ErrInvalidRequest)
	}
	if err := validateRequestID(req.RequestID); err != nil {
		return result, err
	}
	bounds, err := resolveRequestIssueSummaryRange(req)
	if err != nil {
		return result, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve request issue storage: %w", err)
	}
	history, err := store.GetRequestLogStore().List(ctx, req.RequestID)
	if storage.IsNotFound(err) {
		return result, errs.NewUserError(fmt.Errorf("request issue history is not retained: %w", err))
	}
	if err != nil {
		return result, fmt.Errorf("read request issue history: %w", err)
	}
	// Read completion before the page: a complete response must follow all result writes.
	result.Status = requestIssuesStatus(history)
	issues, err := store.GetRequestIssueSummaryStore().List(ctx, req.RequestID, bounds)
	if err != nil {
		return entity.GetIssuesByRequestIDResult{}, fmt.Errorf("list request issues: %w", err)
	}
	pageSize := bounds.Limit - 1
	result.Issues = issues[:min(len(issues), pageSize)]
	if len(issues) > pageSize {
		last := result.Issues[len(result.Issues)-1]
		contents, err := json.Marshal(requestIssuesPageToken{Version: requestIssuesPageTokenVersion, Queue: req.Queue, RequestID: req.RequestID, LastTaskID: last.TaskID, LastUnitID: last.UnitID})
		if err != nil {
			return entity.GetIssuesByRequestIDResult{}, fmt.Errorf("encode request issue continuation: %w", err)
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(contents)
	}
	c.logger.Debugw("request issues retrieved", "queue", req.Queue, "request_id", req.RequestID, "issue_count", len(result.Issues), "status", result.Status)
	return result, nil
}

func requestIssuesStatus(history []entity.RequestLog) entity.RequestIssuesStatus {
	started, completed, abandoned := false, false, false
	for _, log := range history {
		switch log.Event {
		case entity.RequestEventIssueTrackingStarted:
			started = true
		case entity.RequestEventIssuesReconciled:
			completed = true
		case entity.RequestEventIssueTrackingAbandoned:
			abandoned = true
		}
	}
	if abandoned {
		return entity.RequestIssuesStatusIncomplete
	}
	if !started {
		return entity.RequestIssuesStatusUnavailable
	}
	if completed {
		return entity.RequestIssuesStatusComplete
	}
	return entity.RequestIssuesStatusInProgress
}

func resolveRequestIssueSummaryRange(req entity.GetIssuesByRequestIDRequest) (storage.RequestIssueSummaryRange, error) {
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	bounds := storage.RequestIssueSummaryRange{Limit: pageSize + 1}
	if req.PageToken == "" {
		return bounds, nil
	}
	contents, err := base64.RawURLEncoding.Strict().DecodeString(req.PageToken)
	if err != nil {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID invalid page_token encoding: %w", ErrInvalidRequest)
	}
	var token requestIssuesPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID invalid page_token contents: %w", ErrInvalidRequest)
	}
	if token.Version != requestIssuesPageTokenVersion {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID invalid page_token version: %w", ErrInvalidRequest)
	}
	if token.Queue != req.Queue || token.RequestID != req.RequestID {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID page_token does not match the queue and request: %w", ErrInvalidRequest)
	}
	if token.LastTaskID == "" {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID page_token requires a task ID: %w", ErrInvalidRequest)
	}
	if len(token.LastTaskID) > 255 || len(token.LastUnitID) > 1024 {
		return storage.RequestIssueSummaryRange{}, fmt.Errorf("GetIssuesByRequestID page_token key exceeds identifier limits: %w", ErrInvalidRequest)
	}
	bounds.After = storage.RequestIssueSummaryCursor{TaskID: token.LastTaskID, UnitID: token.LastUnitID}
	return bounds, nil
}
