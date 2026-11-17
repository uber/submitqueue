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

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// ListIssueHistoryController reads retained occurrences of one issue across requests.
type ListIssueHistoryController interface {
	ListIssueHistory(context.Context, entity.ListIssueHistoryRequest) (entity.ListIssueHistoryResult, error)
}

type listIssueHistoryController struct {
	scope  tally.Scope
	stores storage.Factory
	queues map[string]struct{}
}

// The maximum byte-length identities can expand through JSON escaping and base64.
const maxIssueHistoryPageTokenLength = 32768

type issueHistoryPageToken struct {
	Version int                    `json:"version"`
	Queue   string                 `json:"queue"`
	TaskID  string                 `json:"task_id"`
	UnitID  string                 `json:"unit_id"`
	After   storage.IssueLogCursor `json:"after"`
}

// NewListIssueHistoryController restricts reads to configured queues.
func NewListIssueHistoryController(scope tally.Scope, stores storage.Factory, queues []string) ListIssueHistoryController {
	configured := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		configured[queue] = struct{}{}
	}
	return &listIssueHistoryController{scope: scope.SubScope("list_issue_history_controller"), stores: stores, queues: configured}
}

func (c *listIssueHistoryController) ListIssueHistory(ctx context.Context, req entity.ListIssueHistoryRequest) (result entity.ListIssueHistoryResult, retErr error) {
	op := metrics.Begin(c.scope, "list", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	if _, ok := c.queues[req.Queue]; !ok || req.Queue == "" || len(req.Queue) > 255 {
		return result, fmt.Errorf("issue history requires a configured queue: %w", ErrInvalidRequest)
	}
	if req.TaskID == "" || len(req.TaskID) > 255 || len(req.UnitID) > 1024 {
		return result, fmt.Errorf("invalid issue task or unit identity: %w", ErrInvalidRequest)
	}
	bounds, err := resolveIssueHistoryRange(req)
	if err != nil {
		return result, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return result, fmt.Errorf("resolve issue history storage: %w", err)
	}
	logs, err := store.GetIssueLogStore().List(ctx, req.TaskID, req.UnitID, bounds)
	if err != nil {
		return result, fmt.Errorf("read issue history: %w", err)
	}
	pageSize := bounds.Limit - 1
	if len(logs) > pageSize {
		logs = logs[:pageSize]
		last := logs[len(logs)-1]
		token := issueHistoryPageToken{Version: 1, Queue: req.Queue, TaskID: req.TaskID, UnitID: req.UnitID,
			After: storage.IssueLogCursor{TimestampMs: last.TimestampMs, BuildID: last.BuildID, Event: last.Event}}
		data, err := json.Marshal(token)
		if err != nil {
			return result, fmt.Errorf("encode issue history continuation: %w", err)
		}
		result.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}
	result.Events = make([]entity.IssueHistoryEntry, 0, len(logs))
	for _, log := range logs {
		entry := entity.IssueHistoryEntry{Log: log}
		build, err := store.GetBuildStore().Get(ctx, log.BuildID)
		// Retained occurrences remain useful even after source builds have expired.
		if err != nil && !storage.IsNotFound(err) {
			return entity.ListIssueHistoryResult{}, fmt.Errorf("resolve issue source request: %w", err)
		}
		if err == nil {
			entry.RequestID = build.RequestID
		}
		result.Events = append(result.Events, entry)
	}
	return result, nil
}

func resolveIssueHistoryRange(req entity.ListIssueHistoryRequest) (storage.IssueLogRange, error) {
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return storage.IssueLogRange{}, fmt.Errorf("invalid issue history page size: %w", ErrInvalidRequest)
	}
	size := int(req.PageSize)
	if size == 0 {
		size = defaultListPageSize
	}
	bounds := storage.IssueLogRange{Limit: size + 1}
	if req.PageToken == "" {
		return bounds, nil
	}
	if len(req.PageToken) > maxIssueHistoryPageTokenLength {
		return bounds, fmt.Errorf("issue history continuation is too large: %w", ErrInvalidRequest)
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(req.PageToken)
	if err != nil {
		return bounds, fmt.Errorf("invalid issue history continuation: %w", ErrInvalidRequest)
	}
	var token issueHistoryPageToken
	if err := json.Unmarshal(data, &token); err != nil {
		return bounds, fmt.Errorf("invalid issue history continuation: %w", ErrInvalidRequest)
	}
	if token.Version != 1 || token.Queue != req.Queue || token.TaskID != req.TaskID || token.UnitID != req.UnitID {
		return bounds, fmt.Errorf("issue history continuation does not match issue: %w", ErrInvalidRequest)
	}
	after := token.After
	if after.TimestampMs <= 0 || after.BuildID == "" || len(after.BuildID) > 1020 {
		return bounds, fmt.Errorf("invalid issue history position: %w", ErrInvalidRequest)
	}
	switch after.Event {
	case entity.IssueEventOpened, entity.IssueEventReported, entity.IssueEventClosed:
	default:
		return bounds, fmt.Errorf("invalid issue history event: %w", ErrInvalidRequest)
	}
	bounds.After = after
	return bounds, nil
}
