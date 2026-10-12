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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	defaultProjectStatusPageSize = 50
	maxProjectStatusPageSize     = 200
)

type projectStatusPageToken struct {
	RequestID string   `json:"request_id"`
	Projects  []string `json:"projects"`
	NextIndex int      `json:"next_index"`
}

// ProjectStatusNotFoundError indicates that no validation request matches a lookup selector.
type ProjectStatusNotFoundError struct {
	// Queue is the queue in the selector.
	Queue string
	// RequestID is the request identifier in an ID selector, empty for a URI selector.
	RequestID string
	// ChangeURI is the commit URI in the selector.
	ChangeURI string
}

func (e *ProjectStatusNotFoundError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("project status not found for queue %q and request ID %q", e.Queue, e.RequestID)
	}
	return fmt.Sprintf("project status not found for queue %q and change URI %q", e.Queue, e.ChangeURI)
}

// IsProjectStatusNotFound reports whether err represents an unknown validation request.
func IsProjectStatusNotFound(err error) bool {
	var target *ProjectStatusNotFoundError
	return errors.As(err, &target)
}

// ProjectStatusConsistencyError indicates that persisted records disagree about a request.
type ProjectStatusConsistencyError struct {
	message string
}

func (e *ProjectStatusConsistencyError) Error() string { return e.message }

// IsProjectStatusConsistency reports whether err represents inconsistent persisted state.
func IsProjectStatusConsistency(err error) bool {
	var target *ProjectStatusConsistencyError
	return errors.As(err, &target)
}

// ProjectStatusController exposes durable validation projections selected by ID or URI.
type ProjectStatusController interface {
	GetProjectStatus(context.Context, entity.GetProjectStatusRequest) (entity.GetProjectStatusResult, error)
}

var _ ProjectStatusController = (*projectStatusController)(nil)

type projectStatusController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	stores       storage.Factory
}

// NewProjectStatusController creates a controller for validation-status lookups.
func NewProjectStatusController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory) ProjectStatusController {
	return &projectStatusController{
		logger:       logger,
		metricsScope: scope.SubScope("project_status_controller"),
		stores:       stores,
	}
}

// GetProjectStatus returns the selected request summary and its repository validation fact, if recorded.
func (c *projectStatusController) GetProjectStatus(ctx context.Context, req entity.GetProjectStatusRequest) (result entity.GetProjectStatusResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_project_status", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := validateProjectStatusRequest(req); err != nil {
		return entity.GetProjectStatusResult{}, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.GetProjectStatusResult{}, fmt.Errorf("GetProjectStatus failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	requestID := req.RequestID
	if req.ChangeURI != "" {
		requestID, err = store.GetRequestURIStore().GetIDByURI(ctx, req.ChangeURI)
		if err != nil {
			if storage.IsNotFound(err) {
				return entity.GetProjectStatusResult{}, errs.NewUserError(&ProjectStatusNotFoundError{Queue: req.Queue, ChangeURI: req.ChangeURI})
			}
			return entity.GetProjectStatusResult{}, fmt.Errorf("GetProjectStatus failed to resolve request for URI %q: %w", req.ChangeURI, err)
		}
	}

	requestSummary, err := store.GetRequestSummaryStore().Get(ctx, requestID)
	if err != nil {
		if storage.IsNotFound(err) {
			if req.RequestID != "" {
				return entity.GetProjectStatusResult{}, errs.NewUserError(&ProjectStatusNotFoundError{Queue: req.Queue, RequestID: req.RequestID})
			}
			// The URI mapping is created before the summary, so this gap is retryable.
			return entity.GetProjectStatusResult{}, errs.NewRetryableError(fmt.Errorf("GetProjectStatus request summary %q is not visible yet", requestID))
		}
		return entity.GetProjectStatusResult{}, fmt.Errorf("GetProjectStatus failed to load request summary %q: %w", requestID, err)
	}
	if requestSummary.RequestID != requestID || requestSummary.Queue != req.Queue || requestSummary.URI == "" || (req.ChangeURI != "" && requestSummary.URI != req.ChangeURI) {
		return entity.GetProjectStatusResult{}, &ProjectStatusConsistencyError{message: "project status selector disagrees with stored request summary"}
	}
	result.RequestSummary = requestSummary
	result.UpdatedAtMs = requestSummary.StateTimestampMs
	factStore := store.GetValidationFactStore()
	fact, err := factStore.Get(ctx, requestSummary.URI, "")
	if err != nil {
		if !storage.IsNotFound(err) {
			return entity.GetProjectStatusResult{}, fmt.Errorf("GetProjectStatus failed to load repository fact for request %q: %w", requestSummary.RequestID, err)
		}
	} else {
		if err := validateRepositoryFact(fact, requestSummary); err != nil {
			return entity.GetProjectStatusResult{}, err
		}
		result.RepositoryValidationFact = fact
		result.HasRepositoryValidationFact = true
		if fact.CreatedAt > result.UpdatedAtMs {
			result.UpdatedAtMs = fact.CreatedAt
		}
	}
	page, err := selectProjectStatusPage(req, requestSummary.RequestID)
	if err != nil {
		return entity.GetProjectStatusResult{}, err
	}
	for _, project := range page.projects {
		projectFact, err := factStore.Get(ctx, requestSummary.URI, project)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}
			return entity.GetProjectStatusResult{}, fmt.Errorf("GetProjectStatus failed to load project fact for request %q and project %q: %w", requestSummary.RequestID, project, err)
		}
		if err := validateProjectFact(projectFact, requestSummary, project); err != nil {
			return entity.GetProjectStatusResult{}, err
		}
		result.ProjectValidationFacts = append(result.ProjectValidationFacts, projectFact)
		if projectFact.CreatedAt > result.UpdatedAtMs {
			result.UpdatedAtMs = projectFact.CreatedAt
		}
	}
	result.NextPageToken = page.nextToken

	c.logger.Debugw("project status retrieved", "request_id", requestSummary.RequestID, "queue", requestSummary.Queue, "change_uri", requestSummary.URI, "has_repository_result", result.HasRepositoryValidationFact, "project_results", len(result.ProjectValidationFacts))
	return result, nil
}

func validateProjectStatusRequest(req entity.GetProjectStatusRequest) error {
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return fmt.Errorf("GetProjectStatus invalid queue=%q: %w", req.Queue, err)
	}
	if (req.RequestID == "") == (req.ChangeURI == "") {
		return fmt.Errorf("GetProjectStatus requires exactly one of request_id or change_uri: %w", ErrInvalidRequest)
	}
	if req.RequestID != "" {
		if err := validateRequestID(req.RequestID); err != nil {
			return fmt.Errorf("GetProjectStatus invalid request_id=%q: %w", req.RequestID, err)
		}
	} else {
		if err := validateHistoryIdentifier("change URI", req.ChangeURI); err != nil {
			return fmt.Errorf("GetProjectStatus invalid change_uri=%q: %w", req.ChangeURI, err)
		}
	}
	if len(req.Projects) == 0 {
		return fmt.Errorf("GetProjectStatus projects must be non-empty: %w", ErrInvalidRequest)
	}
	seen := make(map[string]struct{}, len(req.Projects))
	for _, project := range req.Projects {
		if err := validateHistoryIdentifier("project", project); err != nil {
			return fmt.Errorf("GetProjectStatus invalid project=%q: %w", project, err)
		}
		if _, ok := seen[project]; ok {
			return fmt.Errorf("GetProjectStatus project %q is duplicated: %w", project, ErrInvalidRequest)
		}
		seen[project] = struct{}{}
	}
	if req.PageSize < 0 || req.PageSize > maxProjectStatusPageSize {
		return fmt.Errorf("GetProjectStatus page_size must be between 0 and %d: %w", maxProjectStatusPageSize, ErrInvalidRequest)
	}
	return nil
}

type projectStatusPage struct {
	projects  []string
	nextToken string
}

func selectProjectStatusPage(req entity.GetProjectStatusRequest, requestID string) (projectStatusPage, error) {
	start := 0
	if req.PageToken != "" {
		token, err := decodeProjectStatusPageToken(req.PageToken)
		if err != nil {
			return projectStatusPage{}, err
		}
		if token.RequestID != requestID || !reflect.DeepEqual(token.Projects, req.Projects) || token.NextIndex <= 0 || token.NextIndex >= len(req.Projects) {
			return projectStatusPage{}, fmt.Errorf("GetProjectStatus page_token does not match the selected request and projects: %w", ErrInvalidRequest)
		}
		start = token.NextIndex
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultProjectStatusPageSize
	}
	end := min(start+pageSize, len(req.Projects))
	page := projectStatusPage{projects: req.Projects[start:end]}
	if end < len(req.Projects) {
		token, err := encodeProjectStatusPageToken(projectStatusPageToken{RequestID: requestID, Projects: req.Projects, NextIndex: end})
		if err != nil {
			return projectStatusPage{}, fmt.Errorf("GetProjectStatus failed to create page token: %w", err)
		}
		page.nextToken = token
	}
	return page, nil
}

func encodeProjectStatusPageToken(token projectStatusPageToken) (string, error) {
	contents, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(contents), nil
}

func decodeProjectStatusPageToken(encoded string) (projectStatusPageToken, error) {
	contents, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return projectStatusPageToken{}, fmt.Errorf("GetProjectStatus invalid page_token: %w", ErrInvalidRequest)
	}
	var token projectStatusPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return projectStatusPageToken{}, fmt.Errorf("GetProjectStatus invalid page_token: %w", ErrInvalidRequest)
	}
	return token, nil
}

func validateRepositoryFact(fact entity.ValidationFact, requestSummary entity.RequestSummary) error {
	if fact.URI != requestSummary.URI || fact.Project != "" || fact.RequestID != requestSummary.RequestID {
		return &ProjectStatusConsistencyError{message: "repository validation fact disagrees with stored request"}
	}
	if math.IsNaN(fact.Degree) || fact.Degree < entity.DegreeGreen || fact.Degree > entity.DegreeBroken {
		return &ProjectStatusConsistencyError{message: "repository validation fact has an invalid degree"}
	}
	return nil
}

func validateProjectFact(fact entity.ValidationFact, requestSummary entity.RequestSummary, project string) error {
	if fact.URI != requestSummary.URI || fact.Project != project || fact.RequestID != requestSummary.RequestID {
		return &ProjectStatusConsistencyError{message: "project validation fact disagrees with stored request"}
	}
	if math.IsNaN(fact.Degree) || fact.Degree < entity.DegreeGreen || fact.Degree > entity.DegreeBroken {
		return &ProjectStatusConsistencyError{message: "project validation fact has an invalid degree"}
	}
	return nil
}
