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

//go:generate mockgen -source=project_status.go -destination=mock/project_status_mock.go -package=mock

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

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
	Queue        string   `json:"queue"`
	RequestID    string   `json:"request_id"`
	Projects     []string `json:"projects"`
	NextIndex    int      `json:"next_index"`
	AfterProject string   `json:"after_project,omitempty"`
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

// GetProjectStatus returns the selected request summary and recorded validation facts.
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

	page, err := loadProjectStatusPage(ctx, req, requestSummary, factStore)
	if err != nil {
		return entity.GetProjectStatusResult{}, err
	}

	for _, projectFact := range page.facts {
		if len(req.Projects) == 0 && projectFact.IsGreen() {
			continue
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
	facts     []entity.ValidationFact
	nextToken string
}

func loadProjectStatusPage(
	ctx context.Context,
	req entity.GetProjectStatusRequest,
	summary entity.RequestSummary,
	store storage.ValidationFactStore,
) (projectStatusPage, error) {
	token, err := parseProjectStatusPageToken(req, summary)
	if err != nil {
		return projectStatusPage{}, err
	}

	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultProjectStatusPageSize
	}

	if len(req.Projects) == 0 {
		return listProjectStatusPage(ctx, summary, store, token, pageSize)
	}

	return getRequestedProjectStatusPage(ctx, summary, store, token, pageSize)
}

func getRequestedProjectStatusPage(
	ctx context.Context,
	summary entity.RequestSummary,
	store storage.ValidationFactStore,
	token projectStatusPageToken,
	pageSize int,
) (projectStatusPage, error) {
	end := min(token.NextIndex+pageSize, len(token.Projects))
	var page projectStatusPage

	for _, project := range token.Projects[token.NextIndex:end] {
		fact, err := store.Get(ctx, summary.URI, project)
		if err != nil {
			if storage.IsNotFound(err) {
				continue
			}

			return projectStatusPage{}, fmt.Errorf(
				"GetProjectStatus failed to load project fact for request %q and project %q: %w",
				summary.RequestID, project, err,
			)
		}

		if err := validateProjectFact(fact, summary, project); err != nil {
			return projectStatusPage{}, err
		}

		page.facts = append(page.facts, fact)
	}

	var err error
	if end < len(token.Projects) {
		token.NextIndex = end
		page.nextToken, err = encodeProjectStatusPageToken(token)
	}

	return page, err
}

func listProjectStatusPage(
	ctx context.Context,
	summary entity.RequestSummary,
	store storage.ValidationFactStore,
	token projectStatusPageToken,
	pageSize int,
) (projectStatusPage, error) {
	facts, err := store.ListProjects(ctx, summary.URI, token.AfterProject, pageSize+1)
	if err != nil {
		return projectStatusPage{}, fmt.Errorf("GetProjectStatus failed to list project facts for request %q: %w", summary.RequestID, err)
	}

	for _, fact := range facts {
		if err := validateProjectFact(fact, summary, fact.Project); err != nil {
			return projectStatusPage{}, err
		}

		if fact.Project == "" {
			return projectStatusPage{}, &ProjectStatusConsistencyError{message: "project listing contains a repository fact"}
		}
	}

	page := projectStatusPage{facts: facts}
	if len(facts) > pageSize {
		page.facts = facts[:pageSize]
		token.AfterProject = page.facts[pageSize-1].Project
		page.nextToken, err = encodeProjectStatusPageToken(token)
	}

	return page, err
}

func encodeProjectStatusPageToken(token projectStatusPageToken) (string, error) {
	contents, err := json.Marshal(token)
	if err != nil {
		return "", fmt.Errorf("GetProjectStatus failed to create page token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(contents), nil
}

func parseProjectStatusPageToken(
	req entity.GetProjectStatusRequest,
	summary entity.RequestSummary,
) (projectStatusPageToken, error) {
	if req.PageToken == "" {
		return projectStatusPageToken{
			Queue:     summary.Queue,
			RequestID: summary.RequestID,
			Projects:  req.Projects,
		}, nil
	}

	invalidToken := fmt.Errorf("GetProjectStatus page_token does not match the selected request and projects: %w", ErrInvalidRequest)

	contents, err := base64.RawURLEncoding.DecodeString(req.PageToken)
	if err != nil {
		return projectStatusPageToken{}, invalidToken
	}

	var token projectStatusPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return projectStatusPageToken{}, invalidToken
	}

	if token.Queue != summary.Queue ||
		token.RequestID != summary.RequestID ||
		!slices.Equal(token.Projects, req.Projects) {
		return projectStatusPageToken{}, invalidToken
	}

	if len(req.Projects) == 0 {
		if token.NextIndex != 0 || validateHistoryIdentifier("project", token.AfterProject) != nil {
			return projectStatusPageToken{}, invalidToken
		}
	} else if token.AfterProject != "" ||
		token.NextIndex <= 0 ||
		token.NextIndex >= len(req.Projects) {
		return projectStatusPageToken{}, invalidToken
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
