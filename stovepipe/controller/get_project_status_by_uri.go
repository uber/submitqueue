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
	"errors"
	"fmt"
	"math"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	maxProjectStatusIdentifierBytes = 255
	maxProjectStatusPageSize        = 200
)

// ProjectStatusNotFoundError indicates that the selected request or project does not exist.
type ProjectStatusNotFoundError struct {
	Queue     string
	ChangeURI string
	Project   string
}

// Error implements the error interface.
func (e *ProjectStatusNotFoundError) Error() string {
	if e.Project != "" {
		return fmt.Sprintf("project %q not found for queue %q and change URI %q", e.Project, e.Queue, e.ChangeURI)
	}
	return fmt.Sprintf("request not found for queue %q and change URI %q", e.Queue, e.ChangeURI)
}

// IsProjectStatusNotFound returns true for a ProjectStatusNotFoundError in the error chain.
func IsProjectStatusNotFound(err error) bool {
	var target *ProjectStatusNotFoundError
	return errors.As(err, &target)
}

// ProjectStatusConsistencyError indicates that records for the selected projection disagree.
type ProjectStatusConsistencyError struct {
	Message string
}

// Error implements the error interface.
func (e *ProjectStatusConsistencyError) Error() string {
	return e.Message
}

// IsProjectStatusConsistency returns true for a ProjectStatusConsistencyError in the error chain.
func IsProjectStatusConsistency(err error) bool {
	var target *ProjectStatusConsistencyError
	return errors.As(err, &target)
}

// GetProjectStatusByURIController serves current repository validation status by commit URI.
type GetProjectStatusByURIController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	stores       storage.Factory
}

// NewGetProjectStatusByURIController creates a repository status lookup controller.
func NewGetProjectStatusByURIController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory) *GetProjectStatusByURIController {
	return &GetProjectStatusByURIController{
		logger:       logger,
		metricsScope: scope.SubScope("get_project_status_by_uri_controller"),
		stores:       stores,
	}
}

// GetProjectStatusByURI returns the authoritative request and any whole-repository fact for a commit URI.
func (c *GetProjectStatusByURIController) GetProjectStatusByURI(ctx context.Context, req entity.GetProjectStatusByURIRequest) (result entity.GetProjectStatusByURIResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_project_status_by_uri", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateProjectStatusRequest(req); err != nil {
		return entity.GetProjectStatusByURIResult{}, err
	}

	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	request, err := loadProjectStatusRequest(ctx, store, req)
	if err != nil {
		return entity.GetProjectStatusByURIResult{}, err
	}

	// Exact lookup needs the persisted project list to distinguish absent projects.
	if req.HasProject {
		return entity.GetProjectStatusByURIResult{}, &ProjectStatusNotFoundError{Queue: req.Queue, ChangeURI: req.ChangeURI, Project: req.Project}
	}

	repositoryFact, hasRepositoryFact, err := loadRepositoryValidationFact(ctx, store.GetValidationFactStore(), request)
	if err != nil {
		return entity.GetProjectStatusByURIResult{}, err
	}
	result = entity.GetProjectStatusByURIResult{
		Request:                     request,
		RepositoryValidationFact:    repositoryFact,
		HasRepositoryValidationFact: hasRepositoryFact,
	}

	c.logger.Debugw(
		"repository validation status retrieved",
		"request_id", result.Request.ID,
		"queue", result.Request.Queue,
		"change_uri", result.Request.URI,
		"has_repository_result", result.HasRepositoryValidationFact,
	)
	return result, nil
}

func loadProjectStatusRequest(ctx context.Context, store storage.Storage, req entity.GetProjectStatusByURIRequest) (entity.Request, error) {
	requestID, err := store.GetRequestURIStore().GetIDByURI(ctx, req.ChangeURI)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return entity.Request{}, &ProjectStatusNotFoundError{Queue: req.Queue, ChangeURI: req.ChangeURI}
		}
		return entity.Request{}, fmt.Errorf("failed to resolve request for queue %q and change URI %q: %w", req.Queue, req.ChangeURI, err)
	}

	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Ingest persists the mapping before the Request, so this visibility gap is retryable.
			return entity.Request{}, errs.NewRetryableError(fmt.Errorf("request %q mapped from queue %q and change URI %q is not visible yet", requestID, req.Queue, req.ChangeURI))
		}
		return entity.Request{}, fmt.Errorf("failed to load request %q: %w", requestID, err)
	}
	if request.ID != requestID || request.Queue != req.Queue || request.URI != req.ChangeURI {
		return entity.Request{}, &ProjectStatusConsistencyError{Message: fmt.Sprintf(
			"request mapping disagrees with request: selected id=%q queue=%q change_uri=%q, loaded id=%q queue=%q change_uri=%q",
			requestID, req.Queue, req.ChangeURI, request.ID, request.Queue, request.URI,
		)}
	}
	return request, nil
}

func loadRepositoryValidationFact(ctx context.Context, factStore storage.ValidationFactStore, request entity.Request) (entity.ValidationFact, bool, error) {
	fact, err := factStore.Get(ctx, request.URI, "")
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return entity.ValidationFact{}, false, nil
		}
		return entity.ValidationFact{}, false, fmt.Errorf("failed to load repository validation fact for request %q: %w", request.ID, err)
	}
	if err := validateRepositoryValidationFact(fact, request); err != nil {
		return entity.ValidationFact{}, false, err
	}
	return fact, true, nil
}

func validateRepositoryValidationFact(fact entity.ValidationFact, request entity.Request) error {
	if fact.URI != request.URI || fact.Project != "" || fact.RequestID != request.ID {
		return &ProjectStatusConsistencyError{Message: fmt.Sprintf(
			"repository validation fact disagrees with request %q: uri=%q project=%q request_id=%q",
			request.ID, fact.URI, fact.Project, fact.RequestID,
		)}
	}
	if math.IsNaN(fact.Degree) || fact.Degree < entity.DegreeGreen || fact.Degree > entity.DegreeBroken {
		return &ProjectStatusConsistencyError{Message: fmt.Sprintf("repository validation fact for request %q has degree %v outside [%v, %v]", request.ID, fact.Degree, entity.DegreeGreen, entity.DegreeBroken)}
	}
	return nil
}

func validateProjectStatusRequest(req entity.GetProjectStatusByURIRequest) error {
	if req.Queue == "" {
		return fmt.Errorf("queue must be non-empty: %w", ErrInvalidRequest)
	}
	if len(req.Queue) > maxProjectStatusIdentifierBytes {
		return fmt.Errorf("queue exceeds %d bytes: %w", maxProjectStatusIdentifierBytes, ErrInvalidRequest)
	}
	if req.ChangeURI == "" {
		return fmt.Errorf("change_uri must be non-empty: %w", ErrInvalidRequest)
	}
	if len(req.ChangeURI) > maxProjectStatusIdentifierBytes {
		return fmt.Errorf("change_uri exceeds %d bytes: %w", maxProjectStatusIdentifierBytes, ErrInvalidRequest)
	}
	if req.HasProject {
		if req.Project == "" {
			return fmt.Errorf("project must be non-empty when present: %w", ErrInvalidRequest)
		}
		if len(req.Project) > maxProjectStatusIdentifierBytes {
			return fmt.Errorf("project exceeds %d bytes: %w", maxProjectStatusIdentifierBytes, ErrInvalidRequest)
		}
		if req.PageSize != 0 || req.PageToken != "" {
			return fmt.Errorf("page_size and page_token must be empty when project is present: %w", ErrInvalidRequest)
		}
	}
	if req.PageSize < 0 || req.PageSize > maxProjectStatusPageSize {
		return fmt.Errorf("page_size must be between 0 and %d: %w", maxProjectStatusPageSize, ErrInvalidRequest)
	}
	if req.PageToken != "" {
		return fmt.Errorf("page_token is not valid before project results are available: %w", ErrInvalidRequest)
	}
	return nil
}
