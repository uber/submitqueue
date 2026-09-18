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

const maxProjectStatusPageSize = 200

// ProjectStatusNotFoundError indicates that no validation request matches a lookup selector.
type ProjectStatusNotFoundError struct {
	// Queue is the queue in the selector.
	Queue string
	// ChangeURI is the commit URI in the selector.
	ChangeURI string
}

func (e *ProjectStatusNotFoundError) Error() string {
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

// GetProjectStatusByURIController reads the durable repository-level validation projection.
// Project result reads are added when planned-project storage exists.
type GetProjectStatusByURIController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	stores       storage.Factory
}

// NewGetProjectStatusByURIController creates a controller for validation-status lookups.
func NewGetProjectStatusByURIController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory) *GetProjectStatusByURIController {
	return &GetProjectStatusByURIController{
		logger:       logger,
		metricsScope: scope.SubScope("get_project_status_by_uri_controller"),
		stores:       stores,
	}
}

// GetProjectStatusByURI returns the selected request and its repository validation fact, if recorded.
func (c *GetProjectStatusByURIController) GetProjectStatusByURI(ctx context.Context, req entity.GetProjectStatusByURIRequest) (result entity.GetProjectStatusByURIResult, retErr error) {
	op := metrics.Begin(c.metricsScope, "get_project_status_by_uri", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := validateProjectStatusRequest(req); err != nil {
		return entity.GetProjectStatusByURIResult{}, err
	}
	store, err := c.stores.For(storage.Config{QueueName: req.Queue})
	if err != nil {
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("GetProjectStatusByURI failed to resolve storage for queue %q: %w", req.Queue, err)
	}

	requestID, err := store.GetRequestURIStore().GetIDByURI(ctx, req.ChangeURI)
	if err != nil {
		if storage.IsNotFound(err) {
			return entity.GetProjectStatusByURIResult{}, errs.NewUserError(&ProjectStatusNotFoundError{Queue: req.Queue, ChangeURI: req.ChangeURI})
		}
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("GetProjectStatusByURI failed to resolve request for URI %q: %w", req.ChangeURI, err)
	}

	request, err := store.GetRequestStore().Get(ctx, requestID)
	if err != nil {
		if storage.IsNotFound(err) {
			// The URI mapping is created before the request, so this gap is retryable.
			return entity.GetProjectStatusByURIResult{}, errs.NewRetryableError(fmt.Errorf("GetProjectStatusByURI request %q is not visible yet", requestID))
		}
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("GetProjectStatusByURI failed to load request %q: %w", requestID, err)
	}
	if request.ID != requestID || request.Queue != req.Queue || request.URI != req.ChangeURI {
		return entity.GetProjectStatusByURIResult{}, &ProjectStatusConsistencyError{message: "request URI mapping disagrees with stored request"}
	}
	result.Request = request
	logs, err := store.GetRequestLogStore().List(ctx, request.ID)
	if storage.IsNotFound(err) {
		return entity.GetProjectStatusByURIResult{}, errs.NewRetryableError(fmt.Errorf("GetProjectStatusByURI request %q has no visible lifecycle record yet", request.ID))
	}
	if err != nil {
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("GetProjectStatusByURI failed to load request history for %q: %w", request.ID, err)
	}
	stateRecorded := false
	for _, log := range logs {
		if log.TimestampMs > result.UpdatedAtMs {
			result.UpdatedAtMs = log.TimestampMs
		}
		if log.State == request.State && log.RequestVersion == request.Version {
			stateRecorded = true
		}
	}
	if !stateRecorded {
		return entity.GetProjectStatusByURIResult{}, errs.NewRetryableError(fmt.Errorf("GetProjectStatusByURI request %q lifecycle record is not current", request.ID))
	}
	fact, err := store.GetValidationFactStore().Get(ctx, request.URI, "")
	if err != nil {
		if storage.IsNotFound(err) {
			return result, nil
		}
		return entity.GetProjectStatusByURIResult{}, fmt.Errorf("GetProjectStatusByURI failed to load repository fact for request %q: %w", request.ID, err)
	}
	if err := validateRepositoryFact(fact, request); err != nil {
		return entity.GetProjectStatusByURIResult{}, err
	}
	result.RepositoryValidationFact = fact
	result.HasRepositoryValidationFact = true
	if fact.CreatedAt > result.UpdatedAtMs {
		result.UpdatedAtMs = fact.CreatedAt
	}

	c.logger.Debugw("project status retrieved", "request_id", request.ID, "queue", request.Queue, "change_uri", request.URI, "has_repository_result", true)
	return result, nil
}

func validateProjectStatusRequest(req entity.GetProjectStatusByURIRequest) error {
	if err := validateHistoryIdentifier("queue", req.Queue); err != nil {
		return fmt.Errorf("GetProjectStatusByURI invalid queue=%q: %w", req.Queue, err)
	}
	if err := validateHistoryIdentifier("change URI", req.ChangeURI); err != nil {
		return fmt.Errorf("GetProjectStatusByURI invalid change_uri=%q: %w", req.ChangeURI, err)
	}
	if len(req.Projects) == 0 {
		return fmt.Errorf("GetProjectStatusByURI projects must be non-empty: %w", ErrInvalidRequest)
	}
	seen := make(map[string]struct{}, len(req.Projects))
	for _, project := range req.Projects {
		if err := validateHistoryIdentifier("project", project); err != nil {
			return fmt.Errorf("GetProjectStatusByURI invalid project=%q: %w", project, err)
		}
		if _, ok := seen[project]; ok {
			return fmt.Errorf("GetProjectStatusByURI project %q is duplicated: %w", project, ErrInvalidRequest)
		}
		seen[project] = struct{}{}
	}
	if req.PageSize < 0 || req.PageSize > maxProjectStatusPageSize {
		return fmt.Errorf("GetProjectStatusByURI page_size must be between 0 and %d: %w", maxProjectStatusPageSize, ErrInvalidRequest)
	}
	if req.PageToken != "" {
		return fmt.Errorf("GetProjectStatusByURI page_token is unsupported until project results are available: %w", ErrInvalidRequest)
	}
	return nil
}

func validateRepositoryFact(fact entity.ValidationFact, request entity.Request) error {
	if fact.URI != request.URI || fact.Project != "" || fact.RequestID != request.ID {
		return &ProjectStatusConsistencyError{message: "repository validation fact disagrees with stored request"}
	}
	if math.IsNaN(fact.Degree) || fact.Degree < entity.DegreeGreen || fact.Degree > entity.DegreeBroken {
		return &ProjectStatusConsistencyError{message: "repository validation fact has an invalid degree"}
	}
	return nil
}
