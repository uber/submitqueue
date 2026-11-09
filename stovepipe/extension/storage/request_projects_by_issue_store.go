// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

//go:generate mockgen -source=request_projects_by_issue_store.go -destination=mock/request_projects_by_issue_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestProjectsByIssueStore retains immutable attribution rows per request within the bound queue.
type RequestProjectsByIssueStore interface {
	// Create puts one row by request, task, unit, and project.
	// Duplicates return ErrAlreadyExists.
	Create(ctx context.Context, requestID string, entry entity.ProjectIssueAttribution) error

	// List scans one request in ascending bytewise task/unit/project order.
	// after is exclusive; its zero value starts the range. limit must be positive.
	// An empty page ends the range, not evidence that attribution completed.
	List(ctx context.Context, requestID string, after entity.ProjectIssueAttribution, limit int) ([]entity.ProjectIssueAttribution, error)
}
