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

package storage

//go:generate mockgen -source=request_issues_by_project_store.go -destination=mock/request_issues_by_project_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestIssuesByProjectStore holds immutable project-to-issue lookups within a request.
type RequestIssuesByProjectStore interface {
	// Create puts one lookup entry; duplicates return ErrAlreadyExists.
	Create(ctx context.Context, requestID string, entry entity.ProjectIssueAttribution) error

	// List scans the request/project prefix in ascending bytewise task/unit order.
	// after is exclusive; its zero value starts the range. A nonzero cursor must match project.
	// limit must be positive. An empty page ends the range, not evidence of completed attribution.
	// Pages are full unless the selected prefix is exhausted.
	List(ctx context.Context, requestID, project string, after entity.ProjectIssueAttribution, limit int) ([]entity.ProjectIssueAttribution, error)
}
