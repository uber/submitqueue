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

//go:generate mockgen -source=build_issue_store.go -destination=mock/build_issue_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// BuildIssueStore retains immutable observations keyed by build, task, and unit
// within the bound queue. Presence records an observation, not a currently open issue.
type BuildIssueStore interface {
	// Create returns ErrAlreadyExists for a previously recorded build/task/unit.
	// Callers may treat that duplicate as success when accumulating observations.
	Create(ctx context.Context, buildID string, issue entity.BuildIssue) error

	// Get returns ErrNotFound when the build/task/unit has not been recorded.
	Get(ctx context.Context, buildID, taskID, unitID string) (entity.BuildIssue, error)
}
