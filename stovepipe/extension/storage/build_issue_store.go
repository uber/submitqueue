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

// BuildIssueCursor is an exclusive position in ascending bytewise task/unit order.
type BuildIssueCursor struct {
	// TaskID is the task component; empty with an empty UnitID starts the range.
	TaskID string
	// UnitID is the unit component; empty identifies a task-wide observation.
	UnitID string
}

// BuildIssueRange bounds a primary-key scan within one build.
type BuildIssueRange struct {
	// After excludes this key and earlier keys; the zero cursor starts the range.
	After BuildIssueCursor
	// Limit is the positive maximum number of observations to return.
	Limit int
}

// BuildIssueStore retains immutable observations keyed by build, task, and unit
// within the bound queue. Presence records an observation, not a currently open issue.
// Observation keys require nonempty build and task IDs; an empty unit is task-wide.
type BuildIssueStore interface {
	// Create returns ErrAlreadyExists for a previously recorded build/task/unit.
	// Callers may treat that duplicate as success when accumulating observations.
	Create(ctx context.Context, buildID string, issue entity.BuildIssue) error

	// Get returns ErrNotFound when the build/task/unit has not been recorded.
	Get(ctx context.Context, buildID, taskID, unitID string) (entity.BuildIssue, error)

	// List returns at most Limit observations in ascending bytewise (task, unit) order.
	// An empty slice ends the range; invalid bounds or an empty build ID return an error.
	// Pages are not a snapshot: concurrent observations may change membership.
	List(ctx context.Context, buildID string, bounds BuildIssueRange) ([]entity.BuildIssue, error)
}
