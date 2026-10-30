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

//go:generate mockgen -source=open_issue_store.go -destination=mock/open_issue_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// OpenIssueCursor is an exclusive position in bytewise task/unit order.
type OpenIssueCursor struct {
	// TaskID is the task component; empty with an empty UnitID starts the range.
	TaskID string
	// UnitID is the unit component; empty identifies a task-wide issue.
	UnitID string
}

// OpenIssueRange bounds a primary-key scan within the store's queue.
type OpenIssueRange struct {
	// After excludes this key and earlier keys; the zero cursor starts the range.
	After OpenIssueCursor
	// Limit is the positive maximum number of open issues to return.
	Limit int
}

// OpenIssueStore retains queue-scoped opening references keyed by task/unit.
// Open-issue rows are derived lookups; consumers must validate them against issue summaries.
type OpenIssueStore interface {
	// Create returns ErrAlreadyExists if the task/unit key already exists.
	Create(ctx context.Context, issue entity.OpenIssue) error

	// Get returns ErrNotFound when the task/unit key has no open issue.
	Get(ctx context.Context, taskID, unitID string) (entity.OpenIssue, error)

	// Replace changes the opening only when it matches expectedFirstSeenBuildID.
	// It returns false for a missing key, a mismatch, or an unchanged opening.
	Replace(ctx context.Context, issue entity.OpenIssue, expectedFirstSeenBuildID string) (bool, error)

	// Delete removes the open issue only when its opening matches issue.FirstSeenBuildID.
	// It returns false for a missing key or a different opening.
	Delete(ctx context.Context, issue entity.OpenIssue) (bool, error)

	// List returns at most Limit open issues in bytewise task/unit order.
	// Membership may change between reads; a cursor need not still exist.
	List(ctx context.Context, bounds OpenIssueRange) ([]entity.OpenIssue, error)
}
