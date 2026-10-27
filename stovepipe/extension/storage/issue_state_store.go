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

//go:generate mockgen -source=issue_state_store.go -destination=mock/issue_state_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueStateCursor is an exclusive position in bytewise task/unit order.
type IssueStateCursor struct {
	// TaskID is the task component; empty with an empty UnitID starts the range.
	TaskID string
	// UnitID is the unit component; empty identifies a task-wide issue.
	UnitID string
}

// IssueStateRange bounds a primary-key scan within the store's queue.
type IssueStateRange struct {
	// After excludes this key and earlier keys; the zero cursor starts the range.
	After IssueStateCursor
	// Limit is the positive maximum number of records to read, including closed ones.
	Limit int
}

// IssueStateStore retains one versioned record per task/unit within its queue.
// Closing retains the record and its version; there is no deletion operation.
type IssueStateStore interface {
	// Create returns ErrAlreadyExists for an existing key, including a closed issue.
	Create(context.Context, entity.IssueState) error
	// Get returns ErrNotFound only when the task/unit has never been recorded.
	Get(ctx context.Context, taskID, unitID string) (entity.IssueState, error)
	// Update replaces the record only at oldVersion; callers supply newVersion.
	Update(ctx context.Context, issue entity.IssueState, oldVersion, newVersion int32) error
	// List returns at most Limit records, including closed issues, in bytewise key order.
	// Callers filter by status and continue after the last scanned key, not the last open issue.
	List(context.Context, IssueStateRange) ([]entity.IssueState, error)
}
