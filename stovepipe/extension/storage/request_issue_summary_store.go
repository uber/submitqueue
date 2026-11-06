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

//go:generate mockgen -source=request_issue_summary_store.go -destination=mock/request_issue_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestIssueCursor is an exclusive bytewise build/task/unit position.
type RequestIssueCursor struct {
	// BuildID is the build component; empty with an empty task/unit starts the range.
	BuildID string
	// TaskID is the task component; empty selects the start of the build.
	TaskID string
	// UnitID is the unit component; empty identifies a task-wide observation.
	UnitID string
}

// RequestIssueRange bounds a primary-key range within one request.
type RequestIssueRange struct {
	// After excludes this key and all earlier keys.
	After RequestIssueCursor
	// Limit is the positive maximum number of summaries to return.
	Limit int
}

// RequestIssueSummaryStore retains queue-scoped observation read models by request/build/task/unit.
// The caller merges observations, classification, and repair events; projection versions do not order sources.
type RequestIssueSummaryStore interface {
	// Create returns ErrAlreadyExists when the complete observation key exists.
	Create(context.Context, entity.RequestIssueSummary) error
	// Get returns ErrNotFound for an absent complete key.
	Get(ctx context.Context, requestID, buildID, taskID, unitID string) (entity.RequestIssueSummary, error)
	// Update replaces mutable fields at oldVersion and writes newVersion.
	// A stale or absent key returns ErrVersionMismatch.
	Update(ctx context.Context, summary entity.RequestIssueSummary, oldVersion, newVersion int32) error
	// List reads a bounded request-prefix range in ascending bytewise build/task/unit order.
	// An empty page ends the range; concurrent writes may affect membership across calls.
	List(ctx context.Context, requestID string, bounds RequestIssueRange) ([]entity.RequestIssueSummary, error)
}
