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

// RequestIssueSummaryCursor is an exclusive position in bytewise task/unit order.
type RequestIssueSummaryCursor struct {
	// TaskID is the task component; empty starts the range.
	TaskID string
	// UnitID is the unit component; empty identifies a task-wide issue.
	UnitID string
}

// RequestIssueSummaryRange bounds a scan within one request's primary-key prefix.
type RequestIssueSummaryRange struct {
	// After excludes this key and earlier keys.
	After RequestIssueSummaryCursor
	// Limit is the positive maximum number of results to return.
	Limit int
}

// RequestIssueSummaryStore retains materialized per-request results within the bound queue.
// Existing means reported and already open, not every issue left open in the queue.
type RequestIssueSummaryStore interface {
	// Create returns ErrAlreadyExists without changing a previously recorded result.
	Create(context.Context, entity.RequestIssueSummary) error
	// Get returns ErrNotFound when the complete request/task/unit key is absent.
	Get(ctx context.Context, requestID, taskID, unitID string) (entity.RequestIssueSummary, error)
	// List returns at most Limit results in ascending bytewise task/unit order.
	// Results may be added while reconciliation is in progress; completed results are stable.
	List(ctx context.Context, requestID string, bounds RequestIssueSummaryRange) ([]entity.RequestIssueSummary, error)
	// Update replaces a result if its version matches oldVersion, writing newVersion.
	// Returns ErrVersionMismatch for a stale or missing result; callers own version arithmetic.
	Update(ctx context.Context, summary entity.RequestIssueSummary, oldVersion, newVersion int32) error
}
