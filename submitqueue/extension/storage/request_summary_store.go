// Copyright (c) 2025 Uber Technologies, Inc.
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

//go:generate mockgen -source=request_summary_store.go -destination=mock/request_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestSummarySort defines deterministic request-summary page orderings.
type RequestSummarySort string

const (
	RequestSummarySortAdmittedAsc  RequestSummarySort = "admitted_asc"
	RequestSummarySortAdmittedDesc RequestSummarySort = "admitted_desc"
)

// RequestSummaryStore persists and queries the gateway List read model. Projection decisions belong to callers.
type RequestSummaryStore interface {
	Create(ctx context.Context, summary entity.RequestSummary) error
	Get(ctx context.Context, queue, requestID string) (entity.RequestSummary, error)
	Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int64) error
	List(ctx context.Context, opts RequestSummaryListOptions) (RequestSummaryListResult, error)
}

// RequestSummaryListOptions defines a queue-scoped admission-time page query. Queue, positive StartTimeMs and EndTimeMs values satisfying StartTimeMs < EndTimeMs, and a positive Limit are required. Empty Statuses disables status filtering. Empty Sort selects RequestSummarySortAdmittedAsc. Nil Cursor selects the first page; a non-nil Cursor continues strictly after its complete (StartedAtMs, RequestID) ordering key.
type RequestSummaryListOptions struct {
	Queue       string
	StartTimeMs int64
	EndTimeMs   int64
	Statuses    []entity.RequestStatus
	Sort        RequestSummarySort
	// Cursor is nil for the first page; otherwise it is the last-seen ordering key.
	Cursor *RequestSummaryCursor
	Limit  int
}

// RequestSummaryCursor is the complete last-seen (started_at_ms, request_id) ordering key.
type RequestSummaryCursor struct {
	StartedAtMs int64
	RequestID   string
}

type RequestSummaryListResult struct {
	Requests   []entity.RequestSummary
	NextCursor *RequestSummaryCursor
}
