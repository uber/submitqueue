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

// RequestSummarySort defines supported deterministic orderings for request-summary pages.
type RequestSummarySort string

const (
	// RequestSummarySortAdmittedAsc returns FIFO/admission order.
	RequestSummarySortAdmittedAsc RequestSummarySort = "admitted_asc"
	// RequestSummarySortAdmittedDesc returns newest admissions first.
	RequestSummarySortAdmittedDesc RequestSummarySort = "admitted_desc"
)

// RequestSummaryStore maintains the gateway-owned read model for queue/time-window listing.
type RequestSummaryStore interface {
	// UpsertFromLog incrementally merges one request-log event into the summary read model.
	UpsertFromLog(ctx context.Context, log entity.RequestLog) error

	// List returns a page of request summaries matching the queue, time window, and optional statuses.
	List(ctx context.Context, opts RequestSummaryListOptions) (RequestSummaryListResult, error)
}

// RequestSummaryListOptions defines a page-in request for RequestSummaryStore.
type RequestSummaryListOptions struct {
	Queue       string
	StartTimeMs int64
	EndTimeMs   int64
	Statuses    []entity.RequestStatus
	Sort        RequestSummarySort
	Cursor      *RequestSummaryCursor
	Limit       int
}

// RequestSummaryCursor is the stable cursor position under the selected admission-time ordering.
type RequestSummaryCursor struct {
	StartedAtMs int64
	RequestID   string
}

// RequestSummaryListResult is one page of request summaries.
type RequestSummaryListResult struct {
	Requests   []entity.RequestSummary
	NextCursor *RequestSummaryCursor
}
