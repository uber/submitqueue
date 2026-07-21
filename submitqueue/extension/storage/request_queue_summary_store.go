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

//go:generate mockgen -source=request_queue_summary_store.go -destination=mock/request_queue_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestQueueSummaryCursor is the exclusive keyset boundary for a descending queue-summary query.
type RequestQueueSummaryCursor struct {
	// ReceivedAtMs is the receipt timestamp of the last item from the previous page.
	ReceivedAtMs int64
	// RequestID is the request ID of the last item from the previous page.
	RequestID string
}

// RequestQueueSummaryQuery specifies one bounded queue-summary page query.
type RequestQueueSummaryQuery struct {
	// Queue is the exact queue partition to scan.
	Queue string
	// ReceivedAtOrAfterMs is the inclusive lower receipt-time bound.
	ReceivedAtOrAfterMs int64
	// ReceivedBeforeMs is the exclusive upper receipt-time bound.
	ReceivedBeforeMs int64
	// Cursor is an exclusive continuation boundary when HasCursor is true.
	Cursor RequestQueueSummaryCursor
	// HasCursor selects whether Cursor participates in the query.
	HasCursor bool
	// Limit is the maximum number of rows returned and must be positive.
	Limit int
}

// RequestQueueSummaryStore persists the queue-ordered request projection.
type RequestQueueSummaryStore interface {
	// Create inserts summary and returns ErrAlreadyExists when its full primary key already exists.
	Create(ctx context.Context, summary entity.RequestQueueSummary) error

	// Get returns the row identified by its full primary key, or errs.ErrNotFound when absent.
	Get(ctx context.Context, queue string, receivedAtMs int64, requestID string) (entity.RequestQueueSummary, error)

	// Update conditionally replaces mutable fields when the persisted projection version equals oldVersion.
	// The store writes newVersion exactly as supplied and returns errs.ErrVersionMismatch when the guard does not match.
	Update(ctx context.Context, summary entity.RequestQueueSummary, oldVersion, newVersion int32) error

	// List returns at most query.Limit rows ordered by received_at_ms descending, then request_id descending.
	List(ctx context.Context, query RequestQueueSummaryQuery) ([]entity.RequestQueueSummary, error)
}
