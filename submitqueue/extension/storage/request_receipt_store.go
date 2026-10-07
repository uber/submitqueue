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

//go:generate mockgen -source=request_receipt_store.go -destination=mock/request_receipt_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestReceiptCursor is an exclusive position in descending receipt-time order.
type RequestReceiptCursor struct {
	// ReceivedAtMs is the positive receipt timestamp in Unix milliseconds; zero with an empty RequestID starts the range.
	ReceivedAtMs int64
	// RequestID breaks timestamp ties in descending string order, not numeric order.
	RequestID string
}

// RequestReceiptRange bounds a primary-key scan within the store's queue.
type RequestReceiptRange struct {
	// ReceivedAtOrAfterMs is the inclusive lower receipt-time bound in Unix milliseconds.
	ReceivedAtOrAfterMs int64
	// ReceivedBeforeMs is the exclusive upper receipt-time bound and must exceed the lower bound.
	ReceivedBeforeMs int64
	// Before is the exclusive continuation boundary; its zero value starts the range.
	Before RequestReceiptCursor
	// Limit is the positive maximum number of mappings to return.
	Limit int
}

// RequestReceiptStore retains immutable receipt mappings in its bound queue.
type RequestReceiptStore interface {
	// Create rejects queue mismatches, nonpositive receipt times, and empty request IDs.
	// An existing composite key returns ErrAlreadyExists.
	Create(ctx context.Context, receipt entity.RequestReceipt) error

	// List scans the primary-key range in descending (received_at_ms, request_id) order.
	// It returns at most Limit mappings, an empty slice when none match, and an error for invalid ranges.
	List(ctx context.Context, bounds RequestReceiptRange) ([]entity.RequestReceipt, error)
}
