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

//go:generate mockgen -source=request_acceptance_store.go -destination=mock/request_acceptance_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestAcceptanceCursor is an exclusive position in descending acceptance-time order.
type RequestAcceptanceCursor struct {
	// AcceptedAtMs is the positive acceptance timestamp in Unix milliseconds; zero with an empty RequestID starts the range.
	AcceptedAtMs int64
	// RequestID breaks timestamp ties in descending bytewise order.
	RequestID string
}

// RequestAcceptanceRange bounds a primary-key scan within the store's queue.
type RequestAcceptanceRange struct {
	// AcceptedAtOrAfterMs is the inclusive nonnegative lower time bound in Unix milliseconds.
	AcceptedAtOrAfterMs int64
	// AcceptedBeforeMs is the exclusive upper time bound in Unix milliseconds and must exceed the lower bound.
	AcceptedBeforeMs int64
	// Before excludes this cursor and all keys preceding it in descending order; the zero value starts the range.
	Before RequestAcceptanceCursor
	// Limit is the positive maximum number of mappings to return.
	Limit int
}

// RequestAcceptanceStore retains immutable acceptance mappings in its bound queue.
type RequestAcceptanceStore interface {
	// Create rejects unknown acceptance times and queue mismatches; an existing composite key returns ErrAlreadyExists.
	Create(ctx context.Context, acceptance entity.RequestAcceptance) error

	// List scans the primary-key range in descending (accepted_at_ms, bytewise request_id) order.
	// It returns at most Limit mappings, an empty slice when none match, and an error for invalid ranges.
	List(ctx context.Context, bounds RequestAcceptanceRange) ([]entity.RequestAcceptance, error)
}
