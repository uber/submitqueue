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

// RequestSummaryStore persists the authoritative request-ID materialized view.
type RequestSummaryStore interface {
	// Create inserts summary and returns ErrAlreadyExists when RequestID already exists.
	// The caller owns retry identity and decides whether an existing row is an identical retry or a conflict.
	Create(ctx context.Context, summary entity.RequestSummary) error

	// Get returns the summary for requestID, or ErrNotFound when absent.
	Get(ctx context.Context, requestID string) (entity.RequestSummary, error)

	// Update conditionally replaces the mutable status fields when the persisted projection version equals oldVersion.
	// The store writes newVersion exactly as supplied and returns ErrVersionMismatch when the guard does not match.
	Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int32) error
}
