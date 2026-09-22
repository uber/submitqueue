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

//go:generate mockgen -source=request_summary_store.go -destination=mock/request_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestSummaryStore persists queue-scoped request status projections.
type RequestSummaryStore interface {
	// Create persists summary and returns ErrAlreadyExists when its identity exists.
	Create(ctx context.Context, summary entity.RequestSummary) error

	// Get returns the summary identified by requestID, or ErrNotFound when absent.
	Get(ctx context.Context, requestID string) (entity.RequestSummary, error)

	// Update conditionally replaces summary using the supplied optimistic-lock versions.
	Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int32) error
}
