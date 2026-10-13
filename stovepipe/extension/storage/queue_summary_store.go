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

//go:generate mockgen -source=queue_summary_store.go -destination=mock/queue_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// QueueSummaryStore persists derived queue summaries for its bound queue.
type QueueSummaryStore interface {
	// Get returns the current summary or ErrNotFound when no projection exists.
	Get(context.Context) (entity.QueueSummary, error)
	// Create creates a summary, returning ErrAlreadyExists for an existing queue.
	Create(context.Context, entity.QueueSummary) error
	// Update conditionally replaces the summary, guarding oldVersion and writing newVersion without arithmetic.
	Update(ctx context.Context, summary entity.QueueSummary, oldVersion, newVersion int64) error
}
