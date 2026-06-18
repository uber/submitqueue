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

//go:generate mockgen -source=batch_state_membership_store.go -destination=mock/batch_state_membership_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BatchStateMembershipStore records the app-maintained lookup from
// (queue, batch state) to batch IDs. The batch row remains authoritative:
// callers must resolve IDs through BatchStore and filter on the current
// persisted state.
type BatchStateMembershipStore interface {
	// Add records that batchID belongs to (queue, state). Repeating the same
	// Add is idempotent.
	Add(ctx context.Context, queue string, state entity.BatchState, batchID string) error

	// Remove deletes a single membership row. Removing a missing row is
	// idempotent and succeeds.
	Remove(ctx context.Context, queue string, state entity.BatchState, batchID string) error

	// ListIDs returns every batch ID recorded for (queue, state). An empty slice
	// means no membership rows exist for that key.
	ListIDs(ctx context.Context, queue string, state entity.BatchState) ([]string, error)
}
