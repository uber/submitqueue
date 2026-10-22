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

package buildslot

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// Claim reserves a request's slot in a validated queue snapshot without mutating it.
// The caller must check capacity first. A version conflict returns the current queue
// and storage.ErrVersionMismatch so admission can recheck capacity, coalescing, and baseline.
func Claim(ctx context.Context, store storage.Storage, queueRow entity.Queue, requestID string) (entity.Queue, error) {
	if queueRow.HasBuildSlot(requestID) {
		return queueRow, nil
	}
	queueStore := store.GetQueueStore()

	updated := queueRow
	updated.InFlightRequestIDs = append(slices.Clone(queueRow.InFlightRequestIDs), requestID)
	updated.InFlightCount = int32(len(updated.InFlightRequestIDs))
	newVersion := queueRow.Version + 1
	if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			got, getErr := queueStore.Get(ctx, queueRow.Name)
			if getErr != nil {
				return queueRow, fmt.Errorf("failed to reload queue %s after version mismatch: %w", queueRow.Name, getErr)
			}
			return got, storage.ErrVersionMismatch
		}
		return queueRow, fmt.Errorf("failed to claim build slot for queue %s: %w", queueRow.Name, err)
	}
	updated.Version = newVersion
	return updated, nil
}
