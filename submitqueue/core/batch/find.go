// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// FindByRequestID resolves every batch attempt associated with a request,
// ordered by batch ID. The batches are independent of each other, but a
// deterministic order stabilizes logs, tests, and first-error selection.
//
// An association whose batch row is missing is skipped and counted in stale
// rather than failing the call: the batch row and the association are separate
// writes, so an attempt that died between them leaves the association behind.
// The count is returned so callers can meter it without re-reading.
//
// Unlike ListByStates, which treats a dangling membership record as store
// corruption, a dangling association is an expected retry artifact.
func FindByRequestID(ctx context.Context, store storage.Storage, requestID string) ([]entity.Batch, int, error) {
	associations, err := store.GetRequestBatchStore().GetByRequestID(ctx, requestID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get batch associations for request %s: %w", requestID, err)
	}

	stale := 0
	var batches []entity.Batch
	for _, association := range associations {
		batch, err := store.GetBatchStore().Get(ctx, association.BatchID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				stale++
				continue
			}
			return nil, 0, fmt.Errorf("failed to get associated batch %s for request %s: %w", association.BatchID, requestID, err)
		}
		batches = append(batches, batch)
	}

	sort.Slice(batches, func(i, j int) bool {
		return batches[i].ID < batches[j].ID
	})
	return batches, stale, nil
}
