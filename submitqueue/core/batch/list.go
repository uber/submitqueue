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
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// hydrateConcurrency bounds the parallel per-key batch reads a single
// ListByStates call issues while hydrating candidate IDs.
const hydrateConcurrency = 16

// ListByStates returns the bound queue's batches whose current state is one of the
// given states, read through the queue's membership records: each requested state
// bucket is listed, candidate IDs are deduplicated across buckets, every candidate is
// hydrated by key with bounded concurrency, and the result keeps only batches whose
// hydrated State is in states. Classification always uses the hydrated state — a
// record found in a stale bucket can therefore never misreport a batch, only route
// an extra read. Result order is unspecified.
//
// A candidate ID whose batch does not exist is returned as an error rather than
// skipped: batch rows are never deleted, so a dangling record means the store is
// inconsistent, not that the batch concluded.
func ListByStates(ctx context.Context, store storage.Storage, states []entity.BatchState) ([]entity.Batch, error) {
	wanted := make(map[entity.BatchState]bool, len(states))
	seen := make(map[string]bool)
	var ids []string
	for _, state := range states {
		if wanted[state] {
			continue
		}
		wanted[state] = true

		records, err := store.GetQueueBatchStateStore().List(ctx, state)
		if err != nil {
			return nil, fmt.Errorf("failed to list queue batch state records for state %s: %w", state, err)
		}
		for _, record := range records {
			if seen[record.BatchID] {
				continue
			}
			seen[record.BatchID] = true
			ids = append(ids, record.BatchID)
		}
	}

	hydrated := make([]entity.Batch, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(hydrateConcurrency)
	for i, id := range ids {
		g.Go(func() error {
			batch, err := store.GetBatchStore().Get(gctx, id)
			if err != nil {
				return fmt.Errorf("failed to get batch %s: %w", id, err)
			}
			hydrated[i] = batch
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	var result []entity.Batch
	for _, batch := range hydrated {
		if wanted[batch.State] {
			result = append(result, batch)
		}
	}
	return result, nil
}
