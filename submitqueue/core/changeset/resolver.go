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

package changeset

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/submitqueue/entity"
	storage "github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Stores is the slice of a queue-scoped storage aggregate this package needs.
// Declaring it here rather than naming a service's aggregate keeps `core/`
// free of any dependency on a service package; every aggregate that exposes
// these two accessors satisfies it.
type Stores interface {
	// GetRequestStore returns the queue's RequestStore.
	GetRequestStore() storage.RequestStore

	// GetChangeStore returns the queue's ChangeStore.
	GetChangeStore() storage.ChangeStore
}

// Resolve binds Stores to one queue. The wiring layer supplies it, because
// that is the layer that knows which service's aggregate serves a queue.
type Resolve func(queue string) (Stores, error)

// resolver is the store-backed Resolver. It resolves the batch's queue-scoped
// request and change stores per call, since every resolution is for exactly
// one batch and the batch names its queue.
type resolver struct {
	resolve Resolve
}

// New returns a Resolver that reads through the given per-queue binding.
func New(resolve Resolve) Resolver {
	return resolver{resolve: resolve}
}

// ChangesForBatch resolves a batch's requests to their raw changes, in
// batch.Contains order.
func (r resolver) ChangesForBatch(ctx context.Context, batch entity.Batch) ([]change.Change, error) {
	store, err := r.resolve(batch.Queue)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve storage for queue %q: %w", batch.Queue, err)
	}
	changes := make([]change.Change, 0, len(batch.Contains))
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return nil, fmt.Errorf("failed to get request %s for batch %s: %w", requestID, batch.ID, err)
		}
		changes = append(changes, request.Change)
	}
	return changes, nil
}

// DetailedForBatch resolves a batch into the normalized entity.BatchChanges: one
// ChangeInfo per claimed URI, owned by the requesting request, aggregated across
// the whole batch.
func (r resolver) DetailedForBatch(ctx context.Context, batch entity.Batch) (entity.BatchChanges, error) {
	store, err := r.resolve(batch.Queue)
	if err != nil {
		return entity.BatchChanges{}, fmt.Errorf("failed to resolve storage for queue %q: %w", batch.Queue, err)
	}
	result := entity.BatchChanges{BatchID: batch.ID, Queue: batch.Queue}
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return entity.BatchChanges{}, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		for _, uri := range request.Change.URIs {
			records, err := store.GetChangeStore().GetByURI(ctx, uri)
			if err != nil {
				return entity.BatchChanges{}, fmt.Errorf("failed to read change record for request %s uri=%s: %w", requestID, uri, err)
			}
			for _, rec := range records {
				if rec.RequestID != requestID {
					continue
				}
				result.Changes = append(result.Changes, entity.ChangeInfo{URI: rec.URI, Details: rec.Details})
				break
			}
		}
	}
	return result, nil
}
