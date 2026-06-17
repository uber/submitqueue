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
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// resolver is the store-backed Resolver. It owns the two resolution-target
// stores and nothing else: a request store to walk batch.Contains, and a change
// store to attach provider details for the Detailed view.
type resolver struct {
	requests storage.RequestStore
	changes  storage.ChangeStore
}

// New returns a Resolver backed by the given request and change stores.
func New(requests storage.RequestStore, changes storage.ChangeStore) Resolver {
	return resolver{requests: requests, changes: changes}
}

// ChangesForBatch resolves a batch's requests to their raw changes, in
// batch.Contains order.
func (r resolver) ChangesForBatch(ctx context.Context, batch entity.Batch) ([]change.Change, error) {
	changes := make([]change.Change, 0, len(batch.Contains))
	for _, requestID := range batch.Contains {
		request, err := r.requests.Get(ctx, requestID)
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
	result := entity.BatchChanges{BatchID: batch.ID, Queue: batch.Queue}
	for _, requestID := range batch.Contains {
		request, err := r.requests.Get(ctx, requestID)
		if err != nil {
			return entity.BatchChanges{}, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		for _, uri := range request.Change.URIs {
			records, err := r.changes.GetByURI(ctx, batch.Queue, uri)
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
