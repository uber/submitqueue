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

// Package batchchanges assembles the normalized, batch-level view of a batch's
// changes (entity.BatchChanges) from storage. A Batch references only request
// IDs, so resolving the actual change facts requires reading each request and
// its per-URI change records. Centralizing this here keeps that storage
// traversal out of the extensions (scorer, conflict analyzer), which consume
// entity.BatchChanges and must not touch storage themselves.
package batchchanges

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Collect assembles the normalized entity.BatchChanges for a batch by resolving
// each request and reading its change records per URI. For each URI it selects
// the record owned by the request (GetByURI returns rows for every request that
// ever claimed the URI) and appends its URI + provider-supplied details.
func Collect(ctx context.Context, store storage.Storage, batch entity.Batch) (entity.BatchChanges, error) {
	changes := entity.BatchChanges{BatchID: batch.ID, Queue: batch.Queue}
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return entity.BatchChanges{}, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		for _, uri := range request.Change.URIs {
			records, err := store.GetChangeStore().GetByURI(ctx, batch.Queue, uri)
			if err != nil {
				return entity.BatchChanges{}, fmt.Errorf("failed to read change record for request %s uri=%s: %w", requestID, uri, err)
			}
			for _, rec := range records {
				if rec.RequestID != requestID {
					continue
				}
				changes.Changes = append(changes.Changes, entity.ChangeInfo{URI: rec.URI, Details: rec.Details})
				break
			}
		}
	}
	return changes, nil
}
