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

package storage

//go:generate mockgen -source=queue_batch_state_store.go -destination=mock/queue_batch_state_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// QueueBatchStateStore manages the per-queue membership records that file each in-queue
// batch under a lifecycle state bucket, so "the batches of queue Q filed under state S"
// is a single read keyed by (queue, state) — a primary-key prefix on any backend, with
// no secondary index or server-side filtering required.
//
// Records are advisory: the authoritative state is on the Batch entity, and a record may
// transiently file a batch under a bucket it has already left. Readers therefore treat a
// listing as a set of candidate batch IDs — they load each Batch by key and classify it
// by its own State, never by the bucket the record was found in.
//
// The interface is intentionally per-state and per-record so that any backend (SQL,
// DynamoDB, Bigtable, …) can implement it without multi-key queries or batch atomicity.
// Callers loop over the states they care about; a batch moves buckets via Put of the new
// record followed by Delete of the old one, which keeps at least one record visible
// throughout. All writes are idempotent so queue redeliveries can safely repeat them.
type QueueBatchStateStore interface {
	// List returns every record filed under the bound queue's given state bucket.
	// An empty slice means the bucket is empty. Order is unspecified.
	List(ctx context.Context, state entity.BatchState) ([]entity.QueueBatchState, error)

	// Put persists a record. The record's Queue must match the instance's bound
	// queue. Writing an already-existing (queue, state, batchID) record is a no-op
	// success, so the call is idempotent under redeliveries.
	Put(ctx context.Context, record entity.QueueBatchState) error

	// Delete removes the bound queue's record identified by (state, batchID).
	// Deleting an absent record is a no-op success, so the call is idempotent
	// under redeliveries.
	Delete(ctx context.Context, state entity.BatchState, batchID string) error
}
