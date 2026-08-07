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

// Package batch provides the shared primitives for moving a batch through its
// lifecycle states while keeping the queue's per-state membership records
// (entity.QueueBatchState) in step.
//
// The records are advisory and the Batch entity is authoritative, so the
// primitives follow one protocol:
//
//   - A transition CASes the batch first, then files the record under the new
//     state before removing the one under the old state, so a batch always has
//     at least one record while it is in the queue.
//   - A crash between the CAS and the record move is repaired by the pipeline's
//     at-least-once redelivery: the retry's "already in target state" branch
//     calls EnsureRecord, and every record write is idempotent.
//   - Readers treat records as candidate batch IDs only: they hydrate each
//     batch by key and classify it by its own State, never by the bucket the
//     record was found in, so a stale record can misplace a batch but never
//     misreport it.
package batch

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Transition moves a batch to newState: it performs the optimistic-locking CAS on
// the batch (newVersion = Version+1, assigned in memory only after the store write
// succeeds), then re-files the queue's membership record — Put under newState first,
// Delete under the prior state after, so the batch is never without a record. The
// Delete is skipped when the state is unchanged. It returns the batch as last
// successfully written.
//
// A storage.ErrVersionMismatch from the CAS is returned wrapped (errors.Is works),
// with no record writes attempted, so callers keep their existing lost-race
// semantics. Any other non-nil error means the transition may have partially
// applied — the CAS may have committed with the record move incomplete — and the
// caller is expected to let redelivery retry; the retry's already-in-target-state
// branch repairs the record via EnsureRecord.
func Transition(ctx context.Context, store storage.Storage, batch entity.Batch, newState entity.BatchState) (entity.Batch, error) {
	oldState := batch.State
	newVersion := batch.Version + 1
	updated := batch
	updated.State = newState
	if err := store.GetBatchStore().Update(ctx, updated, batch.Version, newVersion); err != nil {
		return batch, fmt.Errorf("failed to update batch %s state to %s: %w", batch.ID, newState, err)
	}
	updated.Version = newVersion

	record := entity.QueueBatchState{Queue: updated.Queue, State: newState, BatchID: updated.ID}
	if err := store.GetQueueBatchStateStore().Put(ctx, record); err != nil {
		return updated, fmt.Errorf("failed to put queue batch state record for batch %s under state %s: %w", updated.ID, newState, err)
	}
	if oldState != newState {
		if err := store.GetQueueBatchStateStore().Delete(ctx, oldState, updated.ID); err != nil {
			return updated, fmt.Errorf("failed to delete queue batch state record for batch %s under state %s: %w", updated.ID, oldState, err)
		}
	}
	return updated, nil
}

// EnsureRecord idempotently files the batch under its current state bucket. It is
// the repair half of the transition protocol: idempotent redelivery branches that
// skip the CAS because the batch is already in the target state call this instead,
// covering a prior attempt that crashed between the CAS and the record move.
func EnsureRecord(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	record := entity.QueueBatchState{Queue: batch.Queue, State: batch.State, BatchID: batch.ID}
	if err := store.GetQueueBatchStateStore().Put(ctx, record); err != nil {
		return fmt.Errorf("failed to put queue batch state record for batch %s under state %s: %w", batch.ID, batch.State, err)
	}
	return nil
}
