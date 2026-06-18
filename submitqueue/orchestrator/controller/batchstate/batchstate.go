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

// Package batchstate keeps batch-state membership maintenance in the
// orchestrator layer. Storage owns primitive records; this package owns the app
// semantics for creating batches, transitioning states, and resolving
// queue/state membership into authoritative Batch entities.
package batchstate

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// NonTerminalStates are the batch states that should remain discoverable via
// membership lookups.
var NonTerminalStates = []entity.BatchState{
	entity.BatchStateCreated,
	entity.BatchStateScored,
	entity.BatchStateSpeculating,
	entity.BatchStateMerging,
	entity.BatchStateCancelling,
}

// ConflictStates are the active states the batch controller considers when
// building conflict dependencies for a new batch.
var ConflictStates = []entity.BatchState{
	entity.BatchStateCreated,
	entity.BatchStateSpeculating,
	entity.BatchStateMerging,
}

// Create records the initial non-terminal membership before creating the batch
// row, so a successfully persisted batch is not hidden from queue/state reads.
func Create(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	if !batch.State.IsTerminal() {
		if err := store.GetBatchStateMembershipStore().Add(ctx, batch.Queue, batch.State, batch.ID); err != nil {
			return fmt.Errorf("failed to add initial batch state membership for batch %s: %w", batch.ID, err)
		}
	}
	if err := store.GetBatchStore().Create(ctx, batch); err != nil {
		return err
	}
	return nil
}

// UpdateState transitions a batch and maintains queue/state membership in the
// safe direction: add the target non-terminal state before the CAS, then
// best-effort remove the previous non-terminal state after the CAS succeeds.
func UpdateState(ctx context.Context, store storage.Storage, batch entity.Batch, newVersion int32, newState entity.BatchState) error {
	if !newState.IsTerminal() {
		if err := store.GetBatchStateMembershipStore().Add(ctx, batch.Queue, newState, batch.ID); err != nil {
			return fmt.Errorf("failed to add batch state membership for batch %s state %s: %w", batch.ID, newState, err)
		}
	}
	if err := store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, newState); err != nil {
		return err
	}
	removePrevious(ctx, store, batch, newState)
	return nil
}

// UpdateScoreAndState is the score-writing variant of UpdateState.
func UpdateScoreAndState(ctx context.Context, store storage.Storage, batch entity.Batch, newVersion int32, score float64, newState entity.BatchState) error {
	if !newState.IsTerminal() {
		if err := store.GetBatchStateMembershipStore().Add(ctx, batch.Queue, newState, batch.ID); err != nil {
			return fmt.Errorf("failed to add batch state membership for batch %s state %s: %w", batch.ID, newState, err)
		}
	}
	if err := store.GetBatchStore().UpdateScoreAndState(ctx, batch.ID, batch.Version, newVersion, score, newState); err != nil {
		return err
	}
	removePrevious(ctx, store, batch, newState)
	return nil
}

// List returns batches in queue whose current authoritative state is one of
// states. Membership rows are only hints: missing batch rows are skipped, stale
// rows are filtered, and terminal stale rows are best-effort removed.
func List(ctx context.Context, store storage.Storage, queue string, states ...entity.BatchState) ([]entity.Batch, error) {
	if len(states) == 0 {
		return nil, nil
	}

	wanted := make(map[entity.BatchState]struct{}, len(states))
	for _, state := range states {
		wanted[state] = struct{}{}
	}

	seen := make(map[string]struct{})
	results := make([]entity.Batch, 0)
	for _, state := range states {
		ids, err := store.GetBatchStateMembershipStore().ListIDs(ctx, queue, state)
		if err != nil {
			return nil, fmt.Errorf("failed to list batch IDs for queue=%s state=%s: %w", queue, state, err)
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			batch, err := store.GetBatchStore().Get(ctx, id)
			if err != nil {
				if storage.IsNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("failed to get batch id=%s from queue=%s state=%s membership: %w", id, queue, state, err)
			}
			if batch.Queue != queue {
				continue
			}
			if batch.State.IsTerminal() {
				_ = store.GetBatchStateMembershipStore().Remove(ctx, queue, state, id)
				continue
			}
			if _, ok := wanted[batch.State]; !ok {
				continue
			}
			seen[id] = struct{}{}
			results = append(results, batch)
		}
	}
	return results, nil
}

func removePrevious(ctx context.Context, store storage.Storage, batch entity.Batch, newState entity.BatchState) {
	if batch.State.IsTerminal() || batch.State == newState {
		return
	}
	_ = store.GetBatchStateMembershipStore().Remove(ctx, batch.Queue, batch.State, batch.ID)
}
