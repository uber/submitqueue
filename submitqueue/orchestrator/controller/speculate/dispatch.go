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

package speculate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// dispatch saves what finalize left over and hands the build stage its work.
// Everything still outstanding — build results, broken-path cancellations and
// accepted proposals — is folded together per head, so a head costs one
// compare-and-swap however many of its paths changed.
//
// It walks every in-flight batch, not only the speculating ones. Proposals
// apply to speculating heads alone and are simply absent for the rest, but
// observations are not: a merging or cancelling head's paths keep holding CI
// slots until their builds stop, and this is the only writer that can record
// that they have. Batches already finalized arrive here clean —
// commitOutcome persisted their set with their outcome — so only their
// dispatch is left to do.
func (c *Controller) dispatch(ctx context.Context, queue string, snap snapshot, kept []entity.Speculation) error {
	nowMs := time.Now().UnixMilli()

	// Group the accepted proposals by head so each head is written once.
	byHead := make(map[string][]entity.Speculation, len(kept))
	for _, proposal := range kept {
		byHead[proposal.Path.Head] = append(byHead[proposal.Path.Head], proposal)
	}

	for _, batch := range snap.inFlight {
		// A head with no stored set is one nothing has been funded for yet. It
		// gets an empty set to fold this run's proposals into, which persist
		// then creates; a head that ends the run with no paths writes nothing
		// at all, because nothing marked it changed.
		set, exists := snap.pathSets[batch.ID]
		if !exists {
			set = entity.SpeculationPathSet{Queue: queue, Head: batch.ID}
		}

		changed := snap.isDirty(batch.ID)
		for _, proposal := range byHead[batch.ID] {
			if applyProposal(&set, proposal, nowMs) {
				changed = true
			}
		}

		if changed {
			if _, err := c.persist(ctx, snap.store, set, exists); err != nil {
				if errors.Is(err, storage.ErrVersionMismatch) {
					// Skipped rather than failed, and nothing is lost by that.
					//
					// The only other writer of a path set is another speculate
					// run on this same queue — overlap happens when a message
					// is redelivered or a partition moves between consumers.
					// Whichever run wins has read the same queue and reached
					// the same kind of conclusion, and its own dispatch step
					// dispatches and publishes for this head, so the work
					// happens either way. Failing the message instead would
					// replay every other head's decisions to retry this one
					// against a snapshot that is now stale anyway; any later
					// signal on the queue re-plans this head from stored
					// state.
					metrics.NamedCounter(c.metricsScope, opName, "path_set_cas_lost", 1)
					c.logger.Infow("lost a path set write; the next run re-plans this head",
						"batch_id", batch.ID,
						"queue", queue,
					)
					continue
				}
				return err
			}
		}

		// Dispatch whenever the head has paths waiting to start, whether or not
		// this run changed them: a pending path whose earlier dispatch was lost
		// is re-sent until the build stage records it building or terminal.
		// Cancelling paths need no dispatch at all — the poll loop reads the
		// stop off the set and enacts it. Partitioned by batch, so heads
		// dispatch in parallel while one head's dispatches stay ordered.
		if hasActionablePaths(set) {
			if err := c.publishBatchID(ctx, topickey.TopicKeyBuild, batch.ID, queue, batch.ID); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
				return fmt.Errorf("failed to publish batch %s to build: %w", batch.ID, err)
			}
		}
	}

	return nil
}

// persist writes a head's path set, creating it if this run is the first to
// fund the head. It returns the set as stored, with its version advanced, so
// a caller that keeps the set around goes on holding a current copy.
func (c *Controller) persist(ctx context.Context, store storage.Storage, set entity.SpeculationPathSet, exists bool) (entity.SpeculationPathSet, error) {
	pathSets := store.GetSpeculationPathSetStore()

	if !exists {
		set.Version = 1
		if err := pathSets.Create(ctx, set); err != nil {
			if errors.Is(err, storage.ErrAlreadyExists) {
				// Another writer created it between this run's read and now.
				// Treat it as a lost race: the next run reads the winner.
				return set, storage.ErrVersionMismatch
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return set, fmt.Errorf("failed to create path set for batch %s: %w", set.Head, err)
		}
		return set, nil
	}

	newVersion := set.Version + 1
	if err := pathSets.Update(ctx, set, set.Version, newVersion); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			return set, err
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return set, fmt.Errorf("failed to update path set for batch %s: %w", set.Head, err)
	}
	set.Version = newVersion
	return set, nil
}

// applyProposal folds one accepted proposal into the set and reports whether
// it changed anything. The return value says only whether the stored set
// needs rewriting, not whether to dispatch: the build stage is driven by the
// statuses in the set, so a path this returns false for is still dispatched
// if its status calls for it.
//
// The status transitions here are the ones drawn in the package doc's
// lifecycle diagram; the two rules worth calling out:
//
//   - A build proposal for a path already in flight is a no-op, not a new
//     attempt. The Allocator matches candidates to in-flight paths by ID
//     precisely so a path keeps the slot it holds; re-funding it would start
//     a second build for work already running.
//   - A build proposal for a *terminal* path resurrects it: same path (its
//     identity is its assumptions), new attempt. The attempt counter moves
//     and the status returns to pending — the one backwards step in the
//     lifecycle. The build link is keyed by (path, attempt), so the previous
//     attempt's build stays addressable rather than being overwritten.
func applyProposal(set *entity.SpeculationPathSet, proposal entity.Speculation, nowMs int64) bool {
	pathID := proposal.Path.ID()

	for i := range set.Paths {
		entry := &set.Paths[i]
		if entry.ID != pathID {
			continue
		}

		switch proposal.Action {
		case entity.PathActionCancel:
			if entry.Status == entity.SpeculationPathStatusCancelling {
				return false
			}
			// Recording the intent is the whole enactment from this side: the
			// poll loop reads the status off the set on every poll and asks
			// the runner to stop the build. What ends the cancellation is CI
			// actually stopping and a later run recording that on the path.
			entry.Status = entity.SpeculationPathStatusCancelling
			entry.UpdatedAtMs = nowMs
			return true

		case entity.PathActionBuild:
			if !entry.Status.IsTerminal() {
				return false
			}
			entry.Status = entity.SpeculationPathStatusPending
			entry.Attempt++
			entry.UpdatedAtMs = nowMs
			return true
		}
		return false
	}

	// A cancel can only apply to a path already in the set; filterProposals
	// has already rejected the ones that are not.
	if proposal.Action != entity.PathActionBuild {
		return false
	}

	set.Paths = append(set.Paths, entity.SpeculationPathEntry{
		ID:          pathID,
		Path:        proposal.Path,
		Status:      entity.SpeculationPathStatusPending,
		Attempt:     1,
		Version:     1,
		CreatedAtMs: nowMs,
		UpdatedAtMs: nowMs,
	})
	return true
}

// hasActionablePaths reports whether a head has work for the build stage: a
// path waiting to start. Cancelling paths are not the build stage's work —
// the poll loop stops their builds — so they trigger no dispatch.
func hasActionablePaths(set entity.SpeculationPathSet) bool {
	for _, entry := range set.Paths {
		if entry.Status == entity.SpeculationPathStatusPending {
			return true
		}
	}
	return false
}
