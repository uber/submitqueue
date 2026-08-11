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
	"maps"
	"slices"

	"github.com/uber/submitqueue/platform/metrics"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// run re-plans a whole queue from a single read of its state, in the five
// steps the package doc lays out: read, finalize, ask, check, dispatch.
//
// The batch on the triggering message only says which queue woke up; nothing
// about the plan depends on which batch it was, or on any earlier run. Its
// identity is carried through only for crash recovery — see snapshot.trigger.
func (c *Controller) run(ctx context.Context, store storage.Storage, trigger entity.Batch) error {
	snap, err := c.read(ctx, store, trigger.Queue)
	if err != nil {
		return err
	}
	snap.trigger = trigger.ID

	if err := c.finalize(ctx, &snap); err != nil {
		return err
	}
	if len(snap.speculating) == 0 {
		// No head is open to new work, so there is nothing to ask the
		// Speculator. The dispatch step still runs: what the build stages saw about a
		// merging or cancelling head's paths has to be persisted so those
		// paths stop counting against the budget.
		return c.dispatch(ctx, trigger.Queue, snap, nil)
	}

	proposals, err := c.ask(ctx, trigger.Queue, snap)
	if err != nil {
		return err
	}

	kept, rejected := filterProposals(proposals, snap)
	for _, reason := range rejected {
		metrics.NamedCounter(c.metricsScope, opName, "speculation_rejected", 1,
			metrics.NewTag("reason", string(reason)))
		c.logger.Warnw("dropped a speculator proposal",
			"queue", trigger.Queue,
			"reason", string(reason),
		)
	}

	return c.dispatch(ctx, trigger.Queue, snap, kept)
}

// read builds the run's snapshot. Batches come first because their dependency
// lists say which finalized batches still have to be resolved, and their IDs
// say which path sets to load.
func (c *Controller) read(ctx context.Context, store storage.Storage, queue string) (snapshot, error) {
	inFlight, err := corebatch.ListByStates(ctx, store, entity.ActiveBatchStates())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return snapshot{}, c.attributed(fmt.Errorf("failed to list in-flight batches of queue %s: %w", queue, err),
			entity.QueueSubject(queue))
	}

	snap := snapshot{
		store:    store,
		batches:  make(map[string]entity.Batch, len(inFlight)),
		inFlight: inFlight,
		pathSets: make(map[string]entity.SpeculationPathSet, len(inFlight)),
		dirty:    make(map[string]bool, len(inFlight)),
	}

	for _, batch := range inFlight {
		snap.batches[batch.ID] = batch
		if batch.State == entity.BatchStateSpeculating {
			snap.speculating = append(snap.speculating, batch)
		}
	}

	// Also load dependencies that already finished: they are no longer in the
	// in-flight list, but their final states are what decide whether a path's
	// assumptions still hold.
	for _, batch := range inFlight {
		for _, depID := range batch.Dependencies {
			if _, ok := snap.batches[depID]; ok {
				continue
			}
			dep, err := store.GetBatchStore().Get(ctx, depID)
			if err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
				return snapshot{}, c.attributed(fmt.Errorf("failed to get dependency batch %s of %s: %w", depID, batch.ID, err),
					entity.BatchSubject(depID))
			}
			snap.batches[depID] = dep
		}
	}

	for _, batch := range inFlight {
		set, err := store.GetSpeculationPathSetStore().Get(ctx, batch.ID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// Nothing has been funded for this head yet.
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return snapshot{}, c.attributed(fmt.Errorf("failed to get path set for batch %s: %w", batch.ID, err),
				entity.BatchSubject(batch.ID))
		}
		changed, err := c.updatePathsFromBuilds(ctx, store, &set)
		if err != nil {
			return snapshot{}, err
		}
		snap.pathSets[batch.ID] = set
		if changed {
			snap.markDirty(batch.ID)
		}
	}

	return snap, nil
}

// updatePathsFromBuilds updates each live path's status in the in-memory set
// from what its build actually did: a build found running turns a pending
// path into building, and a finished build writes its result (passed, failed,
// cancelled) onto the path.
//
// Paths already in a final status are skipped — their result was written to
// the set by an earlier run — and the live ones are capped by the build
// budget, so this costs a bounded handful of point reads per run, not one per
// path.
//
// The update is applied in memory only. It is persisted by whichever write
// the run makes later, which keeps this controller the path set's single
// writer: the build stages record what CI did on per-build records, and the
// run alone decides when that becomes the path's status.
func (c *Controller) updatePathsFromBuilds(ctx context.Context, store storage.Storage, set *entity.SpeculationPathSet) (bool, error) {
	changed := false
	for i := range set.Paths {
		entry := &set.Paths[i]
		if entry.Status.IsTerminal() {
			continue
		}

		link, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
		if errors.Is(err, storage.ErrNotFound) { //nolint:gocritic // reads better than a switch here
			// No build is recorded for this attempt. Usually nothing was ever
			// dispatched; at worst a dispatch is mid-flight, and a build that
			// materializes after this read still gets its signal, so the poll
			// loop finds it and stops it if its path no longer wants it.
			//
			// A cancelling path therefore has nothing this run must wait for,
			// and is marked cancelled here; nothing else would ever finish the
			// job, and left cancelling it would hold a budget slot forever. The
			// slot may be released a few seconds before a mid-flight build
			// actually stops — a transient budget overshoot on a build already
			// being killed. A pending path keeps the run's own intent: its
			// dispatch is re-sent rather than abandoned.
			if entry.Status == entity.SpeculationPathStatusCancelling {
				entry.Status = entity.SpeculationPathStatusCancelled
				changed = true
				metrics.NamedCounter(c.metricsScope, opName, "path_cancelled_undispatched", 1)
			}
			continue
		}
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return false, c.attributed(fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err),
				entity.BatchSubject(set.Head))
		}

		build, err := store.GetBuildStore().Get(ctx, link.BuildID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// Unreachable in a consistent store: the dispatch writes the
				// build before the link, so a link always has one. Treated as
				// not started rather than fatal, because the alternative is
				// wedging a whole queue's run on one corrupt row.
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return false, c.attributed(fmt.Errorf("failed to get build %s for path %s: %w", link.BuildID, entry.ID, err),
				entity.BatchSubject(set.Head))
		}

		if !build.Status.IsTerminal() {
			// The build is with the runner, so a pending path is now building.
			// Recording that is what stops the dispatch: hasActionablePaths
			// counts pending as work to send, so a path left pending would be
			// re-dispatched on every run for a build that is already running.
			//
			// A cancelling path stays cancelling. The stop is recorded in the
			// set and the poll loop enacts it against the runner; the path keeps
			// that status until a later run sees its build actually stopped, so
			// a build still running must not overwrite the intent.
			if entry.Status == entity.SpeculationPathStatusPending {
				entry.Status = entity.SpeculationPathStatusBuilding
				changed = true
			}
			continue
		}

		result := terminalPathStatus(build.Status)
		entry.Status = result
		changed = true
		metrics.NamedCounter(c.metricsScope, opName, "path_outcome_recorded", 1,
			metrics.NewTag("status", string(result)))
	}
	return changed, nil
}

// terminalPathStatus maps a finished build onto the path status that records
// its outcome. Callers check the build is terminal first, so the default is
// unreachable; it returns the zero value there because that value means "no
// status", not "still running".
func terminalPathStatus(status entity.BuildStatus) entity.SpeculationPathStatus {
	switch status {
	case entity.BuildStatusSucceeded:
		return entity.SpeculationPathStatusPassed
	case entity.BuildStatusFailed:
		return entity.SpeculationPathStatusFailed
	case entity.BuildStatusCancelled:
		return entity.SpeculationPathStatusCancelled
	default:
		return entity.SpeculationPathStatusUnknown
	}
}

// ask hands the snapshot to the queue's Speculator. Its answer is a proposal,
// not an instruction: check decides what is actually enacted.
//
// Both arguments carry the whole queue, whatever state each batch is in. A
// head's dependencies are the facts its paths are built from, so a dependency
// withheld is one the Speculator has to plan around blind. Every in-flight
// path set goes over for the same reason: a path holds its CI slot until its
// build actually stops, so a merging head's superseded siblings and a
// cancelling head's live builds spend the budget just like a speculating
// head's do. Hiding either would let the allocator count occupied slots as
// free and oversubscribe CI.
//
// Passing the full queue cannot widen what gets proposed: a path ID hashes its
// head, and check rejects any proposal aimed at a head that is not
// speculating. A head this run has just decided still reads as Speculating
// here, but it can only rebuild the path that already passed — see
// mergeablePath — which the allocator skips as finished.
func (c *Controller) ask(ctx context.Context, queue string, snap snapshot) ([]entity.Speculation, error) {
	spec, err := c.speculators.For(speculator.Config{QueueName: queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "speculator_errors", 1)
		return nil, c.attributed(fmt.Errorf("failed to build speculator for queue %s: %w", queue, err),
			entity.QueueSubject(queue))
	}

	sets := make([]entity.SpeculationPathSet, 0, len(snap.pathSets))
	for _, batch := range snap.inFlight {
		if set, ok := snap.pathSets[batch.ID]; ok {
			sets = append(sets, set)
		}
	}

	proposals, err := spec.Speculate(ctx, slices.Collect(maps.Values(snap.batches)), sets)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "speculator_errors", 1)
		return nil, c.attributed(fmt.Errorf("speculator failed for queue %s: %w", queue, err),
			entity.QueueSubject(queue))
	}
	return proposals, nil
}
