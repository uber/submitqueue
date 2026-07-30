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
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/speculator"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// run re-plans a whole queue from a single read of its state, in the five
// steps the package doc lays out: read, cancel broken paths, ask, check,
// dispatch.
//
// The batch on the triggering message only says which queue woke up; nothing
// about the run depends on which batch it was, or on any earlier run. That is
// what makes duplicated, delayed, and reordered signals harmless, and what
// lets a later run repair anything an earlier one left half-done.
//
// Cancelling broken paths before asking is what keeps the Speculator's work
// from being wasted: it reasons over the queue as the facts have already left
// it, rather than over a picture this run is about to invalidate.
func (c *Controller) run(ctx context.Context, store storage.Storage, queue string) error {
	snap, err := c.read(ctx, store, queue)
	if err != nil {
		return err
	}
	if len(snap.speculating) == 0 {
		// No head is open to new work, so there is nothing to speculate about.
		return nil
	}

	c.cancelBrokenPaths(&snap)

	proposals, err := c.ask(ctx, queue, snap)
	if err != nil {
		return err
	}

	kept, rejected := filterProposals(proposals, snap)
	for _, reason := range rejected {
		metrics.NamedCounter(c.metricsScope, opName, "speculation_rejected", 1,
			metrics.NewTag("reason", string(reason)))
		c.logger.Warnw("dropped a speculator proposal",
			"queue", queue,
			"reason", string(reason),
		)
	}

	return c.dispatch(ctx, store, queue, snap, kept)
}

// read builds the run's snapshot. Batches come first because their dependency
// lists say which finalized batches still have to be resolved, and their IDs
// say which path sets to load.
func (c *Controller) read(ctx context.Context, store storage.Storage, queue string) (snapshot, error) {
	inFlight, err := corebatch.ListByStates(ctx, store, entity.ActiveBatchStates())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return snapshot{}, fmt.Errorf("failed to list in-flight batches of queue %s: %w", queue, err)
	}

	snap := snapshot{
		batches:  make(map[string]entity.Batch, len(inFlight)),
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
				return snapshot{}, fmt.Errorf("failed to get dependency batch %s of %s: %w", depID, batch.ID, err)
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
			return snapshot{}, fmt.Errorf("failed to get path set for batch %s: %w", batch.ID, err)
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
			// A pending path keeps the run's own intent: its dispatch is
			// re-sent rather than abandoned.
			continue
		}
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return false, fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
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
			return false, fmt.Errorf("failed to get build %s for path %s: %w", link.BuildID, entry.ID, err)
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

// cancelBrokenPaths marks cancelling, across every head in the snapshot, each
// path with a broken assumption. Such a path can never merge its head, so this
// is a fact, not a choice — and folding it in before the Speculator is asked
// keeps it from proposing work on top of paths that are already dead, which
// check would only throw away.
func (c *Controller) cancelBrokenPaths(snap *snapshot) {
	nowMs := time.Now().UnixMilli()

	for _, batch := range snap.speculating {
		set, exists := snap.pathSets[batch.ID]
		if !exists {
			continue
		}
		if cancelBrokenPathsInSet(&set, *snap, nowMs) {
			snap.pathSets[batch.ID] = set
			snap.markDirty(batch.ID)
		}
	}
}

// cancelBrokenPathsInSet marks cancelling every live path in one set with a
// broken assumption, and reports whether anything changed. Cancelling rather
// than cancelled, because the path's build may still be occupying CI — only
// the signal that sees it stop can call it terminal.
func cancelBrokenPathsInSet(set *entity.SpeculationPathSet, snap snapshot, nowMs int64) bool {
	changed := false
	for i := range set.Paths {
		entry := &set.Paths[i]
		if entry.Status.IsTerminal() || entry.Status == entity.SpeculationPathStatusCancelling {
			continue
		}
		if !assumptionBroken(entry.Path, snap) {
			continue
		}
		entry.Status = entity.SpeculationPathStatusCancelling
		entry.UpdatedAtMs = nowMs
		changed = true
	}
	return changed
}

// ask hands the snapshot to the queue's Speculator. Its answer is a proposal,
// not an instruction: check decides what is actually enacted.
func (c *Controller) ask(ctx context.Context, queue string, snap snapshot) ([]entity.Speculation, error) {
	spec, err := c.speculators.For(speculator.Config{QueueName: queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "speculator_errors", 1)
		return nil, fmt.Errorf("failed to build speculator for queue %s: %w", queue, err)
	}

	sets := make([]entity.SpeculationPathSet, 0, len(snap.pathSets))
	for _, batch := range snap.speculating {
		if set, ok := snap.pathSets[batch.ID]; ok {
			sets = append(sets, set)
		}
	}

	proposals, err := spec.Speculate(ctx, snap.speculating, sets)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "speculator_errors", 1)
		return nil, fmt.Errorf("speculator failed for queue %s: %w", queue, err)
	}
	return proposals, nil
}
