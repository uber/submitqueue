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
	"github.com/uber/submitqueue/platform/publish"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// finalize reaches and enacts every outcome this run can conclude, and leaves
// snap.speculating holding the heads still open to new work.
//
// Everything here is a fact, not a choice: a path a resolved dependency ruled
// out is dead, a head whose passed build's assumptions all came true
// merges, and a batch the user cancelled is finished once its last build
// stops. Finalizing before the Speculator is asked is what keeps its work from
// being wasted — asked first, it would propose builds for a head that is
// already merging.
//
// Outcomes cascade, and the head loop runs to a fixed point to collapse a
// whole cascade into this one run:
//
//	A fails ──breaks──► B's last live path [A succeeds] ──► B fails
//	       ──breaks──► C's [A succeeds, ...] paths       ──► maybe C too
//
// One pass would only catch dependents that happen to come after their
// prerequisite in queue order — and that order is not even specified —
// leaving the rest to wait for an unrelated later signal. The loop terminates
// because every iteration but the last finalizes at least one head.
//
// Each generation is committed before the next is derived from it, because an
// outcome must be durable before anything is allowed to depend on it.
// Deciding the whole cascade up front and writing afterwards would enact
// dependents of an outcome whose own write then lost its compare-and-swap —
// and the loser of that race is not always benign: a cancellation loses
// precisely to a merge that got there first, which leaves the batch
// *succeeded*, after its dependents were already failed on the assumption it
// was cancelled. Committing per generation costs no extra reads — the
// snapshot is read once, and the writes are ones this run makes anyway.
func (c *Controller) finalize(ctx context.Context, snap *snapshot) error {
	nowMs := time.Now().UnixMilli()

	// Cancellations first: a cancelled batch is a resolved dependency like any
	// other, and the heads below have to see it as one.
	if err := c.finalizeCancellations(ctx, snap, nowMs); err != nil {
		return err
	}

	open := snap.speculating
	for {
		decided := 0
		var stillOpen []entity.Batch

		for _, batch := range open {
			// Fold in what the facts already imply about this head's paths:
			// a finished dependency breaks every path that bet against it.
			set, exists := snap.pathSets[batch.ID]
			before, hadPassed := passedEntry(set)
			if exists && cancelBrokenPathsInSet(&set, *snap, nowMs) {
				snap.pathSets[batch.ID] = set
				snap.markDirty(batch.ID)
			}

			if err := c.reportSpeculation(ctx, batch, set, *snap, before, hadPassed); err != nil {
				return err
			}

			decision := decide(batch, set, *snap)
			if decision == outcomeWait {
				stillOpen = append(stillOpen, batch)
				continue
			}

			if decision == outcomeMerge {
				// The winning path carries the head out of the queue; its
				// siblings cannot help it any more and are still holding CI
				// slots the rest of the queue could use.
				winner, _ := mergeablePath(set, *snap)
				if supersede(&set, winner.ID, nowMs) {
					snap.pathSets[batch.ID] = set
					snap.markDirty(batch.ID)
				}
			}

			committed, err := c.commitOutcome(ctx, snap, batch, decision)
			if err != nil {
				return err
			}
			if !committed {
				// Another writer owns this batch now, so our view of it is
				// stale. Dropped rather than kept open: nothing may be derived
				// from an outcome that did not land, and the Speculator must
				// not be offered a head whose set we could not write.
				continue
			}
			c.recordOutcome(snap, batch.ID, decision)
			decided++
		}

		open = stillOpen
		if decided == 0 {
			break
		}
	}
	snap.speculating = open

	return nil
}

// reportSpeculation tells a head's members how far speculation has got.
//
// Two moments are worth reporting and neither is a batch state — a head is
// BatchStateSpeculating from admission until its outcome, so the request log is
// the only place either becomes visible:
//
//   - the head has a live passed path. Its own work is done and what remains is
//     other batches finishing, a wait that can run for minutes and reads very
//     differently to still building.
//   - it just lost the one it had, because a dependency resolved against that
//     path's guess. The head is back to building, and without this its members
//     would go on reading as speculated through the whole rebuild.
//
// The second is why before is taken from passedEntry rather than livePassedPath:
// both that predicate and the fold above exclude a contradicted path, so a pair
// of livePassedPath calls could never see the loss happen. What is compared is
// "held a passed build" before the fold against "still has one worth waiting on"
// after it, and only the run that does the breaking sees the difference — every
// later run finds the entry already cancelled.
//
// Both facts are derived from the snapshot rather than stored, so this runs on
// every pass over an open head and relies on the occurrence to collapse the
// repeats: a path ID hashes its head along with its assumptions, so it names the
// batch too, and one passed path re-observed by a hundred runs is a single entry
// while a different path winning after a re-plan is correctly a new one.
func (c *Controller) reportSpeculation(
	ctx context.Context,
	batch entity.Batch,
	set entity.SpeculationPathSet,
	snap snapshot,
	before entity.SpeculationPathEntry,
	hadPassed bool,
) error {
	after, hasPassed := livePassedPath(set, snap)

	status, path := entity.RequestStatusSpeculated, after
	switch {
	case hasPassed:
	case hadPassed:
		status, path = entity.RequestStatusSpeculating, before
	default:
		return nil
	}

	if err := corerequest.PublishBatchLogs(ctx, c.registry, batch.Queue, batch.Contains,
		status, path.ID, map[string]string{
			"batch_id": batch.ID,
			"path_id":  path.ID,
		},
	); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		// Attributed to this head, not the trigger: the loop walks the whole
		// queue, so the batch whose members could not be told is usually not the
		// one the message named.
		return c.attributed(fmt.Errorf("failed to publish request logs for batch %s: %w", batch.ID, err),
			entity.BatchSubject(batch.ID))
	}
	return nil
}

// finalizeCancellations marks every path of a cancelling batch stopped and,
// once they all have, drives the batch to cancelled.
//
// This is the whole of the cancel hand-off: the cancel controller writes the
// user's intent and publishes the batch here, and from then on a cancelling
// batch is just another batch the run walks. A batch is not cancelled the
// moment it is asked to be — its builds hold their CI slots until they
// actually stop — so the run marks the paths, the poll loop asks the runner
// to stop them, and whichever later run sees them stopped finishes the
// job. That is why cancellation is best effort, and why a merge that wins the
// race still prevails.
func (c *Controller) finalizeCancellations(ctx context.Context, snap *snapshot, nowMs int64) error {
	// TODO(respeculate-collateral): re-enqueue Land for every request in batch.Contains
	// except the user-cancelled request. Today the whole batch dies (per spec) and the
	// collateral requests need a fresh request ID and a re-publish to TopicKeyStart so
	// they can be re-batched without the cancelled change.
	for _, batch := range snap.inFlight {
		if batch.State != entity.BatchStateCancelling {
			continue
		}
		metrics.NamedCounter(c.metricsScope, opName, "cancel_batch", 1)

		set, exists := snap.pathSets[batch.ID]
		if exists && cancelAllPaths(&set, nowMs) {
			snap.pathSets[batch.ID] = set
			snap.markDirty(batch.ID)
		}

		// An absent set means nothing was ever funded, so there is nothing to
		// wait for. Statuses here already reflect what the build stages saw —
		// read folded that in — so a path still reading as unstopped really is.
		if exists && !allPathsStopped(set) {
			metrics.NamedCounter(c.metricsScope, opName, "cancel_awaiting_paths", 1)
			c.logger.Infow("cancelling batch; waiting for its builds to stop",
				"batch_id", batch.ID,
				"queue", batch.Queue,
			)
			continue
		}

		committed, err := c.commitOutcome(ctx, snap, batch, outcomeCancel)
		if err != nil {
			return err
		}
		if committed {
			c.recordOutcome(snap, batch.ID, outcomeCancel)
		}
	}
	return nil
}

// commitOutcome persists a decided batch's path changes and enacts its
// outcome, reporting whether the outcome actually landed. False means another
// writer moved the head on, and nothing else in this run may be derived from
// this outcome.
//
// The two writes belong together because the outcome is only meaningful with
// the paths it was read from. A lost path-set compare-and-swap means the
// outcome decided from our copy may no longer be right, so the state write is
// not attempted.
func (c *Controller) commitOutcome(ctx context.Context, snap *snapshot, batch entity.Batch, decision outcome) (bool, error) {
	if snap.isDirty(batch.ID) {
		set, exists := snap.pathSets[batch.ID]
		stored, err := c.persist(ctx, snap.store, set, exists)
		if err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				metrics.NamedCounter(c.metricsScope, opName, "path_set_cas_lost", 1)
				c.logger.Infow("lost a path set write; the next run re-plans this head",
					"batch_id", batch.ID,
					"queue", batch.Queue,
				)
				// Abandon the stale copy so write does not retry it.
				snap.markClean(batch.ID)
				return false, nil
			}
			return false, c.attributed(err, entity.BatchSubject(batch.ID))
		}
		snap.pathSets[batch.ID] = stored
		snap.markClean(batch.ID)
	}

	// Attributed here rather than at each leaf: this is the one place that
	// knows which batch the whole commit is for, and the finalize loop runs it
	// over every decided batch in the queue — rarely the one on the message.
	landed, err := c.applyOutcome(ctx, snap.store, batch, decision, snap.isTrigger(batch.ID))
	if err != nil {
		return landed, c.attributed(err, entity.BatchSubject(batch.ID))
	}
	return landed, nil
}

// recordOutcome writes an outcome's terminal state back into the snapshot so
// the rest of the run reasons from it, exactly as it would from an outcome
// recorded before the run started. It is called only once that state is
// durable — see commitOutcome — because everything concluded about the
// batches stacked on this one is derived from it.
//
// Only a terminal outcome is recorded. Merging is not terminal — a head
// stacked on this one assumed it would *succeed*, and it has not yet — so a
// merge outcome resolves nothing for anybody else.
func (c *Controller) recordOutcome(snap *snapshot, batchID string, decision outcome) {
	state, terminal := decision.terminalState()
	if !terminal {
		return
	}
	batch := snap.batches[batchID]
	batch.State = state
	snap.batches[batchID] = batch
}

// applyOutcome enacts a decided outcome on a batch, reporting whether the
// state write landed.
//
// The publish order differs per arm, but it is one rule read twice: a publish
// may precede a state write only when the consumer does not read the state
// that write produces. The merge stage correlates on the batch ID alone, so
// telling it before the write is safe — a batch recorded Merging that Runway
// never heard about would merely stall. Conclude does read the state (it
// reconciles requests from it and rejects a non-terminal batch outright), so
// it is published only after the write, or it would race the consumer into
// the dead-letter queue.
//
// Losing the state compare-and-swap is not an error: another writer got
// there, and the next run reads whatever they wrote. It is reported as not
// landed, because the outcome this run reached is not the one that took
// effect.
func (c *Controller) applyOutcome(ctx context.Context, store storage.Storage, batch entity.Batch, decision outcome, isTriggerBatch bool) (bool, error) {
	var state entity.BatchState
	terminal := false

	switch decision {
	case outcomeMerge:
		state = entity.BatchStateMerging
		// A batch merges once, so the dispatch names only that as its cause: a
		// redelivery that re-derives outcomeMerge because the state write was
		// lost dedups against the request already sent, instead of asking
		// Runway to merge the same batch twice.
		if err := c.publishBatchID(ctx, topickey.TopicKeyMerge, publish.IntentID(batch.ID, "merge-dispatch"), batch.ID, batch.Queue, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return false, fmt.Errorf("failed to publish batch %s to merge: %w", batch.ID, err)
		}

	case outcomeFail, outcomeCancel:
		state, terminal = decision.terminalState()
		// A batch decided by a cascade is not the one on the message, so no
		// retry or dead letter would ever come back to it — give it a recovery
		// message of its own before it turns terminal.
		if !isTriggerBatch {
			if err := c.recoverable(ctx, store, batch); err != nil {
				return false, err
			}
		}

	default:
		// outcomeWait: nothing to enact. Listed explicitly so an unknown or
		// zero outcome can never fall into an enacting arm.
		return false, nil
	}

	// Through Transition, so the queue's membership record moves with the
	// state. A raw CAS would leave the batch filed under the bucket it just
	// left, and since records are only ever added, every later run of this
	// queue would go on listing and hydrating it.
	if _, err := corebatch.Transition(ctx, store, batch, state); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			metrics.NamedCounter(c.metricsScope, opName, "outcome_cas_lost", 1)
			return false, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return false, err
	}

	metrics.NamedCounter(c.metricsScope, opName, "outcome", 1, metrics.NewTag("outcome", string(decision)))
	c.logger.Infow("batch outcome",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"outcome", string(decision),
		"state", string(state),
	)

	if terminal {
		// Named for the run that decided it, so a redelivery re-deriving the
		// same outcome does not conclude the batch twice, and so it stays
		// distinct from the conclude mergesignal sends for a merged batch.
		// A conclude that goes missing is recovered by fanout, which is
		// deliberately un-deduplicated.
		if err := c.publishBatchID(ctx, topickey.TopicKeyConclude, publish.IntentID(batch.ID, "conclude", "speculate"), batch.ID, batch.Queue, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return true, fmt.Errorf("failed to publish batch %s to conclude: %w", batch.ID, err)
		}
	}
	return true, nil
}

// recoverable gives a batch a message of its own before this run makes it
// terminal, so its fan-out cannot be stranded by a failure afterwards.
//
// Every other terminal batch is repaired through the message that names it: a
// redelivery finds it terminal and re-publishes from Process's self-heal
// branch, and a persistent failure lands it in the dead-letter queue by name.
// A batch decided by a cascade has neither — it is not the batch on the
// message, and once terminal it is gone from the queue listing — so without
// this its requests would simply stay unreconciled.
//
// Distinct per publish: the guarantee being bought is that a message exists at
// all, and a stable ID would let the queue answer "one already did" with a
// success that writes nothing.
//
// Published before the state write, not after: sent afterwards it is one more
// thing that can fail exactly when everything else is failing, leaving the
// obligation created and the means to discharge it gone. Sent first, a
// failure means nothing was written at all and the retry re-derives the whole
// decision from unchanged state. A duplicate is harmless — Speculate
// tolerates any batch state, and if the write never lands the message is just
// a nudge that re-plans a queue nothing has changed.
func (c *Controller) recoverable(ctx context.Context, store storage.Storage, batch entity.Batch) error {
	if err := c.publishBatchID(ctx, topickey.TopicKeySpeculate, publish.UniqueID(batch.ID), batch.ID, batch.Queue, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish recovery signal for batch %s: %w", batch.ID, err)
	}
	metrics.NamedCounter(c.metricsScope, opName, "recovery_signal_published", 1)
	return nil
}

// markCancelling asks every live path in the set to stop — except the ones
// keep returns true for — and reports whether anything changed. A live path
// is one neither terminal nor already cancelling.
//
// Cancelling is intent, not fact: the build may still be occupying CI, and
// only an observation of it actually stopping (or of nothing ever having been
// dispatched) moves the path on to cancelled. cancelBrokenPathsInSet,
// supersede, and cancelAllPaths are the three reasons to ask.
func markCancelling(set *entity.SpeculationPathSet, nowMs int64, keep func(entity.SpeculationPathEntry) bool) bool {
	changed := false
	for i := range set.Paths {
		entry := &set.Paths[i]
		if entry.Status.IsTerminal() || entry.Status == entity.SpeculationPathStatusCancelling {
			continue
		}
		if keep(*entry) {
			continue
		}
		entry.Status = entity.SpeculationPathStatusCancelling
		entry.UpdatedAtMs = nowMs
		changed = true
	}
	return changed
}

// cancelBrokenPathsInSet stops every in-flight path with a broken assumption.
// Those guesses can no longer come true, so their builds are only spending
// budget.
func cancelBrokenPathsInSet(set *entity.SpeculationPathSet, snap snapshot, nowMs int64) bool {
	return markCancelling(set, nowMs, func(entry entity.SpeculationPathEntry) bool {
		return !assumptionBroken(entry.Path, snap)
	})
}

// supersede stops every path other than the winner. Once one path has passed,
// its siblings cannot help the head any more — but they are still holding CI
// slots the rest of the queue could use.
func supersede(set *entity.SpeculationPathSet, winnerID string, nowMs int64) bool {
	return markCancelling(set, nowMs, func(entry entity.SpeculationPathEntry) bool {
		return entry.ID == winnerID
	})
}

// cancelAllPaths stops every path that is still running. Used when the head
// itself was cancelled by the user, so no path can help it any more.
func cancelAllPaths(set *entity.SpeculationPathSet, nowMs int64) bool {
	return markCancelling(set, nowMs, func(entity.SpeculationPathEntry) bool {
		return false
	})
}

// allPathsStopped reports whether none of the head's paths still holds a
// build. A cancelling path has not stopped: its build occupies CI until an
// observation records it terminal.
func allPathsStopped(set entity.SpeculationPathSet) bool {
	for _, entry := range set.Paths {
		if !entry.Status.IsTerminal() {
			return false
		}
	}
	return true
}
