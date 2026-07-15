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

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/speculation"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/dependencylimit"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/enumerator"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/pathscorer"
	"github.com/uber/submitqueue/submitqueue/extension/speculation/selector"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles speculate queue messages.
//
// Each invocation drives the batch's entity.SpeculationTree forward by
// re-running enumerate → reconcile → score → select → finalize against the
// latest persisted state, and advances the batch one step in the state
// machine. Every step recomputes from scratch, so redelivery after a crash
// or a tree/batch version conflict is always safe: the round is a pure
// function of freshly read state. Tree maintenance is pure mechanics: the
// controller runs the queue's configured seams — enumerator (path
// structure), path scorer (path scores), selector (path decisions) —
// validates their outputs, applies them to the persisted tree, and writes it
// back under optimistic concurrency. Which paths exist, how they score, and
// which are promoted or cancelled are the seams' decisions alone; the
// controller never originates one.
//
// Per invocation, the controller advances the batch (and its tree) one step:
//
//   - Created, Scored, or Speculating → speculateBatch, the unified entry
//     point described below.
//   - Cancelling → cancelBatch: sweep the speculation tree toward
//     cancellation (live builds → Cancelling intent, everything else →
//     Cancelled), route the persisted intents to the build stage via
//     prioritize, and — only once every build has quiesced — respeculate
//     dependents, CAS to terminal Cancelled, publish to conclude.
//   - Merging → superviseMerging: wake dependents (a Merging batch already
//     counts as landed for their merge readiness — see below) and fail the
//     batch if no path can confirm anymore (its base failed before the
//     runway hand-off ever happened).
//   - Terminal → re-publish the dependent fan-out and the conclude event.
//     Every terminal transition is routed back through this controller, so
//     this branch is how waiting dependents learn a dependency resolved;
//     redelivery makes the same branch the self-heal for a lost publish.
//
// speculateBatch:
//
//  1. Fetch the batch's active dependencies.
//  2. Load the batch's speculation tree. If none exists yet, gate on the
//     queue's dependency limit (too many active dependencies waits for a
//     later dependency-terminal event to re-trigger us) and, if admitted,
//     enumerate and persist a fresh tree with every path Candidate.
//  3. Reconcile: match recorded builds onto paths (stamping BuildID and
//     mapping build status to path status, never downgrading a terminal
//     path), then mark paths dead — capturing a running build's cancellation
//     as intent (Cancelling) or dropping a pre-build path straight to
//     Cancelled — when they can no longer merge.
//  4. Re-score every path via the queue's scorer.
//  5. Ask the queue's selector which paths to pursue and apply its
//     decisions (Promote → Selected; Cancel → Cancelling/Cancelled).
//  6. Persist the tree, but only if something actually changed.
//  7. CAS the batch to Speculating (Created/Scored only).
//  8. Publish to the queue-wide prioritize stage — every pass, after the CAS,
//     so a lost publish self-heals on the next event and the round is never
//     observed as Speculating without prioritize knowing about it. speculate
//     does not publish straight to build: a path only reaches build once
//     prioritize admits it under the queue's build budget, so the pipeline is
//     speculate → prioritize → build.
//  9. Finalize: if some path is mergeable now — optimistically, see below —
//     publish to merge, CAS to Merging, and wake dependents. Otherwise, if
//     no path can ever merge, CAS to Failed and publish to conclude.
//     Otherwise wait for the next event.
//
// Two things must both hold before a path merges: its own build Passed, and
// its base is out of the way. The build result is checked here on purpose —
// a failed build should stop the merge directly, not surface later as a
// runway merge failure.
//
// The base check is optimistic: a base that is published for merge
// (Merging) already counts, so a dependent finalizes right behind its base
// instead of waiting a runway round-trip per chain link
// (speculation.PathMergePossible). The safety net lives downstream — the merge
// stage holds the runway hand-off until the dependency outcomes are
// actually confirmed (speculation.PathMergeConfirmed), so out-of-order delivery
// cannot land a dependent whose base never landed; and if a base's merge
// fails, mergesignal drives it terminal, the dependent fan-out re-wakes
// this controller, and superviseMerging fails every dependent whose bet
// died.
//
// No code in this file talks to a build runner. Every decision that stops a
// build — a dead or deselected path during speculateBatch (applySelection
// and reconcile), or the batch-wide sweep during cancelBatch — is captured
// as intent only (SpeculationPathStatusCancelling) and flows through
// prioritize to the build stage, the sole owner of runner interaction. The
// path settles to Cancelled only once its build is observed terminal.
//
// The controller is re-triggered on every relevant downstream event
// (buildsignal, a prioritize tree write, mergesignal, a dependency's own
// speculate pass), so each call simply re-evaluates the current state and
// either advances or waits.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	enumerators   enumerator.Factory
	scorers       pathscorer.Factory
	selectors     selector.Factory
	depLimits     dependencylimit.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// NewController creates a new speculate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	enumerators enumerator.Factory,
	scorers pathscorer.Factory,
	selectors selector.Factory,
	depLimits dependencylimit.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("speculate_controller"),
		metricsScope:  scope.SubScope("speculate_controller"),
		store:         store,
		enumerators:   enumerators,
		scorers:       scorers,
		selectors:     selectors,
		depLimits:     depLimits,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles the batch's speculation tree and advances the batch one
// step in the state machine.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// Cancelling intent: the cancel controller has handed this batch off to
	// speculate to drive to terminal. Each pass sweeps the speculation tree
	// toward cancellation, routes any persisted cancel intents to the build
	// stage via prioritize, and — only once every build has quiesced — fans
	// out to dependents, CASes to terminal Cancelled, and publishes to
	// conclude.
	if batch.State == entity.BatchStateCancelling {
		return c.cancelBatch(ctx, batch)
	}

	// Terminal state: wake dependents and re-publish conclude. This branch is
	// the dependent wake-up for every terminal transition — mergesignal routes
	// merge outcomes back through speculate under the batch's own ID, the
	// finalize pass below re-publishes a batch it fails, and re-publishing the
	// dependents here lets each of them re-run its own dependency gate /
	// finalize pass against the new dependency state. On redelivery the same
	// branch doubles as the crash self-heal: both the dependent fan-out and
	// the conclude publish are idempotent re-sends.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_terminal", 1)
		if err := c.respeculateDependents(ctx, batch); err != nil {
			return err
		}
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	// Merging: the merge stage owns the batch's forward motion; speculate
	// supervises — waking dependents and failing the batch if its
	// optimistic bet died. See superviseMerging.
	if batch.State == entity.BatchStateMerging {
		return c.superviseMerging(ctx, batch)
	}

	switch batch.State {
	case entity.BatchStateCreated, entity.BatchStateScored, entity.BatchStateSpeculating:
		return c.speculateBatch(ctx, batch)
	default:
		metrics.NamedCounter(c.metricsScope, opName, "unexpected_state", 1)
		return fmt.Errorf("unexpected batch state %q for batch %s", batch.State, batch.ID)
	}
}

// speculateBatch is the unified entry point for Created, Scored, and
// Speculating batches. It loads the batch's speculation tree (creating it on
// the first pass the dependency gate admits), reconciles it against build
// outcomes and dead dependencies, applies the scorer's and selector's
// outputs, persists the tree if anything changed, CASes the batch to
// Speculating on its first pass, publishes the queue to prioritize every
// pass, and then finalizes the batch from the tree.
func (c *Controller) speculateBatch(ctx context.Context, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, batch)
	if err != nil {
		return err
	}
	depByID := make(map[string]entity.Batch, len(deps))
	for _, d := range deps {
		depByID[d.ID] = d
	}

	tree, err := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, err)
		}

		// Dependency gate: only consulted before a tree exists. Once a batch
		// has a tree, the tree itself reconciles dependency outcomes on every
		// pass, so the gate no longer applies.
		blocked, gerr := c.dependencyGateBlocks(ctx, batch, deps)
		if gerr != nil {
			return gerr
		}
		if blocked {
			metrics.NamedCounter(c.metricsScope, opName, "dependency_gate_blocked", 1)
			c.logger.Debugw("active dependency count exceeds queue's dependency limit; waiting",
				"batch_id", batch.ID,
				"dependency_count", len(deps),
			)
			return nil
		}

		tree, err = c.createTree(ctx, batch, deps)
		if err != nil {
			return err
		}
	}

	tree, reconChanged, err := c.reconcile(ctx, batch, tree, depByID)
	if err != nil {
		return err
	}

	scr, err := c.scorers.For(pathscorer.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "scorer_errors", 1)
		return fmt.Errorf("failed to get speculation scorer for queue %s: %w", batch.Queue, err)
	}
	scores, err := scr.Score(ctx, tree)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "scorer_errors", 1)
		return fmt.Errorf("failed to score speculation tree for batch %s: %w", batch.ID, err)
	}
	tree, scoresChanged := c.applyScores(batch, tree, scores)

	sel, err := c.selectors.For(selector.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "selector_errors", 1)
		return fmt.Errorf("failed to get selector for queue %s: %w", batch.Queue, err)
	}
	decisions, err := sel.Select(ctx, tree)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "selector_errors", 1)
		return fmt.Errorf("failed to select speculation decisions for batch %s: %w", batch.ID, err)
	}

	tree, selectionChanged := c.applySelection(batch, tree, decisions)

	if reconChanged || scoresChanged || selectionChanged {
		newVersion := tree.Version + 1
		if err := c.store.GetSpeculationTreeStore().Update(ctx, batch.ID, tree.Version, newVersion, tree.Paths); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update speculation tree for batch %s: %w", batch.ID, err)
		}
		tree.Version = newVersion
	}

	// Optimistic CAS: if the version has already advanced (concurrent
	// speculate), the next event will see the new state and behave correctly.
	if batch.State == entity.BatchStateCreated || batch.State == entity.BatchStateScored {
		newVersion := batch.Version + 1
		if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateSpeculating); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update batch %s state to speculating: %w", batch.ID, err)
		}
		batch.Version = newVersion
		batch.State = entity.BatchStateSpeculating
	}

	// Publish to prioritize every pass, after the CAS: publishing before the
	// CAS could let a concurrent prioritize round observe the batch as not
	// yet Speculating and skip it with no later wake-up. Publishing after is
	// healed by redelivery — an error or crash here nacks the message, and
	// the next pass republishes, since this runs on every pass regardless of
	// whether anything changed.
	if err := c.publishQueue(ctx, topickey.TopicKeyPrioritize, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish queue %s to prioritize: %w", batch.Queue, err)
	}

	return c.finalize(ctx, batch, tree, depByID)
}

// dependencyGateBlocks reports whether batch's count of active dependencies
// (entity.DependencyBatchStates) exceeds the queue's current dependency
// limit. It is consulted only before a batch's speculation tree exists.
//
// The gate applies the dependencylimit extension's value; the application —
// counting active dependencies and expressing "wait" as an acked no-op — is
// admission control on the batch's pipeline progress and deliberately stays
// in the controller, out of the structure-only enumerator seam. A blocked
// batch is woken by the next dependency event: every dependency terminal
// transition re-publishes the dependents of the newly terminal batch (see
// the terminal branch in Process), and the active count only shrinks via
// those same transitions, so no unblocking event can be missed; a raised
// limit takes effect at the next such event. This cannot deadlock:
// dependencies point at strictly earlier batches (the graph is a DAG) and a
// batch with no active dependencies is never blocked, so the head of every
// chain keeps progressing and eventually wakes its dependents.
func (c *Controller) dependencyGateBlocks(ctx context.Context, batch entity.Batch, deps []entity.Batch) (bool, error) {
	active := 0
	for _, d := range deps {
		if isActiveDependency(d.State) {
			active++
		}
	}

	limiter, err := c.depLimits.For(dependencylimit.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "dependency_limit_errors", 1)
		return false, fmt.Errorf("failed to get dependency limit for queue %s: %w", batch.Queue, err)
	}
	limit, err := limiter.Limit(ctx)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "dependency_limit_errors", 1)
		return false, fmt.Errorf("failed to get dependency limit value for queue %s: %w", batch.Queue, err)
	}

	return active > limit, nil
}

// isActiveDependency reports whether s is one of the states that makes an
// in-flight batch eligible to be a dependency (entity.DependencyBatchStates).
func isActiveDependency(s entity.BatchState) bool {
	for _, st := range entity.DependencyBatchStates() {
		if st == s {
			return true
		}
	}
	return false
}

// createTree enumerates and persists a batch's speculation tree the first
// time it is seen, with every path stamped Candidate. Concurrent creation
// (two events racing to create the same tree) is resolved by re-reading the
// winner's tree rather than erroring: enumeration is deterministic given the
// same (batchID, deps), so either creator's structure is equivalent and the
// loser simply adopts what won.
func (c *Controller) createTree(ctx context.Context, batch entity.Batch, deps []entity.Batch) (entity.SpeculationTree, error) {
	enumFactory, err := c.enumerators.For(enumerator.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "enumerator_errors", 1)
		return entity.SpeculationTree{}, fmt.Errorf("failed to get enumerator for queue %s: %w", batch.Queue, err)
	}

	paths, err := enumFactory.Enumerate(ctx, batch, deps)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "enumerator_errors", 1)
		return entity.SpeculationTree{}, fmt.Errorf("failed to enumerate speculation paths for batch %s: %w", batch.ID, err)
	}

	// The enumerator returns structure only; the controller owns everything
	// else about a persisted path. Each entry is stamped Candidate and minted
	// its ID here, once, at tree creation — immutable thereafter: it is how
	// scores, decisions, and the path->build mapping (PathBuild.PathID) refer
	// to the path. A structural duplicate from the enumerator is a contract
	// violation; the first occurrence wins and the rest are skipped.
	infos := make([]entity.SpeculationPathInfo, 0, len(paths))
	for _, p := range paths {
		dup := false
		for _, existing := range infos {
			if existing.Path.Equal(p) {
				dup = true
				break
			}
		}
		if dup {
			metrics.NamedCounter(c.metricsScope, opName, "duplicate_enumerated_path", 1)
			c.logger.Warnw("enumerator returned duplicate path; skipped",
				"batch_id", batch.ID,
				"path", p,
			)
			continue
		}
		infos = append(infos, entity.SpeculationPathInfo{
			ID:     fmt.Sprintf("%s/path/%d", batch.ID, len(infos)),
			Path:   p,
			Status: entity.SpeculationPathStatusCandidate,
		})
	}
	tree := entity.SpeculationTree{BatchID: batch.ID, Paths: infos, Version: 1}

	if err := c.store.GetSpeculationTreeStore().Create(ctx, tree); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			metrics.NamedCounter(c.metricsScope, opName, "tree_create_race_lost", 1)
			existing, gerr := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
			if gerr != nil {
				metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
				return entity.SpeculationTree{}, fmt.Errorf("failed to re-get speculation tree for batch %s after concurrent create: %w", batch.ID, gerr)
			}
			return existing, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return entity.SpeculationTree{}, fmt.Errorf("failed to create speculation tree for batch %s: %w", batch.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "tree_created", 1)
	return tree, nil
}

// applyScores merges the scorer's path-ID-keyed scores into tree, enforcing
// the seam contract on consume: a score naming a path not in the tree is
// logged and skipped, and a score outside [0, 1] is clamped into range with a
// warning — a scorer bug must not corrupt ranking inputs downstream. Paths
// the scorer omitted keep their last persisted score. The returned bool
// reports whether any persisted score actually changed, so the caller can
// skip the conditional write on a no-op pass.
func (c *Controller) applyScores(batch entity.Batch, tree entity.SpeculationTree, scores []entity.PathScore) (entity.SpeculationTree, bool) {
	changed := false
	for _, ps := range scores {
		idx := tree.PathIndex(ps.PathID)
		if idx == -1 {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_score", 1)
			c.logger.Warnw("illegal score: path not found in tree",
				"batch_id", batch.ID,
				"path_id", ps.PathID,
			)
			continue
		}
		score := ps.Score
		if score < 0 || score > 1 {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_score", 1)
			c.logger.Warnw("illegal score: outside [0, 1]; clamped",
				"batch_id", batch.ID,
				"path_id", ps.PathID,
				"score", score,
			)
			if score < 0 {
				score = 0
			} else {
				score = 1
			}
		}
		if tree.Paths[idx].Score != score {
			tree.Paths[idx].Score = score
			changed = true
		}
	}
	return tree, changed
}

// reconcile folds each path's build outcome into its status, then marks any
// non-terminal, non-Cancelling path dead if pathDead reports its assumption
// has become invalid.
//
// Dead-path cancellation is captured as intent only, mirroring
// applySelection: a Building path drops to Cancelling with no runner call —
// nothing in this file talks to a build runner. The build stage enacts the
// intent on a later pass, which it reliably gets: speculateBatch publishes to
// prioritize on every pass regardless of whether anything changed, and
// prioritize republishes to build for every batch whose tree still carries a
// Cancelling path, not just ones it touched this round. A path with no build
// yet drops straight to Cancelled — there is nothing running to enact.
//
// The returned bool reports whether any path actually changed, so the caller
// can skip the conditional write on a no-op pass.
//
// Cost: one pass over the tree with at most two point reads (path->build
// mapping, then build) per path that is still unresolved — paths already at
// a terminal status (Passed/Failed/Cancelled) are settled and skipped
// without touching storage. Statuses are folded into the tree's own path
// slice in place, mirroring applyScores/applySelection — no per-pass copy.
// The tree's path count itself is the enumerator's to decide and bound:
// implementations are expected to return several paths per batch, but
// always a bounded frontier — never the 2^N power set of dependency
// subsets (see extension/speculation/enumerator) — so a reconcile pass
// stays O(paths) in reads and CPU, allocating nothing.
func (c *Controller) reconcile(ctx context.Context, batch entity.Batch, tree entity.SpeculationTree, depByID map[string]entity.Batch) (entity.SpeculationTree, bool, error) {
	paths := tree.Paths
	changed := false

	for i, p := range paths {
		if isTerminalPathStatus(p.Status) {
			// Settled outcome: reconcileStatus never downgrades a terminal
			// path and the dead-path sweep below skips them too, so there
			// is nothing to read or fold for this path.
			continue
		}
		pb, err := c.store.GetSpeculationPathBuildStore().Get(ctx, p.ID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return entity.SpeculationTree{}, false, fmt.Errorf("failed to get path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, err)
		}

		b, err := c.store.GetBuildStore().Get(ctx, pb.BuildID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// Invariant breach: the write order in the build controller
				// guarantees a mapping is only created once its build row
				// exists. Treat this path like "no build yet" rather than
				// crashing or nack-looping.
				metrics.NamedCounter(c.metricsScope, opName, "mapping_dangling", 1)
				c.logger.Warnw("path->build mapping points at a missing build; treating path as unbuilt",
					"batch_id", batch.ID,
					"path_id", p.ID,
					"build_id", pb.BuildID,
				)
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return entity.SpeculationTree{}, false, fmt.Errorf("failed to get build %s for path %s of batch %s: %w", pb.BuildID, p.ID, batch.ID, err)
		}
		newStatus := reconcileStatus(p.Status, b.Status)
		if p.BuildID != b.ID || p.Status != newStatus {
			p.BuildID = b.ID
			p.Status = newStatus
			paths[i] = p
			changed = true
		}
	}

	for i, p := range paths {
		if isTerminalPathStatus(p.Status) || p.Status == entity.SpeculationPathStatusCancelling {
			continue
		}
		if !pathDead(p.Path, depByID) {
			continue
		}

		if p.Status == entity.SpeculationPathStatusBuilding {
			p.Status = entity.SpeculationPathStatusCancelling
			metrics.NamedCounter(c.metricsScope, opName, "path_dead_cancelling", 1)
		} else {
			p.Status = entity.SpeculationPathStatusCancelled
			metrics.NamedCounter(c.metricsScope, opName, "path_dead_cancelled", 1)
		}
		paths[i] = p
		changed = true
	}

	tree.Paths = paths
	return tree, changed, nil
}

// reconcileStatus folds a build outcome into a path's status. Pure O(1)
// status arithmetic — no I/O; reconcile calls it once per unresolved path.
//
// Two rules shape it:
//
//   - A terminal path status (Passed/Failed/Cancelled) is a settled outcome
//     and is never downgraded, whatever the build now reports.
//   - Cancelling records a decision about the PATH — it was deselected, or
//     its merge assumptions died — not a prediction about the build's
//     outcome. Once recorded, the path can never merge; the only open
//     question is when the runner side quiesces. So a Cancelling path holds
//     until its build reaches ANY terminal state and then settles to
//     Cancelled — including a build that raced to Succeeded before the
//     cancel intent was enacted: its result is moot for a path that will
//     never merge. (The build stage only calls runner.Cancel on builds that
//     are still running; an already-terminal build is left untouched, and
//     this mapping simply settles the path.)
//
// For a live (non-Cancelling) path, the build outcome maps directly:
// Succeeded -> Passed, Failed -> Failed, Cancelled -> Cancelled,
// Accepted/Running -> Building.
func reconcileStatus(current entity.SpeculationPathStatus, build entity.BuildStatus) entity.SpeculationPathStatus {
	if isTerminalPathStatus(current) {
		return current
	}
	if current == entity.SpeculationPathStatusCancelling {
		if build.IsTerminal() {
			return entity.SpeculationPathStatusCancelled
		}
		return entity.SpeculationPathStatusCancelling
	}
	switch build {
	case entity.BuildStatusSucceeded:
		return entity.SpeculationPathStatusPassed
	case entity.BuildStatusFailed:
		return entity.SpeculationPathStatusFailed
	case entity.BuildStatusCancelled:
		return entity.SpeculationPathStatusCancelled
	case entity.BuildStatusAccepted, entity.BuildStatusRunning:
		return entity.SpeculationPathStatusBuilding
	default:
		return current
	}
}

// isTerminalPathStatus reports whether s is a settled path outcome that must
// never be overwritten by a later reconcile pass.
func isTerminalPathStatus(s entity.SpeculationPathStatus) bool {
	switch s {
	case entity.SpeculationPathStatusPassed, entity.SpeculationPathStatusFailed, entity.SpeculationPathStatusCancelled:
		return true
	default:
		return false
	}
}

// pathDead reports whether path can never merge — its assumption set has
// been invalidated by a dependency outcome:
//
//   - a base dependency reached Failed: the path built on top of changes
//     that will never land, so its build result can never back a merge;
//   - a dependency of the head OUTSIDE the base reached Succeeded: the path
//     bet that batch would not land first, and it did.
//
// A Cancelled base dependency deliberately does NOT dead the path (Phase-A
// leniency): the cancelled batch will never land, so it can no longer
// conflict, and the path's build being stale against the original
// (uncancelled) assumption set is accepted — this mirrors the pre-tree
// chain semantics. With exactly one path per batch today (chain
// enumerator), deading that only path on a benign cancel would fail batches
// that currently survive. Once multi-path enumeration lands and a sibling
// path can pick up the slack, a Cancelled base dependency should dead the
// path like a Failed one — tracked in
// https://github.com/uber/submitqueue/issues/369.
func pathDead(path entity.SpeculationPath, depByID map[string]entity.Batch) bool {
	inBase := make(map[string]bool, len(path.Base))
	for _, id := range path.Base {
		inBase[id] = true
		if d, ok := depByID[id]; ok && d.State == entity.BatchStateFailed {
			return true
		}
	}
	for id, d := range depByID {
		if inBase[id] {
			continue
		}
		if d.State == entity.BatchStateSucceeded {
			return true
		}
	}
	return false
}

// applySelection applies the selector's decisions to tree.Paths, mutating it
// in place. The selector owns the policy (which paths to promote or cancel);
// this function owns only the bookkeeping, mapping each decision onto the
// path's current status. Mirrors prioritize.go's apply loop: Promote clears a
// Candidate to Selected; Cancel is captured as intent only — a Building path
// moves to Cancelling, and any pre-build status (Candidate, Selected, or
// Prioritized) drops straight to Cancelled. Nothing here touches a build
// runner — a Cancelling status records the intent for whichever stage owns
// runner interaction to enact. A decision naming a path not in
// the tree, a duplicate decision for the same path, or an action the path's
// current status does not support is a policy bug in the selector: it is
// logged as a warning and skipped rather than corrupting the tree. The
// returned bool reports whether any path status changed, so the caller can
// skip the conditional write on a no-op pass.
func (c *Controller) applySelection(batch entity.Batch, tree entity.SpeculationTree, decisions []entity.PathDecision) (entity.SpeculationTree, bool) {
	if len(decisions) == 0 {
		return tree, false
	}

	paths := tree.Paths
	changed := false
	seen := make(map[string]bool, len(decisions))
	for _, d := range decisions {
		if seen[d.PathID] {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: duplicate decision for path",
				"batch_id", batch.ID,
				"path_id", d.PathID,
				"action", d.Action,
			)
			continue
		}
		seen[d.PathID] = true

		idx := tree.PathIndex(d.PathID)
		if idx == -1 {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: path not found in tree",
				"batch_id", batch.ID,
				"path_id", d.PathID,
				"action", d.Action,
			)
			continue
		}

		info := paths[idx]
		switch {
		case d.Action == entity.SpeculationPathActionPromote && info.Status == entity.SpeculationPathStatusCandidate:
			info.Status = entity.SpeculationPathStatusSelected
			changed = true
			metrics.NamedCounter(c.metricsScope, opName, "selected", 1)

		case d.Action == entity.SpeculationPathActionCancel && info.Status == entity.SpeculationPathStatusBuilding:
			info.Status = entity.SpeculationPathStatusCancelling
			changed = true
			metrics.NamedCounter(c.metricsScope, opName, "cancelling", 1)

		case d.Action == entity.SpeculationPathActionCancel &&
			(info.Status == entity.SpeculationPathStatusCandidate ||
				info.Status == entity.SpeculationPathStatusSelected ||
				info.Status == entity.SpeculationPathStatusPrioritized):
			info.Status = entity.SpeculationPathStatusCancelled
			changed = true
			metrics.NamedCounter(c.metricsScope, opName, "cancelled", 1)

		default:
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: action not valid for path status",
				"batch_id", batch.ID,
				"path_id", d.PathID,
				"action", d.Action,
				"status", string(info.Status),
			)
			continue
		}

		paths[idx] = info
	}

	tree.Paths = paths
	return tree, changed
}

// finalize settles the batch's forward step from the tree. Exactly one of
// three outcomes applies per pass:
//
//  1. Merge now: some path's merge assumptions hold — optimistically: its
//     own build Passed and its base is landed, cancelled out of the way, or
//     itself published for merge (speculation.PathMergePossible; see the
//     Controller doc) — publish to merge, CAS the batch to Merging, and
//     wake dependents, for whom this batch now counts as landed.
//  2. Wait: no path is mergeable yet, but at least one is still viable —
//     no-op; the next event (a build outcome via buildsignal, a dependency
//     going terminal) re-runs this evaluation.
//  3. Fail: no viable path remains — every path has Failed, been Cancelled,
//     or gone dead with no chance of ever merging — so the batch can never
//     merge. The !anyViable branch below runs the shared Failed sequence
//     (failBatch): CAS to Failed, dependent fan-out, conclude.
func (c *Controller) finalize(ctx context.Context, batch entity.Batch, tree entity.SpeculationTree, depByID map[string]entity.Batch) error {
	for _, p := range tree.Paths {
		if !speculation.PathMergePossible(p, depByID) {
			continue
		}

		if err := c.publish(ctx, topickey.TopicKeyMerge, batch.ID, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish to merge: %w", err)
		}

		newVersion := batch.Version + 1
		if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateMerging); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update batch %s state to merging: %w", batch.ID, err)
		}
		batch.Version = newVersion
		batch.State = entity.BatchStateMerging
		metrics.NamedCounter(c.metricsScope, opName, "merging", 1)

		// Wake dependents now: a Merging batch already counts as landed for
		// their merge readiness. Fan out AFTER the CAS so a woken dependent
		// reads this batch as Merging; a crash in between is healed by
		// superviseMerging, which re-runs the fan-out on every later
		// delivery.
		return c.respeculateDependents(ctx, batch)
	}

	anyViable := false
	failedCount, cancelledCount := 0, 0
	for _, p := range tree.Paths {
		if viable(p, depByID) {
			anyViable = true
		}
		switch p.Status {
		case entity.SpeculationPathStatusFailed:
			failedCount++
		case entity.SpeculationPathStatusCancelled, entity.SpeculationPathStatusCancelling:
			cancelledCount++
		}
	}

	if !anyViable {
		c.logger.Warnw("no viable speculation path remains; failing batch",
			"batch_id", batch.ID,
			"failed_paths", failedCount,
			"cancelled_paths", cancelledCount,
		)
		metrics.NamedCounter(c.metricsScope, opName, "no_viable_path", 1)
		return c.failBatch(ctx, batch)
	}

	metrics.NamedCounter(c.metricsScope, opName, "waiting_on_deps", 1)
	c.logger.Debugw("no path mergeable yet; waiting", "batch_id", batch.ID)
	return nil
}

// failBatch runs the shared terminal-Failed sequence for a batch that can
// never merge: CAS to Failed, wake dependents, publish to conclude. Callers
// log and count their own reason before calling.
//
// The fan-out runs AFTER the CAS and BEFORE concluding, mirroring
// cancelBatch's ordering. It is publish-only — one speculate wake-up per
// dependent, each processed asynchronously on its own delivery; nothing is
// re-evaluated inline here. A dependent only drops this batch from its tree
// (dead chain path) and lets a surviving path take over once it observes
// the terminal Failed state. Nothing else re-notifies them — a batch failed
// here never reaches mergesignal, whose fan-out covers the merge-failure
// flow. A crash after the CAS is recovered by the terminal self-heal
// branch, which re-runs this fan-out for Failed.
func (c *Controller) failBatch(ctx context.Context, batch entity.Batch) error {
	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateFailed); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to failed: %w", batch.ID, err)
	}
	batch.Version = newVersion
	batch.State = entity.BatchStateFailed

	if err := c.respeculateDependents(ctx, batch); err != nil {
		return err
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	return nil
}

// superviseMerging handles a speculate wake for a batch already in Merging.
// The merge stage owns the batch's forward motion; this branch owns two
// things:
//
//   - Wake dependents. A Merging batch already counts as landed for their
//     merge readiness (optimistic merge finalization), and re-publishing
//     the fan-out on every delivery self-heals one lost to a crash right
//     after the Merging CAS, exactly like the terminal branch.
//   - Fail the batch if its optimistic bet died. A batch may finalize while
//     a base dependency is itself still Merging; the merge stage only hands
//     off to runway once some path's assumptions are confirmed. If the base
//     failed instead, no hand-off will ever happen and nothing else would
//     move this batch again — so when no path is possible anymore, run the
//     shared Failed sequence. This never races a runway verdict: hand-off
//     requires confirmed assumptions, confirmed outcomes are terminal
//     dependency states that cannot un-happen, and possible ⊇ confirmed —
//     so a handed-off batch always takes the wake branch here.
func (c *Controller) superviseMerging(ctx context.Context, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, batch)
	if err != nil {
		return err
	}
	depByID := make(map[string]entity.Batch, len(deps))
	for _, d := range deps {
		depByID[d.ID] = d
	}

	// A Merging batch always has a tree — it finalized from one, and trees
	// are never deleted — so a miss is corrupted state: the error rejects
	// the message and the DLQ reconciler fails the batch loudly.
	tree, err := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, err)
	}

	for _, p := range tree.Paths {
		if speculation.PathMergePossible(p, depByID) {
			metrics.NamedCounter(c.metricsScope, opName, "merging_fanout", 1)
			// Re-arm the merge trigger before waking dependents. The merge
			// stage's delayed re-check cycle is the only live message for a
			// waiting Merging batch; if that chain is ever lost, nothing
			// else would re-create it, so every healthy supervision wake
			// re-arms it. Duplicated triggers converge: a confirmed batch
			// hands off idempotently (runway dedups on the correlation id)
			// and halted batches ack the trigger away.
			if err := c.republishMergeTrigger(ctx, batch); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
				return fmt.Errorf("failed to re-arm merge trigger for batch %s: %w", batch.ID, err)
			}
			return c.respeculateDependents(ctx, batch)
		}
	}

	metrics.NamedCounter(c.metricsScope, opName, "merge_bet_refuted", 1)
	c.logger.Warnw("no path can confirm for merging batch; failing batch",
		"batch_id", batch.ID,
	)
	return c.failBatch(ctx, batch)
}

// viable reports whether p could still merge in the future: it has not
// reached a non-succeeding terminal or cancelling status, and it is not
// dead. A batch with no viable path left can never merge and is failed.
func viable(p entity.SpeculationPathInfo, depByID map[string]entity.Batch) bool {
	switch p.Status {
	case entity.SpeculationPathStatusFailed, entity.SpeculationPathStatusCancelled, entity.SpeculationPathStatusCancelling:
		return false
	}
	return !pathDead(p.Path, depByID)
}

// cancelBatch drives a batch from BatchStateCancelling to BatchStateCancelled.
// The cancel controller records the user's intent (Cancelling) and hands the
// batch off; speculate owns the rest because all the work that must precede
// the terminal write — settling the speculation tree, respeculating
// dependents — already lives in the speculate domain. The terminal
// transition is the single writer of every non-Cancelling batch state across
// the system.
//
// Like speculateBatch, this is a reconcile pass, not a one-shot — it never
// talks to a build runner. Each invocation sweeps the tree (cancelTree),
// dropping paths with nothing running straight to Cancelled and capturing a
// live build's stop as intent (Cancelling). While any path is still
// Cancelling the batch stays in BatchStateCancelling: the pass publishes the
// queue to prioritize — which routes the persisted intents to the build
// stage, the sole owner of runner interaction — and acks. Each buildsignal
// tick for a winding-down build re-publishes this batch to speculate, so the
// pass re-runs until every build has been observed terminal and no
// Cancelling path remains. Even if a runner cancel is never enacted (lost
// message, runner refusing), the sweep still converges once the build
// finishes on its own.
//
// Only once nothing is pending does the terminal sequence run, in an order
// that matters:
//
//  1. CAS the batch to terminal Cancelled. This must happen BEFORE the
//     dependent fan-out: finalize only drops a Cancelled dep from the chain,
//     so dependents woken with the dep still in Cancelling would wait
//     pending and never get pinged again.
//
//  2. Re-publish each downstream dependent to speculate so they can drop
//     this cancelled batch from their chain and advance (or finalize, if
//     this was their last outstanding dep).
//
//  3. Publish to conclude so contained requests reach RequestStateCancelled.
//
// A crash between steps 1 and 2/3 is recovered on redelivery via the
// terminal self-heal branch, which re-runs the dependent fan-out and the
// conclude publish for already-Cancelled batches.
//
// storage.ErrVersionMismatch on the terminal CAS is returned as-is, like
// every storage error here — retryability is the consumer's classifier
// walk's decision. A retried delivery lands in the self-heal branch and
// completes the fan-out.
func (c *Controller) cancelBatch(ctx context.Context, batch entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "cancel_batch", 1)
	c.logger.Debugw("cancel sweep",
		"batch_id", batch.ID,
		"queue", batch.Queue,
	)

	// TODO(respeculate-collateral): re-enqueue Land for every request in batch.Contains
	// except the user-cancelled request. Today the whole batch dies (per spec) and the
	// collateral requests need a fresh request ID and a re-publish to TopicKeyStart so
	// they can be re-batched without the cancelled change.

	pending, err := c.cancelTree(ctx, batch)
	if err != nil {
		return err
	}

	if pending > 0 {
		// Builds are still winding down. The Cancelling intents are already
		// persisted; hand them to the build stage through the queue-wide
		// prioritize stage and wait for the next buildsignal wake to
		// re-evaluate.
		metrics.NamedCounter(c.metricsScope, opName, "cancel_pending_builds", 1)
		c.logger.Debugw("waiting for builds to quiesce before terminal cancel",
			"batch_id", batch.ID,
			"pending_paths", pending,
		)
		if err := c.publishQueue(ctx, topickey.TopicKeyPrioritize, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish queue %s to prioritize: %w", batch.Queue, err)
		}
		return nil
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateCancelled); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to cancelled: %w", batch.ID, err)
	}
	batch.Version = newVersion
	batch.State = entity.BatchStateCancelled
	c.logger.Infow("batch cancelled",
		"batch_id", batch.ID,
		"queue", batch.Queue,
	)

	if err := c.respeculateDependents(ctx, batch); err != nil {
		return err
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}

	return nil
}

// cancelTree sweeps the batch's speculation tree toward cancellation and
// reports how many paths remain Cancelling — that is, whose builds have not
// yet been observed terminal. Terminal paths are settled outcomes and are
// skipped without I/O on ordinary passes (a pass about to settle re-checks
// Cancelled paths once — see the terminal-transition guard below). Every
// other path is resolved through the path->build mapping keyed by its
// assigned ID (SpeculationPathInfo.ID) — the same two-hop lookup as
// reconcile, with the same tolerance for a path with no mapping yet and the
// same defensive handling of a mapping whose build row is missing: in both
// cases nothing is running, so the path drops straight to Cancelled. A path
// whose build is already terminal likewise settles to Cancelled; a path
// whose build is still in flight is marked Cancelling — intent only, enacted
// by the build stage. A missing tree (the batch was cancelled before it was
// ever speculated on) is benign: nothing to sweep, nothing pending. The
// sweep is persisted under the tree's version lock only when a status
// actually moved, and costs one pass with at most two point reads per
// unresolved path, mirroring reconcile.
func (c *Controller) cancelTree(ctx context.Context, batch entity.Batch) (int, error) {
	tree, err := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "cancel_tree_not_found", 1)
			return 0, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return 0, fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, err)
	}

	paths := tree.Paths
	changed := false
	pending := 0
	// resolved marks paths whose build state this pass already read, so the
	// terminal-transition guard below never re-reads a path settled by this
	// very pass.
	resolved := make([]bool, len(paths))
	for i, p := range paths {
		if isTerminalPathStatus(p.Status) {
			continue
		}
		resolved[i] = true

		status, err := c.cancelPathStatus(ctx, batch, p)
		if err != nil {
			return 0, err
		}
		if status == entity.SpeculationPathStatusCancelling {
			pending++
		}
		if p.Status != status {
			paths[i].Status = status
			changed = true
		}
	}

	// Terminal-transition guard: a path can sit at Cancelled while its build
	// is still live — a pre-build Cancel (selection or prioritize) can land
	// in the window after the build stage triggered a build but before any
	// reconcile stamped it onto the tree. Letting the batch settle then
	// would strand that build forever: a terminal batch stops the
	// buildsignal loop, so the orphan's stop would never be recorded. So on
	// a pass that would otherwise settle, resolve pre-existing Cancelled
	// paths' mappings too and pull any that still has a live build back to
	// Cancelling for the build stage to enact. Only Cancelled can hide a
	// live build — Passed and Failed are derived from terminal builds — and
	// this is the one deliberate downgrade of a terminal path status in the
	// system, costing extra point reads only on would-be-final passes.
	if pending == 0 {
		for i, p := range paths {
			if resolved[i] || p.Status != entity.SpeculationPathStatusCancelled {
				continue
			}
			status, err := c.cancelPathStatus(ctx, batch, p)
			if err != nil {
				return 0, err
			}
			if status != entity.SpeculationPathStatusCancelling {
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "orphan_build_cancelling", 1)
			c.logger.Warnw("cancelled path still has a live build; re-opening cancel intent",
				"batch_id", batch.ID,
				"path_id", p.ID,
			)
			paths[i].Status = entity.SpeculationPathStatusCancelling
			pending++
			changed = true
		}
	}

	if !changed {
		return pending, nil
	}

	newVersion := tree.Version + 1
	if err := c.store.GetSpeculationTreeStore().Update(ctx, batch.ID, tree.Version, newVersion, paths); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return 0, fmt.Errorf("failed to update speculation tree for batch %s: %w", batch.ID, err)
	}
	metrics.NamedCounter(c.metricsScope, opName, "cancel_tree_updated", 1)
	return pending, nil
}

// cancelPathStatus resolves the cancellation status for one non-terminal
// path: Cancelling while its build is still in flight, Cancelled when there
// is no build (no mapping yet, or a dangling mapping) or the build already
// reached a terminal state.
func (c *Controller) cancelPathStatus(ctx context.Context, batch entity.Batch, p entity.SpeculationPathInfo) (entity.SpeculationPathStatus, error) {
	pb, err := c.store.GetSpeculationPathBuildStore().Get(ctx, p.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return entity.SpeculationPathStatusCancelled, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return entity.SpeculationPathStatusUnknown, fmt.Errorf("failed to get path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, err)
	}

	b, err := c.store.GetBuildStore().Get(ctx, pb.BuildID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Invariant breach: see reconcile's identical handling.
			metrics.NamedCounter(c.metricsScope, opName, "mapping_dangling", 1)
			c.logger.Warnw("path->build mapping points at a missing build; treating path as unbuilt",
				"batch_id", batch.ID,
				"path_id", p.ID,
				"build_id", pb.BuildID,
			)
			return entity.SpeculationPathStatusCancelled, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return entity.SpeculationPathStatusUnknown, fmt.Errorf("failed to get build %s for path %s of batch %s: %w", pb.BuildID, p.ID, batch.ID, err)
	}

	if b.Status.IsTerminal() {
		return entity.SpeculationPathStatusCancelled, nil
	}
	return entity.SpeculationPathStatusCancelling, nil
}

// republishMergeTrigger re-arms the batch's merge trigger with a freshly
// minted message ID. The minted ID matters: the queue dedups publishes on
// (topic, partition, id) against un-GC'd rows including consumed ones, so
// re-publishing under the batch ID — the ID finalize's original trigger
// used — could be silently swallowed and heal nothing.
func (c *Controller) republishMergeTrigger(ctx context.Context, batch entity.Batch) error {
	payload, err := entity.BatchID{ID: batch.ID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msgID := fmt.Sprintf("%s/mergeheal/%d", batch.ID, time.Now().UnixMilli())
	msg := entityqueue.NewMessage(msgID, payload, batch.Queue, nil)

	q, ok := c.registry.Queue(topickey.TopicKeyMerge)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyMerge)
	}
	topicName, ok := c.registry.TopicName(topickey.TopicKeyMerge)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyMerge)
	}
	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// respeculateDependents publishes a speculate event for every batch that
// depends on the given batch. The batch controller creates a BatchDependent
// row (with Dependents possibly empty) for every batch it persists, so a
// missing row at this point is a storage invariant violation, not a normal
// "no dependents" case — surface ErrNotFound as a regular storage error so
// the message nacks and either an operator or the batch controller's own
// crash-recovery can resolve the inconsistency.
//
// Called from the cancelBatch terminal flow, from finalize's no-viable-path
// Failed flow, and from the terminal branch in Process — the latter on both
// the first pass after a batch goes terminal (the normal dependent wake-up)
// and on redelivery (self-heal).
func (c *Controller) respeculateDependents(ctx context.Context, batch entity.Batch) error {
	bd, err := c.store.GetBatchDependentStore().Get(ctx, batch.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch dependents for batch %s: %w", batch.ID, err)
	}

	for _, depID := range bd.Dependents {
		// Alternative: process each dependent inline (load batch, run the
		// equivalent of speculateBatch) instead of publishing back to
		// ourselves. Rejected for now: per-message retry isolation, fresh
		// per-dependent reads, consumer-pool parallelism / backpressure, and
		// the existing state-machine dispatch in Process all argue for the
		// publish. Revisit if the extra message hop ever shows up as latency
		// or cost.
		if err := c.publish(ctx, topickey.TopicKeySpeculate, depID, batch.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish dependent batch %s to speculate: %w", depID, err)
		}
		metrics.NamedCounter(c.metricsScope, opName, "dependent_respeculated", 1)
	}
	return nil
}

// fetchDependencies loads each batch in batch.Dependencies. Any storage error
// is surfaced as a retryable infra failure; missing dependencies should not
// happen in practice, but if one does it is treated the same as a transient
// fetch failure (i.e. the message is retried).
func (c *Controller) fetchDependencies(ctx context.Context, batch entity.Batch) ([]entity.Batch, error) {
	deps := make([]entity.Batch, 0, len(batch.Dependencies))
	for _, depID := range batch.Dependencies {
		d, err := c.store.GetBatchStore().Get(ctx, depID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "dependency_fetch_errors", 1)
			return nil, fmt.Errorf("failed to get dependency batch %s of %s: %w", depID, batch.ID, err)
		}
		deps = append(deps, d)
	}
	return deps, nil
}

// fanout re-publishes downstream events for a batch that has already reached
// a terminal state. Used for self-healing when a previous publish was lost:
// re-sending to conclude guarantees request-state reconciliation.
func (c *Controller) fanout(ctx context.Context, batchID, partitionKey string) error {
	if err := c.publish(ctx, topickey.TopicKeyConclude, batchID, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	return nil
}

// publish publishes a batch ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
	bid := entity.BatchID{ID: batchID}
	payload, err := bid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(batchID, payload, partitionKey, nil)

	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}

	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// publishQueue publishes a queue name to the specified topic key. Used for
// queue-scoped stages (prioritize) whose message payload carries no batch
// identity at all — just the queue to re-evaluate.
func (c *Controller) publishQueue(ctx context.Context, key consumer.TopicKey, queue string) error {
	qid := entity.QueueID{Name: queue}
	payload, err := qid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize queue ID: %w", err)
	}

	msg := entityqueue.NewMessage(queue, payload, queue, nil)

	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}

	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "speculate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
