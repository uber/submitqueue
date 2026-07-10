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

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
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
// Each invocation reconciles the batch's entity.SpeculationTree and advances
// the batch one step in the state machine. Tree reconciliation is pure
// mechanics: the controller runs the queue's configured seams — enumerator
// (path structure), path scorer (path scores), selector (path decisions) —
// validates their outputs, applies them to the persisted tree, and writes it
// back under optimistic concurrency. Which paths exist, how they score, and
// which are promoted or cancelled are the seams' decisions alone; the
// controller never originates one. No downstream stage reads the tree yet,
// so the forward step below is driven by the batch's own state, not the
// tree.
//
// Per invocation, the controller advances the batch one step in the state
// machine:
//
//   - Created, Scored, or Speculating → speculateBatch: reconcile the tree
//     (created on the first pass the dependency gate admits), then advance
//     the batch — publish to build and CAS to Speculating for
//     Created/Scored, or tryFinalize for Speculating.
//   - Cancelling        → cancel any in-flight Build entity, respeculate
//     dependents, CAS to terminal Cancelled, publish to conclude. The
//     cancel controller hands the batch off in this state and speculate
//     drives it to terminal.
//   - Merging           → no-op (owned by the merge controller).
//   - Terminal          → re-publish the dependent fan-out and the conclude
//     event. Every terminal transition is routed back through this controller
//     (by mergesignal, or by the cancel flow), so this branch is how waiting
//     dependents learn a dependency resolved; redelivery makes the same
//     branch the self-heal for a lost publish.
//
// Cancel decisions are recorded as path status only
// (SpeculationPathStatusCancelling on an in-flight build) — nothing in this
// file calls a build runner.
//
// The controller is re-triggered on every relevant downstream event
// (buildsignal, merge), so each call simply re-evaluates the current
// state and either advances or waits.
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
	// speculate to drive to terminal. Cancel in-flight builds, fan out to
	// dependents, CAS to terminal Cancelled, and publish to conclude.
	if batch.State == entity.BatchStateCancelling {
		return c.cancelBatch(ctx, batch)
	}

	// Terminal state: wake dependents and re-publish conclude. This branch is
	// the dependent wake-up for success and failure — mergesignal routes every
	// terminal transition back through speculate under the batch's own ID, and
	// re-publishing the dependents here lets each of them re-run its own
	// dependency gate / tryFinalize against the new dependency state. On
	// redelivery the same branch doubles as the crash self-heal: both the
	// dependent fan-out and the conclude publish are idempotent re-sends.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "self_heal_terminal", 1)
		if err := c.respeculateDependents(ctx, batch); err != nil {
			return err
		}
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	// Merging is owned by the merge controller, which has its own self-heal.
	if batch.State == entity.BatchStateMerging {
		metrics.NamedCounter(c.metricsScope, opName, "noop_merging", 1)
		return nil
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
// the first pass the dependency gate admits), applies the scorer's and
// selector's outputs, persists the tree if anything changed, and then
// advances the batch itself: Created/Scored publish to build and CAS to
// Speculating; Speculating runs tryFinalize.
func (c *Controller) speculateBatch(ctx context.Context, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, batch)
	if err != nil {
		return err
	}

	tree, err := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, err)
		}

		// Dependency gate: only consulted before a tree exists. Once a batch
		// has a tree, later passes just re-score/re-select it.
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

	if scoresChanged || selectionChanged {
		newVersion := tree.Version + 1
		if err := c.store.GetSpeculationTreeStore().Update(ctx, batch.ID, tree.Version, newVersion, tree.Paths); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update speculation tree for batch %s: %w", batch.ID, err)
		}
		tree.Version = newVersion
	}

	// The tree does not influence the forward step below — no downstream
	// stage consumes it yet.
	switch batch.State {
	case entity.BatchStateCreated, entity.BatchStateScored:
		return c.startSpeculation(ctx, batch)
	case entity.BatchStateSpeculating:
		return c.tryFinalize(ctx, batch)
	default:
		return nil
	}
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

// startSpeculation publishes the batch to the build stage, then transitions
// it to Speculating.
func (c *Controller) startSpeculation(ctx context.Context, batch entity.Batch) error {
	c.logger.Infow("starting speculation",
		"batch_id", batch.ID,
		"speculation_chain", append(append([]string{}, batch.Dependencies...), batch.ID),
	)

	if err := c.publish(ctx, topickey.TopicKeyBuild, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to build: %w", err)
	}

	// Optimistic CAS: if the version has already advanced (concurrent speculate),
	// the next event will see the new state and behave correctly.
	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateSpeculating); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to speculating: %w", batch.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "started_speculation", 1)
	return nil
}

// tryFinalize publishes to merge and transitions to Merging iff every
// dependency batch has reached Succeeded. Cancelled deps are treated as
// out-of-the-way: the cancelled batch will never land, so it can no longer
// conflict — drop it from the chain and proceed. Failed deps still cascade
// via failOnDependency. If some deps are still in flight, the call is a
// no-op: each dependency's own terminal pass re-publishes this batch to
// speculate (the terminal branch in Process), so waiting never needs a poll.
//
// TODO: when a dependency fails we currently fail this batch outright.
// We will need to respeculate the failed paths — drop the failed dep
// from the chain and re-issue speculation for the surviving ordering(s)
// — instead of cascading the failure into requests that could still land.
func (c *Controller) tryFinalize(ctx context.Context, batch entity.Batch) error {
	deps, err := c.fetchDependencies(ctx, batch)
	if err != nil {
		return err
	}

	pending := make([]string, 0, len(deps))
	for _, d := range deps {
		switch d.State {
		case entity.BatchStateSucceeded:
			// ok
		case entity.BatchStateCancelled:
			// Out-of-the-way: the cancelled batch will never land, so it can
			// no longer conflict. Drop it from the chain and continue.
			c.metricsScope.Counter("dependency_cancelled_skipped").Inc(1)
			c.logger.Infow("dependency cancelled; dropping from speculation chain",
				"batch_id", batch.ID,
				"dependency_id", d.ID,
			)
		case entity.BatchStateFailed:
			return c.failOnDependency(ctx, batch, d)
		default:
			pending = append(pending, d.ID)
		}
	}

	if len(pending) > 0 {
		metrics.NamedCounter(c.metricsScope, opName, "waiting_on_deps", 1)
		c.logger.Debugw("dependencies still in flight; waiting",
			"batch_id", batch.ID,
			"pending_dependency_ids", pending,
		)
		return nil
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

	return nil
}

// failOnDependency transitions a Speculating batch to Failed when one of its
// dependencies has reached a non-succeeding terminal state, then publishes to
// the conclude queue so the request store and request log get reconciled.
// Without this transition the batch would sit in Speculating forever — no
// downstream event ever fires for it again. The batch's own dependents are
// woken right after the terminal CAS so the failure cascades downstream
// instead of stranding them mid-wait; a crash between the CAS and the
// fan-out is recovered by the terminal branch in Process on redelivery.
func (c *Controller) failOnDependency(ctx context.Context, batch entity.Batch, dep entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "dependency_failed", 1)
	c.logger.Warnw("dependency in non-succeeding terminal state; failing batch",
		"batch_id", batch.ID,
		"dependency_id", dep.ID,
		"dependency_state", string(dep.State),
	)

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateFailed); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to failed: %w", batch.ID, err)
	}

	if err := c.respeculateDependents(ctx, batch); err != nil {
		return err
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}

	return nil
}

// cancelBatch drives a batch from BatchStateCancelling to BatchStateCancelled.
// The cancel controller records the user's intent (Cancelling) and hands the
// batch off; speculate owns the rest because all the work that must precede
// the terminal write — flipping in-flight builds, respeculating dependents —
// already lives in the speculate domain. The terminal transition is the
// single writer of every non-Cancelling batch state across the system.
//
// Order matters for correctness:
//
//  1. Cancel the in-flight Build entity (build.ID == batch.ID; one Get + one
//     UpdateStatus covers all builds for this batch). Idempotent: tolerate
//     ErrNotFound (no build was scheduled), skip if already terminal.
//
//  2. CAS the batch to terminal Cancelled. This must happen BEFORE the
//     dependent fan-out: tryFinalize only drops a Cancelled dep from the
//     chain, so dependents woken with the dep still in Cancelling would
//     wait pending and never get pinged again.
//
//  3. Re-publish each downstream dependent to speculate so they can drop
//     this cancelled batch from their chain and advance (or finalize, if
//     this was their last outstanding dep).
//
//  4. Publish to conclude so contained requests reach RequestStateCancelled.
//
// A crash between steps 2 and 3/4 is recovered on redelivery via the
// terminal self-heal branch, which re-runs the dependent fan-out and the
// conclude publish for already-Cancelled batches.
//
// storage.ErrVersionMismatch on the terminal CAS is returned as-is for the
// base controller to classify as retryable; the redelivery will land in the
// self-heal branch and complete the fan-out.
func (c *Controller) cancelBatch(ctx context.Context, batch entity.Batch) error {
	metrics.NamedCounter(c.metricsScope, opName, "cancel_batch", 1)
	c.logger.Infow("cancelling batch",
		"batch_id", batch.ID,
		"queue", batch.Queue,
	)

	// TODO(respeculate-collateral): re-enqueue Land for every request in batch.Contains
	// except the user-cancelled request. Today the whole batch dies (per spec) and the
	// collateral requests need a fresh request ID and a re-publish to TopicKeyStart so
	// they can be re-batched without the cancelled change.

	if err := c.cancelBuild(ctx, batch); err != nil {
		return err
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, entity.BatchStateCancelled); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update batch %s state to cancelled: %w", batch.ID, err)
	}
	batch.Version = newVersion
	batch.State = entity.BatchStateCancelled

	if err := c.respeculateDependents(ctx, batch); err != nil {
		return err
	}

	if err := c.publish(ctx, topickey.TopicKeyConclude, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}

	return nil
}

// cancelBuild flips any in-flight Build entity for the batch to
// BuildStatusCancelled. Builds use build.ID == batch.ID, so a single Get
// covers every build scheduled for the batch. Tolerates ErrNotFound (no
// build was ever scheduled — the batch was cancelled before speculation
// started building) and skips already-terminal builds.
func (c *Controller) cancelBuild(ctx context.Context, batch entity.Batch) error {
	build, err := c.store.GetBuildStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "cancel_build_not_found", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get build for batch %s: %w", batch.ID, err)
	}

	if build.Status.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "cancel_build_already_terminal", 1)
		return nil
	}

	if err := c.store.GetBuildStore().UpdateStatus(ctx, batch.ID, entity.BuildStatusCancelled); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to cancel build for batch %s: %w", batch.ID, err)
	}
	metrics.NamedCounter(c.metricsScope, opName, "cancel_build_done", 1)
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
// Called from the cancelBatch terminal flow, from failOnDependency, and from
// the terminal branch in Process — the latter both on the first pass after
// mergesignal drives a batch terminal (the normal dependent wake-up) and on
// redelivery (self-heal).
func (c *Controller) respeculateDependents(ctx context.Context, batch entity.Batch) error {
	bd, err := c.store.GetBatchDependentStore().Get(ctx, batch.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch dependents for batch %s: %w", batch.ID, err)
	}

	for _, depID := range bd.Dependents {
		// Alternative: process each dependent inline (load batch, run the
		// equivalent of tryFinalize) instead of publishing back to ourselves.
		// Rejected for now: per-message retry isolation, fresh per-dependent
		// reads, consumer-pool parallelism / backpressure, and the existing
		// state-machine dispatch in Process all argue for the publish. Revisit
		// if the extra message hop ever shows up as latency or cost.
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
