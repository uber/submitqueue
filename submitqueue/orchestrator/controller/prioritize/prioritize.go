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

// Package prioritize implements the queue-wide reconcile stage that rations
// a shared build budget across every in-flight batch of a queue.
//
// Unlike every other pipeline stage, prioritize is not batch-scoped: its
// queue message carries only a queue name (entity.QueueID), and each
// invocation re-evaluates every Speculating batch's speculation tree for
// that queue together. It gathers the queue-wide set of candidate paths
// (Selected, Prioritized, or Building), hands them to the queue's
// prioritizer.Prioritizer, and applies the returned decisions:
//
//   - Promote on a Selected path clears it to run (-> Prioritized).
//   - Cancel on a Building path asks the build runner to stop it and marks
//     it Cancelling; the build stage's own signal loop confirms the stop.
//   - Cancel on a Prioritized path (no build yet) drops it straight to
//     Cancelled.
//
// Cancelling batches' trees are loaded alongside the Speculating ones, but
// for routing only: a batch being torn down still carries persisted
// Cancelling intents that must reach the build stage, and this stage is the
// one channel from tree state to build messages. Their paths are never
// offered to the prioritizer — a doomed path neither wants a slot nor is
// worth preempting for.
//
// Each affected tree is persisted under its own optimistic lock, so a
// version conflict only nacks and re-derives that tree's part of the round
// on redelivery — the whole computation is a pure function of freshly read
// state, so recomputing it is always safe. After applying decisions, the
// controller republishes to the build topic for every batch whose tree has
// at least one Prioritized path with no build yet or one Cancelling path
// (a persisted cancel intent), not just ones it touched this round — this
// heals a build message dropped by a prior crash and is itself idempotent,
// since the build stage dedups triggers on the path->build mapping and
// runner cancels are idempotent.
package prioritize

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
	"github.com/uber/submitqueue/submitqueue/extension/speculation/prioritizer"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// Controller consumes queue-wide prioritize messages, ranks every candidate
// speculation path in the queue against its build budget, applies the
// resulting decisions to each affected speculation tree, and republishes to
// build for paths cleared to run.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	prioritizers  prioritizer.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new prioritize controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	prioritizers prioritizer.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("prioritize_controller"),
		metricsScope:  scope.SubScope("prioritize_controller"),
		store:         store,
		prioritizers:  prioritizers,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process re-evaluates the build budget for one queue: it loads every
// Speculating and Cancelling batch's speculation tree, ranks the queue-wide
// candidate paths (Speculating batches only) through the queue's
// prioritizer, applies the returned decisions, persists the affected trees,
// and republishes to build for any path now cleared to run or carrying a
// persisted cancel intent. Returns nil to ack (success), or error to nack
// (retry) — the whole round is a pure function of freshly read state, so
// redelivery simply recomputes it.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	qid, err := entity.QueueIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize queue ID: %w", err)
	}

	batches, err := c.store.GetBatchStore().GetByQueueAndStates(ctx, qid.Name, []entity.BatchState{
		entity.BatchStateSpeculating,
		entity.BatchStateCancelling,
	})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get in-flight batches for queue %s: %w", qid.Name, err)
	}
	if len(batches) == 0 {
		metrics.NamedCounter(c.metricsScope, opName, "no_batches", 1)
		return nil
	}

	trees, err := c.loadTrees(ctx, batches)
	if err != nil {
		return err
	}
	if len(trees) == 0 {
		metrics.NamedCounter(c.metricsScope, opName, "no_trees", 1)
		return nil
	}

	// A Cancelling batch's tree is loaded for routing only: republishBuilds
	// must still deliver its persisted Cancelling intents to the build
	// stage, but none of its paths may compete for (or count against) the
	// build budget.
	cancelling := make(map[string]bool, len(batches))
	for _, b := range batches {
		if b.State == entity.BatchStateCancelling {
			cancelling[b.ID] = true
		}
	}

	candidates := candidatesOf(trees, cancelling)

	pf, err := c.prioritizers.For(prioritizer.Config{QueueName: qid.Name})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "prioritizer_errors", 1)
		return fmt.Errorf("failed to get prioritizer for queue %s: %w", qid.Name, err)
	}
	decisions, err := pf.Prioritize(ctx, candidates)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "prioritizer_errors", 1)
		return fmt.Errorf("failed to prioritize candidates for queue %s: %w", qid.Name, err)
	}

	changed, err := c.applyDecisions(ctx, qid.Name, trees, cancelling, decisions)
	if err != nil {
		return err
	}

	// Persist before publish. If republishBuilds fails below (or the process
	// crashes between the two), the message nacks and the redelivered round
	// recomputes everything from the persisted state — it carries no memory
	// of what this attempt picked, and needs none: paths promoted here are
	// now Prioritized, so the prioritizer counts them as slot holders instead
	// of re-promoting them, and republishBuilds derives "who needs a build
	// message" from the trees themselves (any Prioritized path without a
	// build) rather than from this round's decisions, so a dropped publish is
	// healed on the next pass.
	if err := c.persistTrees(ctx, trees, changed); err != nil {
		return err
	}

	if err := c.republishBuilds(ctx, qid.Name, trees); err != nil {
		return err
	}

	return nil
}

// loadTrees loads the speculation tree for each batch, keyed by batch ID.
// A batch with no tree yet (storage.ErrNotFound) has not been speculated on
// yet and is skipped; any other error aborts the round.
func (c *Controller) loadTrees(ctx context.Context, batches []entity.Batch) (map[string]entity.SpeculationTree, error) {
	trees := make(map[string]entity.SpeculationTree, len(batches))
	for _, b := range batches {
		tree, err := c.store.GetSpeculationTreeStore().Get(ctx, b.ID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				metrics.NamedCounter(c.metricsScope, opName, "tree_not_found_skipped", 1)
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return nil, fmt.Errorf("failed to get speculation tree for batch %s: %w", b.ID, err)
		}
		trees[b.ID] = tree
	}
	return trees, nil
}

// candidatesOf flattens every path across all loaded trees whose status
// represents a queue-wide interest: Selected (wants a slot), or
// Prioritized/Building (holds one). Trees of batches in the cancelling set
// are skipped wholesale — their paths are doomed, so they neither want a
// slot nor hold one worth ranking; those trees are loaded only so
// republishBuilds can route their persisted Cancelling intents.
func candidatesOf(trees map[string]entity.SpeculationTree, cancelling map[string]bool) []entity.SpeculationPathInfo {
	var candidates []entity.SpeculationPathInfo
	for batchID, tree := range trees {
		if cancelling[batchID] {
			continue
		}
		for _, p := range tree.Paths {
			switch p.Status {
			case entity.SpeculationPathStatusSelected,
				entity.SpeculationPathStatusPrioritized,
				entity.SpeculationPathStatusBuilding:
				candidates = append(candidates, p)
			}
		}
	}
	return candidates
}

// applyDecisions maps each prioritizer decision to a status transition on its
// tree's matching path, mutating trees in place. It returns the set of batch
// IDs whose tree actually changed, so the caller persists only those.
//
// Decisions are captured as intent, never enacted here: this controller does
// not talk to the build system. A Cancel on a Building path only flips it to
// Cancelling in the tree; the build stage — the sole owner of runner
// interaction — enacts the runner cancel when it processes the batch, retried
// for free on every build message until the build reaches a terminal state
// and the speculate reconcile settles the path to Cancelled. Persisting the
// intent before any side effect is what makes the flow crash-safe: a lost
// build message is healed by republishBuilds, not by remembering this round.
//
// A decision naming a batch or path the round did not offer as a candidate —
// including any path of a Cancelling batch, whose tree is loaded for routing
// only — or applying an action the path's current status does not support,
// is a policy bug in the prioritizer: it is logged as a warning and skipped
// rather than corrupting the tree.
func (c *Controller) applyDecisions(
	ctx context.Context,
	queue string,
	trees map[string]entity.SpeculationTree,
	cancelling map[string]bool,
	decisions []entity.PathDecision,
) (map[string]bool, error) {
	changed := make(map[string]bool)

	// Decisions name paths by ID only; recover each path's tree from the
	// trees loaded this round rather than parsing anything out of the ID.
	// Cancelling batches' paths are deliberately absent: they were never
	// candidates, so a decision naming one falls through to the "not loaded
	// this round" skip below.
	pathBatch := make(map[string]string)
	for batchID, tree := range trees {
		if cancelling[batchID] {
			continue
		}
		for _, p := range tree.Paths {
			pathBatch[p.ID] = batchID
		}
	}

	seen := make(map[string]bool, len(decisions))
	for _, d := range decisions {
		if seen[d.PathID] {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: duplicate decision for path",
				"queue", queue,
				"path_id", d.PathID,
				"action", d.Action,
			)
			continue
		}
		seen[d.PathID] = true

		batchID, ok := pathBatch[d.PathID]
		if !ok {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: path not loaded this round",
				"queue", queue,
				"path_id", d.PathID,
				"action", d.Action,
			)
			continue
		}
		tree := trees[batchID]

		// pathBatch was built from these same trees, so the ID must resolve;
		// guard anyway so an index bug degrades to a skipped decision.
		idx := tree.PathIndex(d.PathID)
		if idx == -1 {
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: path not found in tree",
				"queue", queue,
				"batch_id", batchID,
				"path_id", d.PathID,
				"action", d.Action,
			)
			continue
		}

		info := tree.Paths[idx]
		switch {
		case d.Action == entity.SpeculationPathActionPromote && info.Status == entity.SpeculationPathStatusSelected:
			info.Status = entity.SpeculationPathStatusPrioritized
			metrics.NamedCounter(c.metricsScope, opName, "promoted", 1)

		case d.Action == entity.SpeculationPathActionCancel && info.Status == entity.SpeculationPathStatusBuilding:
			info.Status = entity.SpeculationPathStatusCancelling
			metrics.NamedCounter(c.metricsScope, opName, "cancelling", 1)

		case d.Action == entity.SpeculationPathActionCancel && info.Status == entity.SpeculationPathStatusPrioritized:
			info.Status = entity.SpeculationPathStatusCancelled
			metrics.NamedCounter(c.metricsScope, opName, "cancelled", 1)

		default:
			metrics.NamedCounter(c.metricsScope, opName, "illegal_decision", 1)
			c.logger.Warnw("illegal decision: action not valid for path status",
				"queue", queue,
				"batch_id", batchID,
				"path_id", d.PathID,
				"action", d.Action,
				"status", string(info.Status),
			)
			continue
		}

		tree.Paths[idx] = info
		trees[batchID] = tree
		changed[batchID] = true
	}

	return changed, nil
}

// persistTrees writes each changed tree's paths under its own optimistic
// lock, bumping Version in trees on success. Version arithmetic is owned by
// the controller; the store performs a pure conditional write.
func (c *Controller) persistTrees(ctx context.Context, trees map[string]entity.SpeculationTree, changed map[string]bool) error {
	for batchID := range changed {
		tree := trees[batchID]
		newVersion := tree.Version + 1
		if err := c.store.GetSpeculationTreeStore().Update(ctx, batchID, tree.Version, newVersion, tree.Paths); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update speculation tree for batch %s: %w", batchID, err)
		}
		tree.Version = newVersion
		trees[batchID] = tree
	}
	return nil
}

// republishBuilds re-publishes a build message for every batch whose tree
// carries work the build stage still has to enact — a Prioritized path with
// no build yet (needs a trigger), or a Cancelling path (a persisted cancel
// intent whose runner cancel the build stage owns) — not just paths touched
// this round. This heals a build message a prior crash dropped between the
// tree write and the publish; it is safe to repeat because the build stage
// dedups triggers on the path->build mapping and runner cancels are
// idempotent.
func (c *Controller) republishBuilds(ctx context.Context, queue string, trees map[string]entity.SpeculationTree) error {
	for batchID, tree := range trees {
		needsBuild := false
		for _, p := range tree.Paths {
			if (p.Status == entity.SpeculationPathStatusPrioritized && p.BuildID == "") ||
				p.Status == entity.SpeculationPathStatusCancelling {
				needsBuild = true
				break
			}
		}
		if !needsBuild {
			continue
		}
		// The message ID encodes (batch, tree version): a round that changed
		// the tree (new promotions) mints a fresh ID and is guaranteed
		// delivery, while pure heal republishes reuse the last one and
		// coalesce under the queue's publish idempotency on
		// (topic, partition_key, id) — a bare batch ID would coalesce a
		// later promotion round away against the first round's
		// not-yet-collected row.
		msgID := fmt.Sprintf("%s/v%d", batchID, tree.Version)
		if err := c.publishBuild(ctx, msgID, batchID, queue); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish batch %s to build: %w", batchID, err)
		}
		metrics.NamedCounter(c.metricsScope, opName, "build_republished", 1)
	}
	return nil
}

// publishBuild publishes a batch ID to the build topic. Only the identifier
// travels on the queue; the build controller reloads the full Batch (and,
// eventually, its speculation tree) from storage.
func (c *Controller) publishBuild(ctx context.Context, msgID, batchID, partitionKey string) error {
	bid := entity.BatchID{ID: batchID}
	payload, err := bid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(msgID, payload, partitionKey, nil)

	q, ok := c.registry.Queue(topickey.TopicKeyBuild)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyBuild)
	}

	topicName, ok := c.registry.TopicName(topickey.TopicKeyBuild)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyBuild)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "prioritize"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
