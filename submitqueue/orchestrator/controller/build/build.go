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

package build

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles build queue messages.
//
// It loads the batch's speculation tree and, for each path, acts on the
// path's status: a Prioritized path that has no build yet gets one triggered
// through the queue's build runner, and a Cancelling path has its in-flight
// build cancelled. Every other status is left untouched, and the tree is
// never written here — path statuses are read-only inputs to this
// controller. It is the sole caller of the build runner for path-level work,
// which is why persisted Cancelling intents are enacted here rather than
// where they were decided.
//
// Terminal batches are skipped before any path work: the cancel flow only
// writes its terminal state after every build has quiesced, and other
// terminal transitions leave stragglers to run out, so there is nothing for
// this controller to enact. A Cancelling batch is the deliberate exception —
// it is being torn down batch-wide, so the loop cancels every path's
// in-flight build regardless of the path's recorded status (the sweep that
// records per-path intents may not have run yet), which is how batch-level
// cancellation reaches the runner — but no new CI is ever triggered for a
// batch that is being torn down.
//
// Dedup for triggering is on the path->build mapping (PathBuildStore), not
// on a Build row keyed by a derived key: the mapping is readable before
// Trigger ever runs, so it is checked first. A crash between Trigger
// succeeding and the mapping Create means redelivery re-triggers a fresh
// build for the same path; the new mapping Create then races the
// (never-persisted) old one and simply wins, orphaning the earlier CI
// build. See the per-path loop below for the full crash-safety ordering.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	buildRunners  buildrunner.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// NewController creates a new build controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	buildRunners buildrunner.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("build_controller"),
		metricsScope:  scope.SubScope("build_controller"),
		store:         store,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a build delivery from the queue.
// Deserializes the batch, loads its speculation tree, and for every path
// either triggers a build (Prioritized, no build yet) or enacts a persisted
// cancel intent (Cancelling), publishing to the build signal topic as
// appropriate. For a Cancelling batch it instead cancels every path's
// in-flight build, whatever the path's recorded status, and triggers
// nothing.
// Returns nil to ack (success), or error to nack/reject.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize batch ID from payload
	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	// Fetch batch from storage
	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	c.logger.Infow("received build event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Terminal batches are settled — the cancel flow only writes Cancelled
	// once every build has been observed terminal, and Failed/Succeeded
	// leave any straggler builds to run out — so there is no path work left
	// here. Cancelling batches proceed: the per-path loop below cancels
	// every in-flight build of a batch being torn down (that is how a
	// batch-level cancel reaches the runner) and guarantees no new CI is
	// ever kicked off for it.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_terminal", 1)
		c.logger.Infow("skipping build for terminal batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// Load this batch's speculation tree. A build message is only ever
	// published by the prioritize stage after it has read the tree, the tree
	// is created before the batch is ever published to prioritize, and trees
	// are never deleted — so by the time this message is readable the tree
	// exists. A Get miss here is an invariant violation (corrupted or
	// manually mutated state), not a transient condition: the error
	// propagates unclassified, the message dead-letters, and the DLQ
	// consumer fails the batch loudly instead of retrying against broken
	// state.
	tree, err := c.store.GetSpeculationTreeStore().Get(ctx, batch.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get speculation tree for batch %s: %w", batch.ID, err)
	}

	// Resolve the build runner lazily: every path in this message shares the
	// same queue, so a pass whose paths need no triggering or cancelling
	// never resolves one.
	resolveRunner := c.runnerResolver(batch.Queue)

	triggered := 0
	for _, p := range tree.Paths {
		if batch.State == entity.BatchStateCancelling {
			// Batch-wide teardown: every path of a Cancelling batch is
			// doomed, so any in-flight build is cancelled right here,
			// regardless of the path's recorded status — a path can still
			// read Building (or even Cancelled — see the pre-build cancel
			// race in speculate's cancelTree) when this message races the
			// tree sweep that records the intents, and waiting for the
			// sweep would only leave CI running one round-trip longer.
			// Passed and Failed are skipped without I/O: those statuses
			// derive from terminal builds, so there is nothing to stop.
			// enactCancel tolerates paths with no build. Never trigger
			// new CI for a batch being torn down; speculate's sweep owns
			// settling the path statuses and the terminal batch write.
			if p.Status == entity.SpeculationPathStatusPassed || p.Status == entity.SpeculationPathStatusFailed {
				continue
			}
			if err := c.enactCancel(ctx, batch, p, resolveRunner); err != nil {
				return err
			}
			continue
		}

		if p.Status == entity.SpeculationPathStatusCancelling {
			// A Cancelling path is a persisted cancel intent — recorded
			// elsewhere, enacted here by the sole caller of the build runner
			// for path-level work.
			if err := c.enactCancel(ctx, batch, p, resolveRunner); err != nil {
				return err
			}
			continue
		}

		if p.Status != entity.SpeculationPathStatusPrioritized {
			// Every other status is another stage's concern: Candidate and
			// Selected haven't been admitted yet, Building/Passed/Failed
			// already have a build in flight or resolved, and Cancelled is
			// terminal. This controller moves admitted (Prioritized) paths
			// forward and enacts persisted Cancelling intents (handled
			// above) — nothing else.
			continue
		}

		// Per-path ordering matters for crash-safety. Dedup happens BEFORE
		// Trigger, keyed on the path->build mapping (readable up front,
		// unlike a Build row that only exists after Trigger succeeds). Once
		// a path needs triggering, the order is: (1) Trigger with the
		// runner, (2) persist the Build row, (3) persist the path->build
		// mapping. A crash between (1) and (3) is recovered by redelivery
		// re-running this loop: the mapping is still absent, so the path is
		// (re-)triggered and the fresh mapping Create either wins outright
		// or loses a race to a concurrent delivery that got there first — in
		// which case we defer to the winner instead of erroring (see below).
		pb, err := c.store.GetSpeculationPathBuildStore().Get(ctx, p.ID)
		if err == nil {
			// A mapping already exists for this path: a build was already
			// triggered, either by a previous pass or a previous delivery of
			// this message. Load it and decide what (if anything) to do.
			b, err := c.store.GetBuildStore().Get(ctx, pb.BuildID)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					// Invariant breach: the write order below guarantees a
					// mapping is only created once its build row exists. The
					// mapping is still authoritative even though its target
					// is missing, so we do not trigger a duplicate build for
					// this path — we just skip it defensively.
					metrics.NamedCounter(c.metricsScope, opName, "mapping_dangling", 1)
					c.logger.Warnw("path->build mapping points at a missing build; skipping path",
						"batch_id", batch.ID,
						"path_id", p.ID,
						"build_id", pb.BuildID,
					)
					continue
				}
				metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
				return fmt.Errorf("failed to get build %s for path %s of batch %s: %w", pb.BuildID, p.ID, batch.ID, err)
			}

			if b.Status.IsTerminal() {
				// The path's build already ran to completion; a later
				// reconcile pass folds that outcome into the path's status,
				// so triggering here would only race it. A Prioritized path
				// pointing at a terminal build is therefore always the
				// pre-reconcile window — never
				// "build it again": in the current model a path is built at
				// most once, Cancel is only ever applied to paths whose
				// assumption is dead (or preemptively evicted), and no stage
				// re-admits a Cancelled path. If a future preemptive policy
				// adds cancel-then-readmit, this branch is the seam it lands
				// in: re-trigger and conditionally re-point the mapping to
				// the fresh build under SpeculationPathBuild.Version.
				continue
			}

			// Non-terminal existing build: republish buildsignal to close the
			// crash window between the original Create and the original
			// buildsignal publish. This creates duplicate poll loops on
			// redelivery, but that is harmless — buildsignal's self-republish
			// reuses the message ID (build.ID), and the mysql queue publisher
			// dedups publishes on (topic, partition, id), so redundant
			// re-triggers of the poll loop coalesce rather than double-poll
			// forever.
			if err := c.publish(ctx, topickey.TopicKeyBuildSignal, b); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
				return fmt.Errorf("failed to re-publish to buildsignal: %w", err)
			}
			continue
		} else if !errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to get path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, err)
		}

		// No mapping yet: load the path's base (assumed-good predecessor
		// batches) as identity; the build runner resolves each batch's
		// changes itself. head is this batch.
		base, err := c.loadBatches(ctx, p.Path.Base)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to load base batches for path (head=%s, base=%v) of batch %s: %w", p.Path.Head, p.Path.Base, batch.ID, err)
		}

		// Trigger the build with the queue's build runner. metadata is nil
		// until a caller-supplied source materializes (e.g. requester /
		// ticket pulled off the originating LandRequest).
		runner, err := resolveRunner()
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
			return fmt.Errorf("failed to get build runner for batch %s: %w", batch.ID, err)
		}
		runnerID, err := runner.Trigger(ctx, base, batch, nil)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
			return fmt.Errorf("failed to trigger build for batch %s: %w", batch.ID, err)
		}

		build := entity.Build{
			ID:                runnerID.ID,
			BatchID:           batch.ID,
			SpeculationPathID: p.ID,
			Status:            entity.BuildStatusAccepted,
		}

		// Persist the initial Build snapshot so the buildsignal poll loop has
		// a row to UpdateStatus against. ErrAlreadyExists is benign — a
		// redelivery of this message after a previous successful Create for
		// this specific path. This tolerance is not the dedup mechanism (the
		// mapping below is); it stays for the same crash-safety reason as
		// before: a build row can pre-exist from a prior partial pass.
		if err := c.store.GetBuildStore().Create(ctx, build); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to persist build %s: %w", build.ID, err)
		}

		// Persist the path->build mapping. This is the dedup marker for the
		// path, checked at the top of this loop before Trigger runs.
		mapping := entity.SpeculationPathBuild{
			PathID:    p.ID,
			BuildID:   build.ID,
			BatchID:   batch.ID,
			Version:   1,
			CreatedAt: time.Now().UnixMilli(),
		}
		if err := c.store.GetSpeculationPathBuildStore().Create(ctx, mapping); err != nil {
			if errors.Is(err, storage.ErrAlreadyExists) {
				// A concurrent delivery already won the mapping race for this
				// path. Our just-created build row above is now an accepted
				// orphan: lost mapping races cost one orphaned CI build by
				// design — duplicates are the retry/redundancy mechanism, and
				// the recorded mapping (not the build row) is the source of
				// truth for "which build resolves this path." Republish
				// buildsignal for the winner so it has an active poll loop
				// even if its own publish was lost.
				metrics.NamedCounter(c.metricsScope, opName, "trigger_race_lost", 1)
				winner, gerr := c.store.GetSpeculationPathBuildStore().Get(ctx, p.ID)
				if gerr != nil {
					metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
					return fmt.Errorf("failed to re-get path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, gerr)
				}
				winnerBuild, gerr := c.store.GetBuildStore().Get(ctx, winner.BuildID)
				if gerr != nil {
					metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
					return fmt.Errorf("failed to get winning build %s for path %s of batch %s: %w", winner.BuildID, p.ID, batch.ID, gerr)
				}
				if err := c.publish(ctx, topickey.TopicKeyBuildSignal, winnerBuild); err != nil {
					metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
					return fmt.Errorf("failed to re-publish to buildsignal: %w", err)
				}
				continue
			}
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to persist path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, err)
		}

		// Hand off to the buildsignal poll loop; it calls Status, updates the
		// persisted Build, publishes to speculate, and re-publishes itself
		// via PublishAfter until terminal.
		if err := c.publish(ctx, topickey.TopicKeyBuildSignal, build); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
			return fmt.Errorf("failed to publish to buildsignal: %w", err)
		}

		c.logger.Infow("published build to buildsignal",
			"batch_id", batch.ID,
			"build_id", build.ID,
			"status", string(build.Status),
			"topic_key", topickey.TopicKeyBuildSignal,
		)

		triggered++
	}

	if triggered == 0 {
		// Either no path was Prioritized, or every Prioritized path already
		// had a matching Build row. This is a no-op republish, not an error.
		metrics.NamedCounter(c.metricsScope, opName, "no_paths_to_trigger", 1)
	}

	return nil // Success - message will be acked
}

// runnerResolver returns a lazily-memoizing accessor for queue's build
// runner: the factory is consulted on the first call only, and every
// subsequent call reuses that result. Callers that never invoke the accessor
// never resolve a runner at all.
func (c *Controller) runnerResolver(queue string) func() (buildrunner.BuildRunner, error) {
	var runner buildrunner.BuildRunner
	return func() (buildrunner.BuildRunner, error) {
		if runner != nil {
			return runner, nil
		}
		r, err := c.buildRunners.For(buildrunner.Config{QueueName: queue})
		if err != nil {
			return nil, err
		}
		runner = r
		return r, nil
	}
}

// enactCancel stops path p's build, if one is in flight: it resolves the
// path's build via the path->build mapping, issues a runner Cancel against
// it if the build is not already terminal, and republishes buildsignal so
// the poll loop observes the cancellation promptly instead of waiting out
// its current delay. Called both for a path's persisted Cancelling intent
// and for every path of a Cancelling batch's batch-wide teardown. Cancel is
// idempotent from the runner's point of view, so redelivery (e.g. after a
// crash between Cancel and the republish) simply re-issues it.
//
// Every lookup miss here is tolerated rather than treated as an error: a
// path can be marked for cancellation before this controller ever triggered
// a build for it (no mapping yet), or the mapping can point at a build row
// that a defensive skip elsewhere left dangling. In both cases there is
// nothing to cancel, so settling the path's status is left to speculate's
// next pass.
func (c *Controller) enactCancel(ctx context.Context, batch entity.Batch, p entity.SpeculationPathInfo, resolveRunner func() (buildrunner.BuildRunner, error)) error {
	pb, err := c.store.GetSpeculationPathBuildStore().Get(ctx, p.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "cancel_no_mapping", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get path->build mapping for path %s of batch %s: %w", p.ID, batch.ID, err)
	}

	b, err := c.store.GetBuildStore().Get(ctx, pb.BuildID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Same invariant-breach defense as the trigger path above: the
			// mapping is authoritative even though its target is missing, so
			// skip rather than guessing.
			metrics.NamedCounter(c.metricsScope, opName, "mapping_dangling", 1)
			c.logger.Warnw("path->build mapping points at a missing build; skipping cancel",
				"batch_id", batch.ID,
				"path_id", p.ID,
				"build_id", pb.BuildID,
			)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get build %s for path %s of batch %s: %w", pb.BuildID, p.ID, batch.ID, err)
	}

	if b.Status.IsTerminal() {
		// Nothing in flight to cancel; speculate's reconcile settles the
		// path's own status on its next pass.
		metrics.NamedCounter(c.metricsScope, opName, "cancel_already_terminal", 1)
		return nil
	}

	runner, err := resolveRunner()
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "cancel_errors", 1)
		return fmt.Errorf("failed to get build runner for batch %s: %w", batch.ID, err)
	}
	if err := runner.Cancel(ctx, entity.BuildID{ID: b.ID}); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "cancel_errors", 1)
		return fmt.Errorf("failed to cancel build %s for path %s of batch %s: %w", b.ID, p.ID, batch.ID, err)
	}

	if err := c.publish(ctx, topickey.TopicKeyBuildSignal, b); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to re-publish to buildsignal after cancel: %w", err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "cancel_enacted", 1)
	c.logger.Infow("enacted cancel for speculation path",
		"batch_id", batch.ID,
		"path_id", p.ID,
		"build_id", b.ID,
	)
	return nil
}

// loadBatches loads each batch by ID, preserving order. Used to load the base
// (dependency batches) identity handed to BuildRunner.Trigger; the build runner
// resolves each batch's changes itself.
func (c *Controller) loadBatches(ctx context.Context, batchIDs []string) ([]entity.Batch, error) {
	if len(batchIDs) == 0 {
		return nil, nil
	}
	batches := make([]entity.Batch, 0, len(batchIDs))
	for _, bID := range batchIDs {
		b, err := c.store.GetBatchStore().Get(ctx, bID)
		if err != nil {
			return nil, fmt.Errorf("failed to get batch %s: %w", bID, err)
		}
		batches = append(batches, b)
	}
	return batches, nil
}

// publish publishes a build's ID to the specified topic key. Only the
// identifier travels on the queue; the consumer loads the full Build from
// storage, keeping the message small and the store the single source of truth.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, build entity.Build) error {
	payload, err := entity.BuildID{ID: build.ID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize build ID: %w", err)
	}

	msg := entityqueue.NewMessage(build.ID, payload, build.BatchID, nil)

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
	return "build"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
