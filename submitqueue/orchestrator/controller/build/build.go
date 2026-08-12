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

// Package build starts the builds a head batch's speculation paths have been
// funded for. A message names only the head; the path set is the instruction,
// and each pending entry in it is a build to start. Nothing about the action
// travels on the wire, so a dispatch cannot go stale between publish and
// delivery: a path refuted after its message was sent is simply no longer
// pending by the time the set is read.
//
// This stage only starts builds. Stopping them is the poll loop's job: every
// build started here is handed to the buildsignal stage, which follows it to a
// terminal state and stops it the moment its path no longer wants it. The
// split is what keeps this stage simple — it decides whether a build should
// exist, never whether one should die — and it holds together because of one
// invariant this stage maintains: every build that gets a link gets a signal
// (see ensureSignal for the crash case).
package build

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles build queue messages.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
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
	stores storage.Factory,
	buildRunners buildrunner.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("build_controller"),
		metricsScope:  scope.SubScope("build_controller"),
		stores:        stores,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process starts a build for every pending path in the head's set.
// Returns nil to ack (success), or error to nack (retry).
//
// This controller never writes the path set. The set is the speculate run's
// state — what it decided to fund and what it wants stopped — and a second
// writer on it would make speculate lose compare-and-swap races across its own
// far longer read-decide-write window. What this stage knows is the build it
// started, and that goes in records of its own.
//
// A failure partway through nacks the whole message and redelivery re-runs all
// of it: starts are made idempotent by the link record each one writes (see
// startPath), and a redelivery additionally re-publishes the signal for any
// live path already linked to a build, in case the crashed attempt died
// between the link and the signal (see ensureSignal).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	// Fetch batch from storage
	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}

	set, err := store.GetSpeculationPathSetStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// No speculation run has funded this head yet, so there is nothing
			// to dispatch. A later run publishes again once it has.
			metrics.NamedCounter(c.metricsScope, opName, "no_path_set", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get path set for batch %s: %w", batch.ID, err)
	}

	c.logger.Debugw("received build event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"paths", len(set.Paths),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// halted covers terminal and cancelling batches, and gates only starts:
	// a halted batch gets no new builds, and its running ones are stopped by
	// the poll loop, which reads the batch state on every poll.
	halted := entity.IsBatchStateHalted(batch.State)

	// Attempt here is the queue's redelivery counter for this message — how
	// many times it has been handed out without being acked — and has nothing
	// to do with a path's build attempt. A value above 1 means an earlier
	// processing of this same message died part-way (nacked or crashed), and
	// only then can a linked build be missing its signal — a first delivery
	// has not published any — so only then is the repair worth the republishes
	// it would otherwise scatter (see ensureSignal).
	recovering := delivery.Attempt() > 1

	for _, entry := range set.Paths {
		if entry.Status.IsTerminal() {
			continue
		}

		if entry.Status == entity.SpeculationPathStatusPending && !halted {
			if err := c.startPath(ctx, store, batch, entry); err != nil {
				return err
			}
			continue
		}

		if entry.Status == entity.SpeculationPathStatusPending {
			metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		}

		if recovering {
			if err := c.ensureSignal(ctx, store, batch, entry); err != nil {
				return err
			}
		}
	}

	return nil
}

// startPath triggers the build for a pending path and records it.
//
// The base is the path's own — the dependencies it assumes will succeed, in its
// order. This is the behavioral heart of speculation: dependencies the path
// assumes will fail are absent from the base, which is what lets the head be
// verified before they resolve.
//
// The write order is Trigger, then the Build record, then the link, then the
// signal — each write makes the previous one reachable. The Build record gives
// the runner's ID a home, the link makes the build findable from the path the
// caller holds, and the signal puts the poll loop behind it. The link is the
// idempotency point: a redelivery that finds it re-publishes the signal and
// stops, and a concurrent dispatch that loses the link's first-insert race
// hands both builds to the poll loop, which keeps the one the link names (see
// the lost-race branch below).
//
// A crash between the Trigger and the link orphans the build — redelivery
// cannot find it and triggers again — the accepted cost of not being able to
// name a build before the runner mints its ID.
//
// TODO: pass an idempotency key derived from (path ID, attempt) once
// BuildRunner.Trigger accepts one, so a retry re-attaches to the existing build
// instead of orphaning it. Only the runner can close this window, because the
// build exists before anything here can write it down.
func (c *Controller) startPath(ctx context.Context, store storage.Storage, batch entity.Batch, entry entity.SpeculationPathEntry) error {
	existing, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
	switch {
	case err == nil:
		// Already dispatched and named; all that can be missing is the signal,
		// and a re-publish is deduped by the queue when it is not.
		metrics.NamedCounter(c.metricsScope, opName, "already_dispatched", 1)
		return c.publishBuildSignal(ctx, existing.BuildID, batch.Queue)
	case !errors.Is(err, storage.ErrNotFound):
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	base, err := c.loadBase(ctx, store, entry.Path)
	if err != nil {
		return err
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to build runner for batch %s: %w", batch.ID, err)
	}

	// metadata is nil until a caller-supplied source materializes (e.g.
	// requester / ticket pulled off the originating LandRequest).
	buildID, err := buildRunner.Trigger(ctx, base, batch, nil)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to trigger build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	build := entity.Build{
		ID:      buildID.ID,
		BatchID: batch.ID,
		PathID:  entry.ID,
		Attempt: entry.Attempt,
		Status:  entity.BuildStatusAccepted,
	}

	// Persist the initial Build snapshot so the buildsignal poll loop has a
	// row to Update against. ErrAlreadyExists is benign — a redelivery
	// of this message after a previous successful Create.
	if err := store.GetBuildStore().Create(ctx, build); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to record build %s: %w", buildID.ID, err)
	}

	link := entity.PathBuild{Queue: batch.Queue, PathID: entry.ID, Attempt: entry.Attempt, BuildID: buildID.ID}
	if err := store.GetPathBuildStore().Create(ctx, link); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			// Another dispatch named this attempt first. Its build is the one
			// the rest of the system will watch; ours is surplus. Hand both to
			// the poll loop: it keeps the build the link names and stops the
			// other one — and the winner's signal must be sent from here too,
			// because the dispatch that won may have died before sending it,
			// and acking this message would retire the redelivery that would
			// otherwise repair that.
			metrics.NamedCounter(c.metricsScope, opName, "lost_dispatch_race", 1)
			c.logger.Infow("lost the dispatch race; handing both builds to the poll loop",
				"batch_id", batch.ID,
				"path_id", entry.ID,
				"attempt", entry.Attempt,
				"surplus_build_id", buildID.ID,
			)
			if err := c.publishBuildSignal(ctx, buildID.ID, batch.Queue); err != nil {
				return err
			}
			winner, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
			if err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
				return fmt.Errorf("failed to look up the winning build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
			}
			return c.publishBuildSignal(ctx, winner.BuildID, batch.Queue)
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to link path %s attempt %d to build %s: %w", entry.ID, entry.Attempt, buildID.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "build_triggered", 1)
	c.logger.Infow("triggered build for speculation path",
		"batch_id", batch.ID,
		"path_id", entry.ID,
		"attempt", entry.Attempt,
		"build_id", buildID.ID,
		"base_size", len(base),
	)

	return c.publishBuildSignal(ctx, buildID.ID, batch.Queue)
}

// ensureSignal re-publishes the build signal for a live path whose build is
// already linked. Called only on redeliveries.
//
// It closes the one crack in the "every linked build gets a signal" invariant:
// a dispatch that dies between writing the link and publishing the signal
// leaves a build no poll chain will ever watch. The message it was processing
// was never acked, so it comes back — but by then the entry may have moved on
// to building (an observation found the link) or cancelling (the run called it
// off), or its batch may have halted: states no start would touch. Without
// this, such a build runs unobserved forever and its path never settles.
//
// The republish uses the build ID as the message ID, so it dedups against the
// original signal whenever that signal was actually sent. The dedup horizon is
// the queue's GC of consumed rows, which is why this runs only on redelivery:
// scattered over every ordinary dispatch, late republishes would now and then
// slip past dedup and fork a second, redundant poll chain for a healthy build.
func (c *Controller) ensureSignal(ctx context.Context, store storage.Storage, batch entity.Batch, entry entity.SpeculationPathEntry) error {
	link, err := store.GetPathBuildStore().Get(ctx, entry.ID, entry.Attempt)
	if errors.Is(err, storage.ErrNotFound) {
		// Nothing was dispatched for this attempt; there is no build to watch.
		return nil
	}
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to look up build for path %s attempt %d: %w", entry.ID, entry.Attempt, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "signal_ensured", 1)
	c.logger.Debugw("re-published build signal on redelivery",
		"batch_id", batch.ID,
		"path_id", entry.ID,
		"attempt", entry.Attempt,
		"build_id", link.BuildID,
	)
	return c.publishBuildSignal(ctx, link.BuildID, batch.Queue)
}

// loadBase loads the batches the path is stacked on top of.
//
// Which dependencies those are is the path's own to say — see
// SpeculationPath.Base — so this only resolves the IDs it is
// given. Nothing about assumptions is interpreted here.
func (c *Controller) loadBase(ctx context.Context, store storage.Storage, path entity.SpeculationPath) ([]entity.Batch, error) {
	deps := path.Base()
	if len(deps) == 0 {
		return nil, nil
	}

	base := make([]entity.Batch, 0, len(deps))
	for _, depID := range deps {
		b, err := store.GetBatchStore().Get(ctx, depID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return nil, fmt.Errorf("failed to get dependency batch %s of path %s: %w", depID, path.ID(), err)
		}
		base = append(base, b)
	}
	return base, nil
}

// publishBuildSignal hands a build off to the poll loop.
//
// Only the runner's build ID travels, and it is also the partition key: the
// poll loop writes nothing but that build's own record, so there is nothing to
// serialize across builds, and successive polls of one build stay ordered
// because they share the key. Partitioning by batch instead would put every
// path of a head behind whichever of its builds polls slowest.
//
// The build ID is the message ID too, with no cause: a build is handed off
// once in its life, so a repeat hand-off for the same build is meant to dedup
// away while the original signal is still in the queue's un-GC'd window (see
// publish.IntentID).
func (c *Controller) publishBuildSignal(ctx context.Context, buildID, queue string) error {
	payload, err := entity.BuildID{ID: buildID, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize build ID: %w", err)
	}

	if err := publish.Message(ctx, c.registry, topickey.TopicKeyBuildSignal, publish.IntentID(buildID), payload, buildID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to buildsignal: %w", err)
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
