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

// Package buildsignal implements the build poll loop. Each message names one
// build by the runner's own ID; the controller calls BuildRunner.Status, writes
// the latest status to that build's record, and wakes the speculate run for its
// batch, holding its delivery between polls while the build is still in
// flight.
//
// The poll loop is also where builds are stopped. It follows every build to a
// terminal state anyway, so on each poll it checks whether anything still
// wants the build running — the batch not halted, the path's current attempt,
// with a status that is not a stop, linked to this very build — and asks the
// runner to cancel when nothing does. That makes cancellation level-triggered:
// the speculate run records intent in the path set and nothing more, no cancel
// message exists to go stale, and a check that misses one poll is remade on
// the next.
//
// The path set is read here as that kill list, and never written — the
// speculate run stays its only writer, which is what lets a run hold one
// version of a head's paths across its whole decision without a poll
// invalidating it. The read must come from the primary: a stale replica read
// could report a wanted build unwanted, and a cancel is irreversible.
//
// Each build partitions independently, so slow polls on one build do not block
// another, and successive polls of one build stay ordered. A webhook-capable
// backend can publish into this same topic — the controller cannot tell a
// poll-driven message from a push.
package buildsignal

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/publish"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Poll delays for non-terminal statuses. Vars (not consts) so tests can
// shorten them; the orchestrator always uses the defaults.
//
// TODO: make these poll delays configurable per queue via the queueconfig
// extension instead of package-level vars, so operators can tune poll cadence
// without a code change.
var (
	// PollDelayAcceptedMs is the delay between Status calls while the build
	// is queued by the runner but has not started executing.
	PollDelayAcceptedMs int64 = 5000
	// PollDelayRunningMs is the delay between Status calls while the build
	// is executing.
	PollDelayRunningMs int64 = 2000
)

// opName is the metric operation name shared by every emit in this file.
const opName = "process"

// Controller consumes build signal messages, polls BuildRunner.Status,
// persists the result, and drives the polling loop.
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

// NewController creates a new build signal controller for the orchestrator.
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
		logger:        logger.Named("buildsignal_controller"),
		metricsScope:  scope.SubScope("buildsignal_controller"),
		stores:        stores,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process polls one attempt's build status, stops the build if nothing wants
// it running any more, persists the status, wakes the speculate run, and
// holds the delivery for the next poll while the build is still in flight.
// Returns nil to ack (success), or error to nack/reject.
//
// There is deliberately no short-circuit for halted batches. A cancelling batch
// reaches its terminal state only once its paths stop, and this loop is the
// only thing watching them stop — and, now, the thing stopping them: speculate
// marks a path cancelling, the next poll here asks the runner to cancel, and a
// later poll observes CI actually stop and records it. Skipping the work —
// including the hold — for a halted batch would leave every cancelled
// batch stranded in Cancelling forever.
//
// Error classification: deserialize, Status, the kill-list reads, the
// persistence writes, and the speculate publish stay non-retryable — they
// reject straight to DLQ on the first failure, where the operational republish
// path is the recovery mechanism. The Cancel call is best-effort instead:
// failing the message for it would kill the poll chain that is the only thing
// that will retry the cancel, so a failure is logged and the next poll remakes
// the whole check. The poll loop's continuation is a hold, not a publish: the
// framework postpones the delivery, and a failed postpone write lapses into a
// normal visibility-timeout redelivery, so the loop cannot stall on an
// enqueue.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	buildID, err := entity.BuildIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed.
		return fmt.Errorf("failed to deserialize build ID: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: buildID.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", buildID.Queue, err)
	}

	// Only the build ID travels on the queue; load the full Build from
	// storage, which is the single source of truth for its BatchID and the
	// snapshot the poll loop updates.
	build, err := store.GetBuildStore().Get(ctx, buildID.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get build %s: %w", buildID.ID, err)
	}

	c.logger.Debugw("polling build status",
		"build_id", build.ID,
		"batch_id", build.BatchID,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Load the batch first: it gives us the queue (needed to build the right
	// BuildRunner) and lets us short-circuit halted batches before polling.
	batch, err := store.GetBatchStore().Get(ctx, build.BatchID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", build.BatchID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if buildID.Queue != "" && buildID.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", buildID.Queue, batch.Queue, batch.ID)
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "status_errors", 1)
		return fmt.Errorf("failed to build runner for batch %s: %w", batch.ID, err)
	}

	status, _, err := buildRunner.Status(ctx, buildID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "status_errors", 1)
		return fmt.Errorf("failed to get status for build %s: %w", buildID.ID, err)
	}

	// Reconcile before recording: a build still running that nothing wants any
	// more is asked to stop. Best-effort by design — the reschedule below is
	// what guarantees the request is remade, so a failed Cancel must not fail
	// the message and take that reschedule with it.
	if !status.IsTerminal() {
		stop, err := c.unwanted(ctx, store, batch, build)
		if err != nil {
			return err
		}
		if stop {
			if err := buildRunner.Cancel(ctx, buildID); err != nil {
				metrics.NamedCounter(c.metricsScope, opName, "cancel_errors", 1)
				c.logger.Warnw("failed to cancel an unwanted build; the next poll retries",
					"build_id", build.ID,
					"batch_id", build.BatchID,
					"error", err,
				)
			} else {
				metrics.NamedCounter(c.metricsScope, opName, "build_cancelled", 1)
				c.logger.Infow("requested cancellation of a build nothing wants running",
					"build_id", build.ID,
					"batch_id", build.BatchID,
					"path_id", build.PathID,
					"attempt", build.Attempt,
				)
			}
		}
	}

	if status != build.Status {
		build.Status = status
		if err := store.GetBuildStore().Update(ctx, build); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to update status for build %s: %w", build.ID, err)
		}
	}

	// Wake the speculate run so it re-plans the queue with this result. It
	// reads the status from the record above rather than being told it, so a
	// duplicated or reordered signal costs nothing.
	if err := c.publishBatchID(ctx, topickey.TopicKeySpeculate, batch.ID, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to speculate: %w", err)
	}

	if status.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "terminal", 1, metrics.NewTag("status", string(status)))
		c.logger.Infow("build reached terminal status",
			"build_id", build.ID,
			"batch_id", build.BatchID,
			"status", string(status),
		)
		return nil
	}

	// Not terminal yet: hold the delivery so this same message redelivers
	// after the poll delay, without counting toward the retry limit.
	delayMs := pollDelay(status)
	metrics.NamedCounter(c.metricsScope, opName, "rescheduled", 1, metrics.NewTag("status", string(status)))
	delivery.Hold(delayMs)

	c.logger.Debugw("holding for next build status poll",
		"build_id", build.ID,
		"status", string(status),
		"delay_ms", delayMs,
	)
	return nil
}

// pollDelay returns the delay before the next Status call for a non-terminal status.
func pollDelay(status entity.BuildStatus) int64 {
	switch status {
	case entity.BuildStatusRunning:
		return PollDelayRunningMs
	default:
		// Accepted and any unknown non-terminal state.
		return PollDelayAcceptedMs
	}
}

// unwanted reports whether nothing wants this build running any more: its
// batch has halted, its path was called off or has moved to another attempt,
// or the attempt's link names a different build (this one lost a dispatch
// race). Every one of those conditions is permanent once true, so a stale read
// can only err toward keeping a build — never toward cancelling a wanted one.
//
// The two anomaly cases run the other way on purpose. A missing set or entry
// cannot legitimately happen — the dispatch read the entry out of the set to
// start this build, and entries are not removed — and a missing link cannot
// either, because the signal that led here is published after the link. Both
// therefore indicate store corruption, and since a cancel is irreversible, a
// corrupt kill list keeps the build rather than killing it; a halted batch is
// still caught by the first check, which needs none of those records.
func (c *Controller) unwanted(ctx context.Context, store storage.Storage, batch entity.Batch, build entity.Build) (bool, error) {
	if entity.IsBatchStateHalted(batch.State) {
		return true, nil
	}

	// A build without path coordinates predates per-path dispatch; the batch
	// state above is the only kill list it has.
	if build.PathID == "" {
		return false, nil
	}

	set, err := store.GetSpeculationPathSetStore().Get(ctx, batch.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "kill_list_anomalies", 1)
			c.logger.Warnw("build exists but its head has no path set; keeping the build",
				"build_id", build.ID,
				"batch_id", batch.ID,
			)
			return false, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return false, fmt.Errorf("failed to get path set for batch %s: %w", batch.ID, err)
	}

	entry, found := findEntry(set, build.PathID)
	if !found {
		metrics.NamedCounter(c.metricsScope, opName, "kill_list_anomalies", 1)
		c.logger.Warnw("build exists but its path is gone from the set; keeping the build",
			"build_id", build.ID,
			"batch_id", batch.ID,
			"path_id", build.PathID,
		)
		return false, nil
	}

	switch entry.Status {
	case entity.SpeculationPathStatusCancelling, entity.SpeculationPathStatusCancelled:
		return true, nil
	}

	// The path has moved on to a newer attempt; this build belongs to a
	// superseded one.
	if entry.Attempt != build.Attempt {
		return true, nil
	}

	link, err := store.GetPathBuildStore().Get(ctx, build.PathID, build.Attempt)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, opName, "kill_list_anomalies", 1)
			c.logger.Warnw("build exists but its attempt has no link; keeping the build",
				"build_id", build.ID,
				"path_id", build.PathID,
				"attempt", build.Attempt,
			)
			return false, nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return false, fmt.Errorf("failed to look up build for path %s attempt %d: %w", build.PathID, build.Attempt, err)
	}

	// The attempt's build is a different one: this build lost the dispatch
	// race, and nothing downstream will ever look at it.
	return link.BuildID != build.ID, nil
}

// findEntry returns the set's entry for a path ID.
func findEntry(set entity.SpeculationPathSet, pathID string) (entity.SpeculationPathEntry, bool) {
	for _, entry := range set.Paths {
		if entry.ID == pathID {
			return entry, true
		}
	}
	return entity.SpeculationPathEntry{}, false
}

// publishBatchID publishes a batch ID to the topic identified by key, stamped
// with and partitioned by the batch's queue, with a distinct message ID per
// publish (publish.UniqueID) so a later wake-up for the same batch is never
// deduplicated away.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, batchID string, queue string) error {
	payload, err := entity.BatchID{ID: batchID, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}
	return publish.Message(ctx, c.registry, key, publish.UniqueID(batchID), payload, queue)
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "buildsignal"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
