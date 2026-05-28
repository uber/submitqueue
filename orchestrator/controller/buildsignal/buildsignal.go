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

// Package buildsignal implements the build poll loop. Each message carries
// a Build; the controller calls BuildRunner.Status, writes the latest
// status to the BuildStore, publishes the batch ID to TopicKeySpeculate
// so the state machine re-evaluates, and re-publishes itself via
// PublishAfter when the build has not yet reached a terminal state. Each
// buildID partitions independently, so slow polls on one build do not
// block others. A webhook-capable backend can publish into this same
// topic — the controller cannot tell a poll-driven message from a push.
package buildsignal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/extension/storage"
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

// Controller consumes build signal messages, polls BuildRunner.Status,
// persists the result, and drives the polling loop.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	buildRunner   buildrunner.BuildRunner
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
	store storage.Storage,
	buildRunner buildrunner.BuildRunner,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("buildsignal_controller"),
		metricsScope:  scope.SubScope("buildsignal_controller"),
		store:         store,
		buildRunner:   buildRunner,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process polls the build's current status, persists it, publishes the
// batch ID to speculate so the state machine re-evaluates, and re-publishes
// a delayed message back to this topic when the build is still in flight.
// Returns nil to ack (success), or error to nack/reject.
//
// Error classification: deserialize, Status, UpdateStatus, and the speculate
// publish stay non-retryable — they reject straight to DLQ on the first
// failure, where the operational republish path is the recovery mechanism.
// Only the PublishAfter self-reschedule is retryable: it is the poll loop's
// heartbeat and runs only after status/persist/speculate have all succeeded,
// so a transient enqueue blip nacks and replays (up to MaxAttempts) rather
// than silently stalling the build, then still falls through to DLQ if it
// persists.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	buildID, err := entity.BuildIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed.
		return fmt.Errorf("failed to deserialize build ID: %w", err)
	}

	// Only the build ID travels on the queue; load the full Build from
	// storage, which is the single source of truth for its BatchID and the
	// snapshot the poll loop updates.
	build, err := c.store.GetBuildStore().Get(ctx, buildID.ID)
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

	status, _, err := c.buildRunner.Status(ctx, buildID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "status_errors", 1)
		return fmt.Errorf("failed to get status for build %s: %w", buildID.ID, err)
	}

	// Short-circuit if the batch is already halted (terminal OR cancelling).
	// Speculate is already idempotent on terminal, but skipping the publish
	// avoids noise. For Cancelling batches the cancel controller owns the
	// terminal write and the downstream fan-out, so further pipeline work
	// would race against it; silent ack is the only safe action.
	batch, err := c.store.GetBatchStore().Get(ctx, build.BatchID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", build.BatchID, err)
	}
	if entity.IsBatchStateHalted(batch.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping buildsignal publish for halted batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	build.Status = status

	if err := c.store.GetBuildStore().UpdateStatus(ctx, build.ID, status); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to update status for build %s: %w", build.ID, err)
	}

	// Re-evaluate the batch state machine with the latest build status.
	if err := c.publishBatchID(ctx, consumer.TopicKeySpeculate, build.BatchID, msg.PartitionKey); err != nil {
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

	delayMs := pollDelay(status)
	metrics.NamedCounter(c.metricsScope, opName, "rescheduled", 1, metrics.NewTag("status", string(status)))
	if err := c.publishBuild(ctx, c.topicKey, build, delayMs); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		// Retryable: this is the poll loop's heartbeat. A transient enqueue
		// failure should nack and replay rather than DLQ the only message
		// keeping this build's status loop alive.
		return errs.NewRetryableError(fmt.Errorf("failed to re-publish to buildsignal: %w", err))
	}

	c.logger.Debugw("rescheduled build status poll",
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

// publishBuild publishes a build's ID to the topic identified by key. delayMs > 0
// uses PublishAfter; otherwise it uses Publish. Only the identifier travels on
// the queue — the consumer reloads the full Build from storage.
func (c *Controller) publishBuild(ctx context.Context, key consumer.TopicKey, build entity.Build, delayMs int64) error {
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

	publisher := q.Publisher()
	if delayMs > 0 {
		return publisher.PublishAfter(ctx, topicName, msg, delayMs)
	}
	return publisher.Publish(ctx, topicName, msg)
}

// publishBatchID publishes a batch ID to the topic identified by key.
func (c *Controller) publishBatchID(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
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
