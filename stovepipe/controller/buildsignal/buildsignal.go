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

// Package buildsignal holds the buildsignal-stage queue controller. It consumes
// BuildSignal messages (a build id), polls the build-runner until the build
// reaches a terminal status, persists that status, and — once terminal —
// publishes the build id to record. See
// doc/rfc/stovepipe/steps/buildsignal.md.
package buildsignal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Poll delays for non-terminal statuses. Vars (not consts) so tests can
// shorten them; the server always uses the defaults.
//
// TODO: move these behind a queueconfig-style extension so operators can
// tune poll cadence per queue without a code change.
var (
	// PollDelayAcceptedMs is the delay before the next Status call while the
	// build is queued by the runner but has not started executing.
	PollDelayAcceptedMs int64 = 5000
	// PollDelayRunningMs is the delay before the next Status call while the
	// build is executing.
	PollDelayRunningMs int64 = 2000
)

// Controller consumes BuildSignal messages, polls the build-runner toward a
// terminal status, persists the result, and either reschedules itself or
// publishes the build id to record. Implements consumer.Controller.
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

// _opName is the metric operation name shared by every emit in this file.
const _opName = "buildsignal"

// NewController creates a new buildsignal controller.
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
		logger:        logger.Named("buildsignal_controller"),
		metricsScope:  scope.SubScope("buildsignal_controller"),
		store:         store,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the build referenced by the delivery, polls its runner for
// the latest status, persists a real transition, and either reschedules a
// poll or, once terminal, publishes the build id to record. Returns nil to
// ack (success) or an error to nack (retry) / reject (DLQ).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, _opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	sig := &stovepipemq.BuildSignal{}
	if err := stovepipemq.Unmarshal(msg.Payload, sig); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize build signal: %w", err)
	}

	build, err := c.loadBuild(ctx, sig.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	request, err := c.loadRequest(ctx, build.RequestID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	// The request is done (record already ran, or the head was superseded);
	// stop polling.
	if request.State.IsTerminal() {
		return nil
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: request.Queue})
	if err != nil {
		// A queue with no registered builder is a config error.
		return fmt.Errorf("failed to resolve build runner for queue %s: %w", request.Queue, err)
	}

	status, _, err := buildRunner.Status(ctx, entity.BuildID{ID: build.ID})
	if err != nil {
		return fmt.Errorf("failed to poll status for build %s: %w", build.ID, err)
	}

	effective, err := c.reconcile(ctx, build, status)
	if err != nil {
		return err
	}

	if effective.IsTerminal() {
		if err := c.publishRecord(ctx, build.ID, request.ID); err != nil {
			return fmt.Errorf("failed to publish record for build %s: %w", build.ID, err)
		}
		c.logger.Infow("build reached terminal status",
			"build_id", build.ID,
			"request_id", request.ID,
			"status", string(effective),
		)
		return nil
	}

	delayMs := pollDelay(effective)
	if err := c.publishBuildSignal(ctx, build.ID, delayMs); err != nil {
		return errs.NewRetryableError(fmt.Errorf("failed to reschedule poll for build %s: %w", build.ID, err))
	}
	c.logger.Debugw("rescheduled build status poll",
		"build_id", build.ID,
		"status", string(effective),
		"delay_ms", delayMs,
	)
	return nil
}

// reconcile persists a real status transition and returns the status that
// should drive the rest of Process: the polled status when persisted (or
// already unchanged), or the stored status when a stored terminal status is
// write-once-protected against a differing poll.
func (c *Controller) reconcile(ctx context.Context, build entity.Build, status entity.BuildStatus) (entity.BuildStatus, error) {
	if status == build.Status {
		return build.Status, nil
	}

	// Terminal is write-once: a later poll of a flaky backend must never
	// overwrite an already-committed terminal status.
	if build.Status.IsTerminal() {
		return build.Status, nil
	}

	newVersion := build.Version + 1
	updated := build
	updated.Status = status
	if err := c.store.GetBuildStore().Update(ctx, updated, build.Version, newVersion); err != nil {
		return "", fmt.Errorf("failed to persist status for build %s: %w", build.ID, err)
	}
	return status, nil
}

// loadBuild returns the build for id.
func (c *Controller) loadBuild(ctx context.Context, id string) (entity.Build, error) {
	return loader.ByID(ctx, id, c.store.GetBuildStore().Get, "build")
}

// loadRequest returns the request for id.
func (c *Controller) loadRequest(ctx context.Context, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, c.store.GetRequestStore().Get, "request")
}

// pollDelay returns the delay before the next Status call for a non-terminal status.
func pollDelay(status entity.BuildStatus) int64 {
	switch status {
	case entity.BuildStatusRunning:
		return PollDelayRunningMs
	default:
		// Accepted and any unknown non-terminal status.
		return PollDelayAcceptedMs
	}
}

// publishRecord publishes buildID to the record stage, partitioned by
// requestID.
func (c *Controller) publishRecord(ctx context.Context, buildID, requestID string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: buildID})
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	msg := entityqueue.NewMessage(buildID, payload, requestID, nil)
	return c.publish(ctx, stovepipemq.TopicKeyRecord, msg, 0)
}

// publishBuildSignal re-publishes buildID to buildsignal after delayMs,
// partitioned by build id so each build's poll loop runs in its own
// partition. A fresh message, not a nack — polling is not failure.
func (c *Controller) publishBuildSignal(ctx context.Context, buildID string, delayMs int64) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: buildID})
	if err != nil {
		return fmt.Errorf("failed to serialize build signal: %w", err)
	}
	msg := entityqueue.NewMessage(buildID, payload, buildID, nil)
	return c.publish(ctx, stovepipemq.TopicKeyBuildSignal, msg, delayMs)
}

// publish sends msg to the queue registered for key, using PublishAfter when
// delayMs > 0 and Publish otherwise.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, msg entityqueue.Message, delayMs int64) error {
	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}
	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}
	if delayMs > 0 {
		return q.Publisher().PublishAfter(ctx, topicName, msg, delayMs)
	}
	return q.Publisher().Publish(ctx, topicName, msg)
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
