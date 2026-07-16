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

// Package build holds the build-stage queue controller. It consumes BuildRequest
// messages (a request id), reloads the Request, triggers the build-runner for
// the scope process already decided, persists a Build row, and publishes the
// build id to buildsignal.
package build

import (
	"context"
	"errors"
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

// Controller consumes BuildRequest messages, reloads the referenced Request,
// triggers a build for its already-decided scope, and publishes the resulting
// build id to buildsignal. Implements consumer.Controller.
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
const _opName = "build"

// NewController creates a new build controller.
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

// Process reloads the request referenced by the delivery, triggers a build for
// its decided scope, and publishes the build id to buildsignal. Returns nil to
// ack (success) or an error to nack (retry) / reject (DLQ).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, _opName)
	defer func() { op.Complete(retErr, metrics.LongLatencyBuckets) }()

	msg := delivery.Message()

	br := &stovepipemq.BuildRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, br); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize build request: %w", err)
	}

	request, err := c.loadRequest(ctx, br.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	// A redelivery after record already finished, or after process superseded
	// the head, must not start a fresh build.
	if request.State.IsTerminal() {
		return nil
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: request.Queue})
	if err != nil {
		// A queue with no registered builder is a config error.
		return fmt.Errorf("failed to resolve build runner for queue %s: %w", request.Queue, err)
	}

	// process decided the scope; build never re-derives incremental-vs-full.
	if request.BuildStrategy == entity.BuildStrategyUnknown {
		metrics.NamedCounter(c.metricsScope, _opName, "strategy_not_visible", 1)
		return errs.NewRetryableError(fmt.Errorf("request %s has no build strategy yet", request.ID))
	}
	baseURI := ""
	if request.BuildStrategy == entity.BuildStrategyIncrementalSinceGreen {
		baseURI = request.BaseURI
	}

	buildID, err := buildRunner.Trigger(ctx, baseURI, request.URI, nil)
	if err != nil {
		return fmt.Errorf("failed to trigger build for request %s: %w", request.ID, err)
	}

	build := entity.Build{
		ID:        buildID.ID,
		RequestID: request.ID,
		Status:    entity.BuildStatusAccepted,
		Version:   1,
	}
	if err := c.store.GetBuildStore().Create(ctx, build); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("failed to persist build %s: %w", build.ID, err)
	}

	if err := c.publishBuildSignal(ctx, build.ID); err != nil {
		return fmt.Errorf("failed to publish build signal for %s: %w", build.ID, err)
	}

	c.logger.Debugw("triggered build",
		"request_id", request.ID,
		"build_id", build.ID,
		"queue", request.Queue,
		"base_uri", baseURI,
	)
	return nil
}

// loadRequest returns the request for id.
func (c *Controller) loadRequest(ctx context.Context, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, c.store.GetRequestStore().Get, "request")
}

// publishBuildSignal publishes buildID to the buildsignal stage, partitioned by
// build id so each build's poll loop runs in its own partition.
func (c *Controller) publishBuildSignal(ctx context.Context, buildID string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: buildID})
	if err != nil {
		return fmt.Errorf("failed to serialize build signal: %w", err)
	}

	msg := entityqueue.NewMessage(buildID, payload, buildID, nil)

	q, ok := c.registry.Queue(stovepipemq.TopicKeyBuildSignal)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", stovepipemq.TopicKeyBuildSignal)
	}
	topicName, ok := c.registry.TopicName(stovepipemq.TopicKeyBuildSignal)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", stovepipemq.TopicKeyBuildSignal)
	}
	return q.Publisher().Publish(ctx, topicName, msg)
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
