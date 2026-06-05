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

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/buildrunner"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles build queue messages.
// It consumes batches, triggers builds, and publishes scheduled builds to the build signal stage (which processes build results).
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
// Deserializes the batch, triggers a build, and publishes a build entity to the build signal topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

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

	// If the batch is halted (terminal OR cancelling), skip triggering CI and
	// ack. This is a forward-progress controller: per the cancel design, the
	// speculate controller owns cancelling any in-flight Build and driving the
	// batch to its terminal state, so the build stage simply short-circuits
	// while speculate does the work. No external CI is ever kicked off.
	if entity.IsBatchStateHalted(batch.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping build for halted batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// Assemble base (dependency batches in order) and head (this batch).
	base, err := c.collectChanges(ctx, batch.Dependencies)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to assemble base changes for batch %s: %w", batch.ID, err)
	}
	head, err := c.collectChanges(ctx, []string{batch.ID})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to assemble head changes for batch %s: %w", batch.ID, err)
	}

	// Trigger the build with the queue's build runner. metadata is nil
	// until a caller-supplied source materializes (e.g. requester / ticket
	// pulled off the originating LandRequest).
	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: batch.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to build runner for batch %s: %w", batch.ID, err)
	}
	buildID, err := buildRunner.Trigger(ctx, base, head, nil)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to trigger build for batch %s: %w", batch.ID, err)
	}

	build := entity.Build{
		ID:              buildID.ID,
		BatchID:         batch.ID,
		SpeculationPath: entity.SpeculationPathInfo{Base: append([]string{}, batch.Dependencies...)},
		Status:          entity.BuildStatusAccepted,
	}

	// Persist the initial Build snapshot so the buildsignal poll loop has a
	// row to UpdateStatus against. ErrAlreadyExists is benign — a redelivery
	// of this message after a previous successful Create.
	if err := c.store.GetBuildStore().Create(ctx, build); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to persist build %s: %w", build.ID, err)
	}

	// Hand off to the buildsignal poll loop; it calls Status, updates the
	// persisted Build, publishes to speculate, and re-publishes itself via
	// PublishAfter until terminal.
	if err := c.publish(ctx, consumer.TopicKeyBuildSignal, build); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to buildsignal: %w", err)
	}

	c.logger.Infow("published build to buildsignal",
		"batch_id", batch.ID,
		"build_id", build.ID,
		"status", string(build.Status),
		"topic_key", consumer.TopicKeyBuildSignal,
	)

	return nil // Success - message will be acked
}

// collectChanges loads each batch by ID and concatenates the Change values
// from its contained requests in batch order. Used to build the base
// (dependency batches) and head (this batch) inputs to BuildRunner.Trigger.
func (c *Controller) collectChanges(ctx context.Context, batchIDs []string) ([]entity.Change, error) {
	if len(batchIDs) == 0 {
		return nil, nil
	}
	var changes []entity.Change
	for _, bID := range batchIDs {
		b, err := c.store.GetBatchStore().Get(ctx, bID)
		if err != nil {
			return nil, fmt.Errorf("failed to get batch %s: %w", bID, err)
		}
		for _, reqID := range b.Contains {
			req, err := c.store.GetRequestStore().Get(ctx, reqID)
			if err != nil {
				return nil, fmt.Errorf("failed to get request %s for batch %s: %w", reqID, bID, err)
			}
			changes = append(changes, req.Change)
		}
	}
	return changes, nil
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
