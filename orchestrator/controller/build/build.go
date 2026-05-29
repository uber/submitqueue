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
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/build"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles build queue messages.
// It consumes batches, triggers builds, and publishes scheduled builds to the build signal stage (which processes build results).
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	buildManager  build.BuildManager
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
	buildManager build.BuildManager,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("build_controller"),
		metricsScope:  scope.SubScope("build_controller"),
		store:         store,
		buildManager:  buildManager,
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

	// Assemble the changes to build from the batch's requests.
	changes := make([]entity.BuildChange, 0, len(batch.Contains))
	for _, reqID := range batch.Contains {
		req, err := c.store.GetRequestStore().Get(ctx, reqID)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
			return fmt.Errorf("failed to get request %s for batch %s: %w", reqID, batch.ID, err)
		}
		changes = append(changes, entity.BuildChange{
			Change: req.Change,
			Action: entity.ChangeActionValidate,
		})
	}

	// Trigger the build with the configured build manager.
	buildID, status, err := c.buildManager.Trigger(ctx, batch.Queue, changes)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "trigger_errors", 1)
		return fmt.Errorf("failed to trigger build for batch %s: %w", batch.ID, err)
	}

	build := entity.Build{
		ID:      buildID,
		BatchID: batch.ID,
		Status:  status,
	}

	// Publish build to build signal topic
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

// publish publishes a build to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, build entity.Build) error {
	payload, err := build.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize build: %w", err)
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
