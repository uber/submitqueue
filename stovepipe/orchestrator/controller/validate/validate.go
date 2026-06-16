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

package validate

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	entity "github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap"
)

// Controller handles validate queue messages.
//
// This step will include any validation activities prior to adding the commit to a batch.
//
// Ordering key is decided once at ingestion and carried through the pipeline.
//
// Currently a forwarding stub.

var _ consumer.Controller = (*Controller)(nil)

type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Params are the parameters for creating a new validate controller.
type Params struct {
	Registry      consumer.TopicRegistry
	TopicKey      consumer.TopicKey
	ConsumerGroup string

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new validate controller for the orchestrator.
func NewController(p Params) *Controller {
	return &Controller{
		logger:        p.Logger.Named("validate_controller"),
		metricsScope:  p.Scope.SubScope("validate_controller"),
		registry:      p.Registry,
		topicKey:      p.TopicKey,
		consumerGroup: p.ConsumerGroup,
	}
}

// Process resolves the commit metadata and forwards the commit to the batch stage.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	event, err := entity.ChangeEventFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize change event: %w", err)
	}

	// The ordering key lives on the message envelope, stamped by the producer at
	// ingestion; the controller propagates it verbatim to the next stage.
	partitionKey := msg.PartitionKey
	if partitionKey == "" {
		metrics.NamedCounter(c.metricsScope, opName, "missing_partition_key", 1)
		return fmt.Errorf("change event for uri=%s is missing a partition key (must be stamped by the producer)", event.URI)
	}

	c.logger.Infow("received change event",
		"uri", event.URI,
		"attempt", delivery.Attempt(),
		"partition_key", partitionKey,
	)

	// Core Logic to be added here:
	// - Validation before publishing to batch
	// - Emit status + log events

	if err := c.publish(ctx, topickey.TopicKeyBatch, event, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to batch: %w", err)
	}

	c.logger.Infow("published commit to batch",
		"uri", event.URI,
		"topic_key", topickey.TopicKeyBatch,
	)

	return nil
}

func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, event entity.ChangeEvent, partitionKey string) error {
	payload, err := event.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize change event: %w", err)
	}

	msg := entityqueue.NewMessage(event.URI, payload, partitionKey, nil)

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
	return "validate"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
