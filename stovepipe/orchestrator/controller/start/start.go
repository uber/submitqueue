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

package start

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	entity "github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap"
)

// Controller handles start queue messages. It is the pipeline entry point: it
// deserializes the gateway-produced ingest request and forwards it to the validate
// stage, propagating the envelope partition key for ordering.
//
// The ordering key is decided once at ingestion and carried through the pipeline.
//
// Currently a forwarding stub. Per the Stovepipe workflow RFC, start will also record
// each commit as `unknown` (keyed by SHA, making ingest idempotent across the webhook
// and poll producers) and emit status + log events. Until a commit store exists it
// forwards the full request downstream rather than a thin ID reference — submitqueue
// persists the request here and forwards only the ID, but Stovepipe has no store yet.

var _ consumer.Controller = (*Controller)(nil)

type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Params are the parameters for creating a new start controller.
type Params struct {
	Registry      consumer.TopicRegistry
	TopicKey      consumer.TopicKey
	ConsumerGroup string

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new start controller for the orchestrator.
func NewController(p Params) *Controller {
	return &Controller{
		logger:        p.Logger.Named("start_controller"),
		metricsScope:  p.Scope.SubScope("start_controller"),
		registry:      p.Registry,
		topicKey:      p.TopicKey,
		consumerGroup: p.ConsumerGroup,
	}
}

// Process deserializes the ingest request and forwards it to the validate stage.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	request, err := entity.IngestRequestFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed regardless of retry count.
		return fmt.Errorf("failed to deserialize ingest request: %w", err)
	}

	// The ordering key lives on the message envelope, stamped by the gateway at
	// ingestion; the controller propagates it verbatim to the next stage.
	partitionKey := msg.PartitionKey
	if partitionKey == "" {
		metrics.NamedCounter(c.metricsScope, opName, "missing_partition_key", 1)
		return fmt.Errorf("ingest request %s is missing a partition key (must be stamped by the producer)", request.ID)
	}

	c.logger.Infow("received ingest request",
		"spid", request.ID,
		"queue", request.Queue,
		"change_uris", request.Change.URIs,
		"change_count", len(request.Change.URIs),
		"attempt", delivery.Attempt(),
		"partition_key", partitionKey,
	)

	// Core logic to be added here:
	// - Record each commit as `unknown` (keyed by SHA)
	// - Emit status + log events

	if err := c.publish(ctx, topickey.TopicKeyValidate, request, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to validate: %w", err)
	}

	c.logger.Infow("published ingest request to validate",
		"spid", request.ID,
		"topic_key", topickey.TopicKeyValidate,
	)

	return nil
}

func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, request entity.IngestRequest, partitionKey string) error {
	payload, err := request.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize ingest request: %w", err)
	}

	msg := entityqueue.NewMessage(request.ID, payload, partitionKey, nil)

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
	return "start"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
