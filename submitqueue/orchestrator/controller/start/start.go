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
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles start queue messages.
// It consumes requests, persists them to the request store, and publishes to the
// validate stage. The request write is idempotent on retries. URI claiming and
// duplicate detection are performed downstream by the validate controller (which
// claims each URI in the change store with its provider details and consults the
// request store for liveness). Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new start controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("start_controller"),
		metricsScope:  scope.SubScope("start_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a request delivery from the queue.
// Persists the request and publishes to validate. Returns nil to ack (success),
// or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	landRequest, err := entity.LandRequestFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize land request: %w", err)
	}

	request := entity.Request{
		ID:           landRequest.ID,
		Queue:        landRequest.Queue,
		Change:       landRequest.Change,
		LandStrategy: landRequest.LandStrategy,
		State:        entity.RequestStateStarted,
		Version:      1,
	}

	c.logger.Infow("received land request event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"land_strategy", string(request.LandStrategy),
		"change_uris", request.Change.URIs,
		"change_count", len(request.Change.URIs),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Persist request to storage. ErrAlreadyExists means a queue redelivery of the same
	// request_id (an at-least-once retry of THIS message), not a cross-request collision.
	if err := c.store.GetRequestStore().Create(ctx, request); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Record the "new" status in the request log.
	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusStarted, request.Version, "", nil)
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request log: %w", err)
	}

	if err := c.publish(ctx, topickey.TopicKeyValidate, request.ID, request.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to validate: %w", err)
	}

	c.logger.Infow("published request to validate",
		"request_id", request.ID,
		"topic_key", topickey.TopicKeyValidate,
	)

	return nil
}

// publish publishes a request ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, requestID string, partitionKey string) error {
	rid := entity.RequestID{ID: requestID}
	payload, err := rid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request ID: %w", err)
	}

	msg := entityqueue.NewMessage(requestID, payload, partitionKey, nil)

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
