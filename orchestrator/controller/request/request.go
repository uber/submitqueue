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

package request

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles request queue messages.
// It consumes requests, persists them to storage, and publishes to the validate stage.
// Implements consumer.Controller interface for integration with the consumer.
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

// NewController creates a new request controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("request_controller"),
		metricsScope:  scope.SubScope("request_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a request delivery from the queue.
// Deserializes the request and publishes to the validate topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	// Deserialize request entity
	request, err := entity.RequestFromBytes(msg.Payload)
	if err != nil {
		c.logger.Errorw("failed to deserialize request",
			"message_id", msg.ID,
			"partition_key", msg.PartitionKey,
			"attempt", delivery.Attempt(),
			"error", err,
		)
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize request: %w", err)
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

	// Persist request to storage (idempotent — ErrAlreadyExists means a retry)
	if err := c.store.GetRequestStore().Create(ctx, request); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		c.logger.Errorw("failed to create request in storage",
			"request_id", request.ID,
			"error", err,
		)
		c.metricsScope.Counter("storage_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to create request: %w", err))
	}

	// Record the "new" status in the request log
	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusNew, request.Version, "", nil)
	// Using request.ID as the partition key to ensure ordering of log entries for the same request
	// and parallel processing of log entries for different requests.
	if err := c.publishLog(ctx, logEntry, request.ID); err != nil {
		c.metricsScope.Counter("request_log_errors").Inc(1)
		return fmt.Errorf("failed to publish request log: %w", err)
	}

	// Publish to validate topic
	if err := c.publish(ctx, consumer.TopicKeyValidate, request); err != nil {
		c.logger.Errorw("failed to publish output",
			"request_id", request.ID,
			"topic_key", consumer.TopicKeyValidate,
			"error", err,
		)
		c.metricsScope.Counter("publish_errors").Inc(1)
		return errs.NewRetryableError(fmt.Errorf("failed to publish to validate: %w", err))
	}

	c.logger.Infow("published request to validate",
		"request_id", request.ID,
		"topic_key", consumer.TopicKeyValidate,
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// publish publishes a request to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, request entity.Request) error {
	payload, err := request.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request: %w", err)
	}

	msg := entityqueue.NewMessage(request.ID, payload, request.Queue, nil)

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

// publishLog publishes a request log entry to the log topic for async persistence.
func (c *Controller) publishLog(ctx context.Context, logEntry entity.RequestLog, partitionKey string) error {
	payload, err := logEntry.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request log: %w", err)
	}

	msg := entityqueue.NewMessage(logEntry.RequestID, payload, partitionKey, nil)

	q, ok := c.registry.Queue(consumer.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", consumer.TopicKeyLog)
	}

	topicName, ok := c.registry.TopicName(consumer.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", consumer.TopicKeyLog)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "request"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
