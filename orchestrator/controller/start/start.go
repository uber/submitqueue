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
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	corerequest "github.com/uber/submitqueue/core/request"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/changestore"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles start queue messages.
// It consumes requests, persists them to the request store, claims their URIs in the change
// store, and publishes to the validate stage. Both writes are idempotent on retries; the
// duplicate-detection check itself is performed downstream by the validate controller.
// Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	changeStore   changestore.ChangeStore
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
	changeStore changestore.ChangeStore,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("start_controller"),
		metricsScope:  scope.SubScope("start_controller"),
		store:         store,
		changeStore:   changeStore,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a request delivery from the queue.
// Deserializes the request, persists it, and publishes to the validate topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	c.metricsScope.Counter("received").Inc(1)

	msg := delivery.Message()

	// Deserialize land request from gateway
	landRequest, err := entity.LandRequestFromBytes(msg.Payload)
	if err != nil {
		c.metricsScope.Counter("deserialize_errors").Inc(1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize land request: %w", err)
	}

	// Construct the full versioned Request entity with orchestrator-owned fields
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
	// Cross-request URI overlap is detected downstream in the validate controller.
	if err := c.store.GetRequestStore().Create(ctx, request); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		c.metricsScope.Counter("storage_errors").Inc(1)
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Claim this request's URIs in the change store. INSERT IGNORE makes this idempotent
	// on retries with the same request_id. The validate controller reads from this store
	// to detect cross-request URI overlap.
	if err := c.claimURIs(ctx, request); err != nil {
		c.metricsScope.Counter("change_store_errors").Inc(1)
		return fmt.Errorf("failed to claim URIs for request %s: %w", request.ID, err)
	}

	// Record the "new" status in the request log
	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusStarted, request.Version, "", nil)
	// Using request.ID as the partition key to ensure ordering of log entries for the same request
	// and parallel processing of log entries for different requests.
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		c.metricsScope.Counter("request_log_errors").Inc(1)
		return fmt.Errorf("failed to publish request log: %w", err)
	}

	// Publish to validate topic
	if err := c.publish(ctx, consumer.TopicKeyValidate, request.ID, request.Queue); err != nil {
		c.metricsScope.Counter("publish_errors").Inc(1)
		return fmt.Errorf("failed to publish to validate: %w", err)
	}

	c.logger.Infow("published request to validate",
		"request_id", request.ID,
		"topic_key", consumer.TopicKeyValidate,
	)

	c.metricsScope.Counter("processed").Inc(1)

	return nil // Success - message will be acked
}

// claimURIs persists one ChangeRecord per URI in the request. Each Create call is
// independent; the change store's per-PK idempotency makes the loop safe under
// queue redelivery (same (Queue, URI, RequestID) is a no-op on retry). Different
// requests with overlapping URIs do NOT collide on insert; the validate controller
// queries the change store to detect that overlap.
func (c *Controller) claimURIs(ctx context.Context, request entity.Request) error {
	now := time.Now().UnixMilli()
	for _, uri := range request.Change.URIs {
		record := entity.ChangeRecord{
			URI:       uri,
			RequestID: request.ID,
			Queue:     request.Queue,
			CreatedAt: now,
			UpdatedAt: now,
			Version:   1,
		}
		if err := c.changeStore.Create(ctx, record); err != nil {
			return fmt.Errorf("failed to claim uri=%s for request %s: %w", uri, request.ID, err)
		}
	}
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
