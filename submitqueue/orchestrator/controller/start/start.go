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
	"slices"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storage "github.com/uber/submitqueue/submitqueue/extension/storage"
	corerequest "github.com/uber/submitqueue/submitqueue/orchestrator/core/request"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

// Controller persists start requests and repairs their log and validate hand-off.
// Primary and final-DLQ deliveries preserve the persisted request's current owner.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
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
	stores orchstorage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("start_controller"),
		metricsScope:  scope.SubScope("start_controller"),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a request delivery from the queue.
// Persists the request and publishes to validate. Returns nil to ack (success),
// or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	landRequest, err := sqmq.UnmarshalLandRequest(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize land request: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, landRequest.Queue); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}

	if landRequest.ID == "" {
		return fmt.Errorf("land request id is required")
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: landRequest.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", landRequest.Queue, err)
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

	request, err = createOrLoadRequest(ctx, store.GetRequestStore(), request)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}

	switch request.State {
	case entity.RequestStateStarted:
	case entity.RequestStateValidated, entity.RequestStateBatched, entity.RequestStateProcessing, entity.RequestStateCancelling:
		return nil
	case entity.RequestStateLanded, entity.RequestStateError, entity.RequestStateCancelled:
		return corerequest.PublishTerminalRequestLog(ctx, c.registry, request)
	default:
		return fmt.Errorf("unsupported request state %q for request %s", request.State, request.ID)
	}

	logEntry := entity.NewRequestStatusLog(request.Queue, request.ID, entity.RequestStatusStarted, request.Version, "", nil)
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID, ""); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to publish request log: %w", err)
	}

	if err := c.publishToValidate(ctx, request); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to publish to validate: %w", err)
	}

	c.logger.Infow("published request to validate",
		"request_id", request.ID,
		"topic_key", topickey.TopicKeyValidate,
	)

	return nil
}

func createOrLoadRequest(ctx context.Context, store storage.RequestStore, initial entity.Request) (entity.Request, error) {
	if err := store.Create(ctx, initial); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return entity.Request{}, fmt.Errorf("failed to create request: %w", err)
	}
	// Create can lose to a delayed attempt or a later stage; only the persisted snapshot owns replay.
	request, err := store.Get(ctx, initial.ID)
	if err != nil {
		return entity.Request{}, fmt.Errorf("failed to load request after create: %w", err)
	}
	if request.ID != initial.ID || request.Queue != initial.Queue ||
		request.LandStrategy != initial.LandStrategy || !slices.Equal(request.Change.URIs, initial.Change.URIs) {
		return entity.Request{}, fmt.Errorf("conflicting immutable inputs for request %s in queue %q", initial.ID, initial.Queue)
	}
	return request, nil
}

// publishToValidate uses one stable intent ID so replay deduplicates the hand-off.
func (c *Controller) publishToValidate(ctx context.Context, request entity.Request) error {
	payload, err := sqmq.MarshalID(topickey.TopicKeyValidate, request.ID, request.Queue)
	if err != nil {
		return fmt.Errorf("failed to serialize request ID: %w", err)
	}

	if err := publish.Message(ctx, c.registry, topickey.TopicKeyValidate, publish.MessageParams{
		Tenant:       request.Queue,
		ID:           publish.IntentID(request.ID),
		Payload:      payload,
		PartitionKey: request.Queue,
	}); err != nil {
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
