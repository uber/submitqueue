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

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	coremetrics "github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	entityqueue "github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/changeprovider"
	"github.com/uber/submitqueue/extension/mergechecker"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles validate queue messages.
// It consumes requests, performs validation checks (merge conflicts, duplicate requests, etc.),
// and publishes to the batch stage. Validation logic is extensible to support additional checks.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	store          storage.Storage
	registry       consumer.TopicRegistry
	mergeChecker   mergechecker.MergeChecker
	changeProvider changeprovider.ChangeProvider
	topicKey       consumer.TopicKey
	consumerGroup  string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new validate controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	mergeChecker mergechecker.MergeChecker,
	changeProvider changeprovider.ChangeProvider,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:         logger.Named("validate_controller"),
		metricsScope:   scope.SubScope("validate_controller"),
		store:          store,
		registry:       registry,
		mergeChecker:   mergeChecker,
		changeProvider: changeProvider,
		topicKey:       topicKey,
		consumerGroup:  consumerGroup,
	}
}

// Process processes a validate delivery from the queue.
// Deserializes the request and publishes to the batch topic.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := coremetrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

	// Fetch request from storage
	request, err := c.store.GetRequestStore().Get(ctx, rid.ID)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", rid.ID, err)
	}

	c.logger.Infow("received validate event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Merge conflict check
	mergeResult, err := c.mergeChecker.Check(ctx, request.Queue, request.Change)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "merge_check_errors", 1)
		return fmt.Errorf("merge check failed: %w", err)
	}
	if !mergeResult.Mergeable {
		c.logger.Infow("request not mergeable",
			"request_id", request.ID,
			"queue", request.Queue,
			"reason", mergeResult.Reason,
		)
		coremetrics.NamedCounter(c.metricsScope, "process", "not_mergeable", 1)
		return errs.NewUserError(fmt.Errorf("request %s is not mergeable: %s", request.ID, mergeResult.Reason))
	}

	// Fetch change metadata
	changeInfos, err := c.changeProvider.Get(ctx, request.Change)
	if err != nil {
		c.logger.Errorw("failed to fetch change information",
			"request_id", request.ID,
			"change_uris", request.Change.URIs,
			"error", err,
		)
		coremetrics.NamedCounter(c.metricsScope, "process", "change_provider_errors", 1)
		return fmt.Errorf("failed to fetch change information: %w", err)
	}

	c.logger.Infow("fetched change information",
		"request_id", request.ID,
		"change_count", len(changeInfos),
		"total_files", totalFiles(changeInfos),
	)

	// Publish to batch topic
	if err := c.publish(ctx, consumer.TopicKeyBatch, request.ID, request.Queue); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "publish_errors", 1)
		return fmt.Errorf("failed to publish to batch: %w", err)
	}

	c.logger.Infow("published request to batch",
		"request_id", request.ID,
		"topic_key", consumer.TopicKeyBatch,
	)

	return nil // Success - message will be acked
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

// totalFiles returns the total number of files across all changeInfos.
func totalFiles(infos []changeprovider.ChangeInfo) int {
	total := 0
	for _, info := range infos {
		total += len(info.ChangedFiles)
	}
	return total
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
