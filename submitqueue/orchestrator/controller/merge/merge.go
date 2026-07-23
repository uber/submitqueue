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

// Package merge implements the trigger stage for the asynchronous merge. It
// consumes a batch ready to land, builds the full merge request from the
// batch's member requests (one step per request, in Contains order), and
// publishes it to runway's merge queue using the batch id as the client-owned
// correlation id. Runway performs the merge out of process and publishes the
// result to the merge-signal queue, which the mergesignal stage consumes and
// correlates back to the batch by that id.
package merge

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Controller handles merge queue messages. Implements consumer.Controller.
//
// It loads the batch and its member requests, assembles the full merge request
// (one step per member request, in Contains order, each carrying that request's
// change and land strategy), and publishes it to runway's merge queue. Runway
// performs the merge out of process and returns the result on the merge-signal
// queue; the mergesignal stage consumes it and transitions the batch. This
// controller therefore performs no state transition itself.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	store          storage.Storage
	registry       consumer.TopicRegistry
	runwayTopicKey consumer.TopicKey
	topicKey       consumer.TopicKey
	consumerGroup  string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new merge controller for the orchestrator.
// runwayTopicKey is the runway-owned topic this controller publishes merge
// requests to (TopicKeyMerge).
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	runwayTopicKey consumer.TopicKey,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:         logger.Named("merge_controller"),
		metricsScope:   scope.SubScope("merge_controller"),
		store:          store,
		registry:       registry,
		runwayTopicKey: runwayTopicKey,
		topicKey:       topicKey,
		consumerGroup:  consumerGroup,
	}
}

// Process publishes the full merge request to runway. Returns nil to ack
// (success), or error to nack/reject.
//
// Error classification: deserialize and storage failures are non-retryable
// (reject to DLQ). The publish to runway is retryable — it is the hand-off that
// keeps the merge alive, so a transient enqueue blip should replay rather than
// strand the batch.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	c.logger.Infow("received merge event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit halted batches (terminal or cancelling): no merge should be
	// kicked off for a batch that will not proceed. Unlike the old synchronous
	// merge there is no terminal re-fan-out here — the mergesignal stage owns the
	// state transition and fan-out once runway's result returns, so a redelivery
	// at this stage simply acks.
	if entity.IsBatchStateHalted(batch.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping merge for halted batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// Build the full payload runway needs to perform the merge. The batch id is
	// the client-owned correlation id, so a redelivery republishes the same id
	// and runway dedupes on it; the result is matched straight back to the batch.
	req, err := c.buildMergeRequest(ctx, batch)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to build merge request for batch %s: %w", batch.ID, err)
	}

	if err := c.publish(ctx, c.runwayTopicKey, req, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to runway merge: %w", err)
	}

	c.logger.Infow("published merge to runway",
		"batch_id", batch.ID,
		"steps", len(req.Steps),
		"topic_key", c.runwayTopicKey,
	)

	return nil // Success - message will be acked
}

// buildMergeRequest loads the batch's member requests and assembles the runway
// merge request: one MergeStep per request, in Contains order, attributed by
// request id and carrying that request's change and land strategy.
func (c *Controller) buildMergeRequest(ctx context.Context, batch entity.Batch) (*runwaymq.MergeRequest, error) {
	steps := make([]*runwaymq.MergeStep, 0, len(batch.Contains))
	for _, requestID := range batch.Contains {
		request, err := c.store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return nil, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		steps = append(steps, &runwaymq.MergeStep{
			StepId:   request.ID,
			Changes:  []*changepb.Change{{Uris: request.Change.URIs}},
			Strategy: toProtoStrategy(request.LandStrategy),
		})
	}
	return &runwaymq.MergeRequest{
		Id:        batch.ID,
		QueueName: batch.Queue,
		Steps:     steps,
	}, nil
}

// toProtoStrategy maps the shared mergestrategy.MergeStrategy entity to the
// proto Strategy enum carried on the wire. An unknown strategy maps to DEFAULT,
// letting runway apply the queue's configured default.
func toProtoStrategy(s mergestrategy.MergeStrategy) strategypb.Strategy {
	switch s {
	case mergestrategy.MergeStrategyRebase:
		return strategypb.Strategy_REBASE
	case mergestrategy.MergeStrategySquashRebase:
		return strategypb.Strategy_SQUASH_REBASE
	case mergestrategy.MergeStrategyMerge:
		return strategypb.Strategy_MERGE
	default:
		return strategypb.Strategy_DEFAULT
	}
}

// publish serializes the runway merge request and publishes it to the given
// topic key, partitioned by queue.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, req *runwaymq.MergeRequest, partitionKey string) error {
	payload, err := runwaymq.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to serialize merge request: %w", err)
	}

	msg := entityqueue.NewMessage(req.Id, payload, partitionKey, nil)

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
	return "merge"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
