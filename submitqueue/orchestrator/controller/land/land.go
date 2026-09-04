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

// Package land implements the trigger stage for the asynchronous land. It
// consumes a batch ready to land, builds the full land request from the
// batch's member requests (one step per request, in Contains order), and
// publishes it to runway's land queue using the batch id as the client-owned
// correlation id. Runway performs the land out of process and publishes the
// result to the land-signal queue, which the landsignal stage consumes and
// correlates back to the batch by that id.
package land

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	strategypb "github.com/uber/submitqueue/api/base/landstrategy/protopb"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/base/landstrategy"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Controller handles land queue messages. Implements consumer.Controller.
//
// It loads the batch and its member requests, assembles the full land request
// (one step per member request, in Contains order, each carrying that request's
// change and land strategy), and publishes it to runway's land queue. Runway
// performs the land out of process and returns the result on the land-signal
// queue; the landsignal stage consumes it and transitions the batch. This
// controller therefore performs no state transition itself.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	stores         storage.Factory
	registry       consumer.TopicRegistry
	runwayTopicKey consumer.TopicKey
	topicKey       consumer.TopicKey
	consumerGroup  string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new land controller for the orchestrator.
// runwayTopicKey is the runway-owned topic this controller publishes land
// requests to (TopicKeyLand).
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	runwayTopicKey consumer.TopicKey,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:         logger.Named("land_controller"),
		metricsScope:   scope.SubScope("land_controller"),
		stores:         stores,
		registry:       registry,
		runwayTopicKey: runwayTopicKey,
		topicKey:       topicKey,
		consumerGroup:  consumerGroup,
	}
}

// Process publishes the full land request to runway. Returns nil to ack
// (success), or error to nack/reject.
//
// Error classification: deserialize and storage failures are non-retryable
// (reject to DLQ). The publish to runway is retryable — it is the hand-off that
// keeps the land alive, so a transient enqueue blip should replay rather than
// strand the batch.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	// The payload's queue must match the batch's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if bid.Queue != "" && bid.Queue != batch.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}

	c.logger.Infow("received land event",
		"batch_id", batch.ID,
		"queue", batch.Queue,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit halted batches (terminal or cancelling): no land should be
	// kicked off for a batch that will not proceed. Unlike the old synchronous
	// land there is no terminal re-fan-out here — the landsignal stage owns the
	// state transition and fan-out once runway's result returns, so a redelivery
	// at this stage simply acks.
	if entity.IsBatchStateHalted(batch.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping land for halted batch",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	// Build the full payload runway needs to land the batch. The batch id is
	// the client-owned correlation id, so a redelivery republishes the same id
	// and runway dedupes on it; the result is matched straight back to the batch.
	req, err := c.buildLandRequest(ctx, store, batch)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to build land request for batch %s: %w", batch.ID, err)
	}

	// Report that the members are landing before the request goes out, so a
	// publish that fails nacks with nothing announced, and a crash between the
	// two re-publishes under the same occurrence and dedupes.
	if err := corerequest.PublishBatchLogs(ctx, c.registry, batch.Queue, batch.Contains,
		entity.RequestStatusLanding, batch.ID, map[string]string{"batch_id": batch.ID},
	); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request log for batch %s: %w", batch.ID, err)
	}

	if err := c.publish(ctx, c.runwayTopicKey, req, batch.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to runway land: %w", err)
	}

	c.logger.Infow("published land to runway",
		"batch_id", batch.ID,
		"steps", len(req.Steps),
		"topic_key", c.runwayTopicKey,
	)

	return nil // Success - message will be acked
}

// buildLandRequest loads the batch's member requests and assembles the runway
// land request: one LandStep per request, in Contains order, attributed by
// request id and carrying that request's change and land strategy.
func (c *Controller) buildLandRequest(ctx context.Context, store storage.Storage, batch entity.Batch) (*runwaymq.LandRequest, error) {
	steps := make([]*runwaymq.LandStep, 0, len(batch.Contains))
	for _, requestID := range batch.Contains {
		request, err := store.GetRequestStore().Get(ctx, requestID)
		if err != nil {
			return nil, fmt.Errorf("failed to get request %s: %w", requestID, err)
		}
		steps = append(steps, &runwaymq.LandStep{
			StepId:   request.ID,
			Change:   &changepb.Change{Uris: request.Change.URIs},
			Strategy: toProtoStrategy(request.LandStrategy),
		})
	}
	return &runwaymq.LandRequest{
		Id:        batch.ID,
		QueueName: batch.Queue,
		Steps:     steps,
	}, nil
}

// toProtoStrategy maps the shared landstrategy.Strategy entity to the
// proto Strategy enum carried on the wire. An unknown strategy maps to DEFAULT,
// letting runway apply the queue's configured default.
func toProtoStrategy(s landstrategy.Strategy) strategypb.Strategy {
	switch s {
	case landstrategy.StrategyRebase:
		return strategypb.Strategy_REBASE
	case landstrategy.StrategySquashRebase:
		return strategypb.Strategy_SQUASH_REBASE
	case landstrategy.StrategyMerge:
		return strategypb.Strategy_MERGE
	default:
		return strategypb.Strategy_DEFAULT
	}
}

// publish serializes the runway land request and publishes it to the given
// topic key, partitioned by queue.
//
// The correlation ID is the message ID with no cause: a batch is asked to land
// once, so a redelivery that re-asks is meant to dedup rather than have Runway
// land the same batch twice.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, req *runwaymq.LandRequest, partitionKey string) error {
	payload, err := runwaymq.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to serialize land request: %w", err)
	}

	if err := publish.Message(ctx, c.registry, key, publish.IntentID(req.GetId()), payload, partitionKey); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "land"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
