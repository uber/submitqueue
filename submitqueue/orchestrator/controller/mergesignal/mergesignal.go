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

// Package mergesignal consumes merge results from runway's merge-signal queue,
// correlates them to the batch by the echoed id, and transitions the batch to a
// terminal state — Succeeded when runway merged the batch, Failed when it could
// not — then fans the batch out to conclude (so member requests pick up the
// outcome) and speculate (so dependents can re-plan). Like mergeconflictsignal
// it is purely result-driven — runway pushes the result, so there is no poll
// loop or self-reschedule.
package mergesignal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles mergesignal queue messages. Implements consumer.Controller.
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

// NewController creates a new mergesignal controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("mergesignal_controller"),
		metricsScope:  scope.SubScope("mergesignal_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process consumes a runway merge result and advances or fails the batch.
// Returns nil to ack, or error to nack/reject.
//
// A not-merged verdict is an expected outcome of the merge, not a failure: the
// batch is driven to terminal Failed inline and the message is acked. Only
// infrastructure faults — deserialize, storage, the state transition, and the
// fan-out publishes — return an error and reject to the DLQ, where the batch is
// reconciled to Failed.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// The runway result carries full data (it crosses the service boundary). Its
	// id is the batch id echoed back, so correlate straight to the batch.
	result := &runwaymq.MergeResult{}
	if err := runwaymq.Unmarshal(msg.Payload, result); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize merge result: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, result.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", result.Id, err)
	}

	c.logger.Infow("received merge signal",
		"batch_id", batch.ID,
		"merged", result.Outcome == runwaypb.Outcome_SUCCEEDED,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Cancelling: the cancel path (via speculate) owns the terminal write and the
	// downstream fan-out for a batch the user asked to cancel. Silently ack — do
	// not transition (a racing terminal merge result must not override the
	// cancel) and do not fan out.
	if batch.State == entity.BatchStateCancelling {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_cancelling", 1)
		return nil
	}

	// Idempotency: a previous delivery already transitioned this batch to a
	// terminal state. Re-fan-out in case that attempt missed the downstream
	// publishes, then ack.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_terminal", 1)
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	var newState entity.BatchState
	if result.Outcome == runwaypb.Outcome_SUCCEEDED {
		newState = entity.BatchStateSucceeded
		c.logger.Infow("merged batch",
			"batch_id", batch.ID,
			"steps", result.Steps,
		)
	} else {
		metrics.NamedCounter(c.metricsScope, opName, "not_merged", 1)
		newState = entity.BatchStateFailed
		c.logger.Warnw("batch merge failed",
			"batch_id", batch.ID,
			"reason", result.Reason,
		)
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, newState); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "state_update_errors", 1)
		return fmt.Errorf("failed to transition batch %s to %s: %w", batch.ID, newState, err)
	}
	batch.Version = newVersion
	batch.State = newState

	return c.fanout(ctx, batch.ID, batch.Queue)
}

// fanout publishes the batch ID to conclude (so requests are updated) and to
// speculate (so dependents can re-evaluate now that this batch is done).
func (c *Controller) fanout(ctx context.Context, batchID, partitionKey string) error {
	if err := c.publish(ctx, topickey.TopicKeyConclude, batchID, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "publish_conclude_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	if err := c.publish(ctx, topickey.TopicKeySpeculate, batchID, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "publish_speculate_errors", 1)
		return fmt.Errorf("failed to publish to speculate: %w", err)
	}
	return nil
}

// publish publishes a batch ID to the given topic key, partitioned by queue.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
	payload, err := entity.BatchID{ID: batchID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	msg := entityqueue.NewMessage(batchID, payload, partitionKey, nil)

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
	return "mergesignal"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
