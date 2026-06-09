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

package merge

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/submitqueue/core/consumer"
	coremetrics "github.com/uber/submitqueue/core/metrics"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/pusher"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Controller handles merge queue messages. It loads every request in a batch,
// hands the resulting list of Changes to the configured Pusher, and
// transitions the batch to a terminal state based on the Pusher's outcome.
// After updating state it forwards the batch to conclude (so requests pick
// up the outcome) and to speculate (so downstream batches can re-plan).
//
// Conflicts are user-caused: the batch goes to BatchStateFailed and the
// queue message is acked. Any other Pusher error is treated as transient
// infra: the batch is left in place and the message is nacked.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	pushers       pusher.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new merge controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	pushers pusher.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("merge_controller"),
		metricsScope:  scope.SubScope("merge_controller"),
		store:         store,
		registry:      registry,
		pushers:       pushers,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process performs the merge for a batch and forwards it to conclude/speculate.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := coremetrics.Begin(c.metricsScope, "process")
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize batch ID: %w", err)
	}

	batch, err := c.store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "storage_errors", 1)
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

	// Cancelling intent: the cancel controller marked this batch as not landing
	// and handed it off to speculate. Silently ack — do not push (the inherent
	// push-already-committed race is acknowledged elsewhere) and do not fan out
	// (speculate owns the terminal write to Cancelled and the downstream
	// dependent / conclude publishes).
	if batch.State == entity.BatchStateCancelling {
		coremetrics.NamedCounter(c.metricsScope, "process", "skipped_cancelling", 1)
		return nil
	}

	// Idempotency: if the batch is already in a terminal state, a previous
	// attempt has already merged (or failed) — just re-fan-out the events
	// in case downstream stages missed them.
	if batch.State.IsTerminal() {
		coremetrics.NamedCounter(c.metricsScope, "process", "skipped_terminal", 1)
		return c.fanout(ctx, batch.ID, batch.Queue)
	}

	push, err := c.pushers.For(pusher.Config{QueueName: batch.Queue})
	if err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "push_errors", 1)
		return fmt.Errorf("failed to build pusher for batch %s: %w", batch.ID, err)
	}

	// Push a single batch today; the pusher resolves its changes itself. The
	// list parameter designs for a future merge-train.
	pushRes, pushErr := push.Push(ctx, []entity.Batch{batch})

	var newState entity.BatchState
	switch {
	case pushErr == nil:
		newState = entity.BatchStateSucceeded
		c.logger.Infow("merged batch",
			"batch_id", batch.ID,
			"outcomes", pushRes.Batches,
		)
	case errors.Is(pushErr, pusher.ErrConflict):
		coremetrics.NamedCounter(c.metricsScope, "process", "push_conflicts", 1)
		newState = entity.BatchStateFailed
		c.logger.Warnw("batch merge failed",
			"batch_id", batch.ID,
			"state", string(newState),
			"error", pushErr,
		)
	default:
		coremetrics.NamedCounter(c.metricsScope, "process", "push_errors", 1)
		return fmt.Errorf("push failed for batch %s: %w", batch.ID, pushErr)
	}

	newVersion := batch.Version + 1
	if err := c.store.GetBatchStore().UpdateState(ctx, batch.ID, batch.Version, newVersion, newState); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "state_update_errors", 1)
		return fmt.Errorf("failed to transition batch %s to %s: %w", batch.ID, newState, err)
	}
	batch.Version = newVersion
	batch.State = newState

	return c.fanout(ctx, batch.ID, batch.Queue)
}

// fanout publishes the batch ID to conclude (so requests are updated) and
// to speculate (so dependents can re-evaluate now that this batch is done).
func (c *Controller) fanout(ctx context.Context, batchID, partitionKey string) error {
	if err := c.publish(ctx, topickey.TopicKeyConclude, batchID, partitionKey); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "publish_conclude_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	if err := c.publish(ctx, topickey.TopicKeySpeculate, batchID, partitionKey); err != nil {
		coremetrics.NamedCounter(c.metricsScope, "process", "publish_speculate_errors", 1)
		return fmt.Errorf("failed to publish to speculate: %w", err)
	}
	return nil
}

// publish publishes a batch ID to the specified topic key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, batchID string, partitionKey string) error {
	bid := entity.BatchID{ID: batchID}
	payload, err := bid.ToBytes()
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
