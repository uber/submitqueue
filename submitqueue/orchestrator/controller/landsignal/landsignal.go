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

// Package landsignal consumes land results from runway's land-signal queue,
// correlates them to the batch by the echoed id, and transitions the batch to a
// terminal state — Succeeded when runway landed the batch, Failed when it could
// not — then fans the batch out to conclude (so member requests pick up the
// outcome) and speculate (so dependents can re-plan). Like landconflictsignal
// it is purely result-driven — runway pushes the result, so there is no poll
// loop or self-reschedule.
package landsignal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	corebatch "github.com/uber/submitqueue/submitqueue/core/batch"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles landsignal queue messages. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new landsignal controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("landsignal_controller"),
		metricsScope:  scope.SubScope("landsignal_controller"),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process consumes a runway land result and advances or fails the batch.
// Returns nil to ack, or error to nack/reject.
//
// A not-landed verdict is an expected outcome of the land, not a failure: the
// batch is driven to terminal Failed inline and the message is acked. Only
// infrastructure faults — deserialize, storage, the state transition, and the
// fan-out publishes — return an error and reject to the DLQ, where the batch is
// reconciled to Failed.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	// The runway result carries full data (it crosses the service boundary). Its
	// id is the batch id echoed back, so correlate straight to the batch.
	result := &runwaymq.LandResult{}
	if err := runwaymq.Unmarshal(msg.Payload, result); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize land result: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: result.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", result.GetQueueName(), err)
	}

	batch, err := store.GetBatchStore().Get(ctx, result.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", result.Id, err)
	}

	c.logger.Infow("received land signal",
		"batch_id", batch.ID,
		"landed", result.Outcome == runwaypb.Outcome_SUCCEEDED,
		"state", string(batch.State),
		"version", batch.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Cancelling: the cancel path (via speculate) owns the terminal write and the
	// downstream fan-out for a batch the user asked to cancel. Silently ack — do
	// not transition (a racing terminal land result must not override the
	// cancel) and do not fan out.
	if batch.State == entity.BatchStateCancelling {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_cancelling", 1)
		return nil
	}

	// A land failure's reason travels to conclude on the fan-out message, not on
	// the batch, so it reaches the request's terminal log without becoming durable
	// batch state. Empty on the landed path. Computed before the idempotency check
	// so a redelivered failed batch re-fans-out with its reason intact.
	var failureReason string
	if result.Outcome != runwaypb.Outcome_SUCCEEDED {
		failureReason = result.Reason
		if failureReason == "" {
			failureReason = "land failed"
		}
	}

	// Idempotency: a previous delivery already transitioned this batch to a
	// terminal state. Repair the membership record (a prior attempt may have
	// CAS'd without completing the record move), re-fan-out in case that
	// attempt missed the downstream publishes, then ack.
	if batch.State.IsTerminal() {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_terminal", 1)
		if err := corebatch.EnsureRecord(ctx, store, batch); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "state_update_errors", 1)
			return err
		}
		return c.fanout(ctx, batch.ID, batch.Queue, failureReason)
	}

	var newState entity.BatchState
	if result.Outcome == runwaypb.Outcome_SUCCEEDED {
		newState = entity.BatchStateSucceeded
		c.logger.Infow("landed batch",
			"batch_id", batch.ID,
			"steps", result.Steps,
		)
	} else {
		metrics.NamedCounter(c.metricsScope, opName, "not_landed", 1)
		newState = entity.BatchStateFailed
		c.logger.Warnw("batch land failed",
			"batch_id", batch.ID,
			"reason", result.Reason,
		)
	}

	batch, err = corebatch.Transition(ctx, store, batch, newState)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "state_update_errors", 1)
		return err
	}

	return c.fanout(ctx, batch.ID, batch.Queue, failureReason)
}

// fanout publishes the batch ID to conclude (so requests are updated) and to
// speculate (so dependents can re-evaluate now that this batch is done).
//
// Both messages name the land as their cause. Without it the speculate
// publish would reuse the bare batch ID, which the batch controller already
// published at creation, and the queue would drop this one as a duplicate for
// as long as that row survives — leaving dependents unwoken. Conclude is
// scoped the same way because speculate publishes there too when a batch goes
// terminal on its own; the two mean the same thing but are decided at
// different moments, so neither may swallow the other.
func (c *Controller) fanout(ctx context.Context, batchID, queue, failureReason string) error {
	var concludeMeta map[string]string
	if failureReason != "" {
		concludeMeta = map[string]string{topickey.MetadataKeyFailureReason: failureReason}
	}
	if err := c.publish(ctx, topickey.TopicKeyConclude, publish.IntentID(batchID, "conclude", "landed"), batchID, queue, concludeMeta); err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "publish_conclude_errors", 1)
		return fmt.Errorf("failed to publish to conclude: %w", err)
	}
	if err := c.publish(ctx, topickey.TopicKeySpeculate, publish.IntentID(batchID, "landed"), batchID, queue, nil); err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "publish_speculate_errors", 1)
		return fmt.Errorf("failed to publish to speculate: %w", err)
	}
	return nil
}

// publish publishes a batch ID to the given topic key under msgID, stamped
// with and partitioned by the batch's queue. metadata rides the message as
// side-band headers (nil for none).
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, msgID, batchID, queue string, metadata map[string]string) error {
	payload, err := entity.BatchID{ID: batchID, Queue: queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	if err := publish.MessageWithMetadata(ctx, c.registry, key, msgID, payload, queue, metadata); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "landsignal"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
