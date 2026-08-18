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

package batch

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/extension/counter"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles batch queue messages.
// It consumes validated requests, mints a batch for each, and hands it to the dependency-analysis stage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	counters      counter.Factory
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

const opName = "process"

// counterDomainBatch names the per-queue sequence that mints batch IDs. The batch
// ID is built independently as "<queue>/batch/<counter_value>", so the domain is a
// sequence name only and never appears in the ID.
const counterDomainBatch = "batch"

// NewController creates a new batch controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	counters counter.Factory,
	stores storage.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("batch_controller"),
		metricsScope:  scope.SubScope("batch_controller"),
		registry:      registry,
		counters:      counters,
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a batch delivery from the queue.
// Mints a batch for the request and hands it to the dependency-analysis topic,
// which decides whether that batch is the one that enrols the request.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	// Deserialize request ID from payload
	rid, err := entity.RequestIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize request ID: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: rid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rid.Queue, err)
	}

	// Fetch request from storage
	request, err := store.GetRequestStore().Get(ctx, rid.ID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", rid.ID, err)
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if rid.Queue != "" && rid.Queue != request.Queue {
		metrics.NamedCounter(c.metricsScope, opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", rid.Queue, request.Queue, request.ID)
	}

	c.logger.Infow("received batch event",
		"request_id", request.ID,
		"queue", request.Queue,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit if the request has been halted — either it already reached a
	// terminal state, or the cancel controller has recorded a cancellation intent
	// (RequestStateCancelling). A halted request must never spawn a new batch.
	// If cancellation races with an attempt already initializing below, speculate re-checks the contained request state before starting work.
	if entity.IsRequestStateHalted(request.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping batch for halted request",
			"request_id", request.ID,
			"state", string(request.State),
		)
		return nil
	}

	// TODO: if capacity is full, wait here for other requests to accumulate to batch them together, or include a request into an existing batch if it's not too late.

	// Generate a globally unique batch ID.
	queueCounter, err := c.counters.For(counter.Config{QueueName: request.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "counter_errors", 1)
		return fmt.Errorf("failed to resolve counter for queue=%s: %w", request.Queue, err)
	}
	seq, err := queueCounter.Next(ctx, counterDomainBatch)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "counter_errors", 1)
		return fmt.Errorf("failed to generate batch ID for queue=%s: %w", request.Queue, err)
	}

	// Dependencies stay empty here: what the batch must serialize behind is
	// resolved by the dependency-analysis stage, which fills them in as it
	// promotes the batch out of Creating.
	batch := entity.Batch{
		ID:           fmt.Sprintf("%s/batch/%d", request.Queue, seq),
		Queue:        request.Queue,
		Contains:     []string{request.ID},
		Dependencies: []string{},
		State:        entity.BatchStateCreating,
		Version:      1,
	}

	// Creating is inert: it is in neither ActiveBatchStates nor
	// DependencyBatchStates, and nothing is associated with it yet, so a batch
	// abandoned here is a row nobody can reach.
	if err := store.GetBatchStore().Create(ctx, batch); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "batch_store_errors", 1)
		return fmt.Errorf("failed to create batch in batch store: %w", err)
	}

	// Reported once the batch row exists, which is what makes it true: a batch is
	// being built for this request. Deduped per (request, status), so a
	// redelivery that mints another batch re-emits it harmlessly.
	logEntry := entity.NewRequestStatusLog(request.Queue, request.ID, entity.RequestStatusBatching, request.Version, "", map[string]string{
		"batch_id": batch.ID,
	})
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID, ""); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "request_log_errors", 1)
		return fmt.Errorf("failed to publish request log for request %s: %w", request.ID, err)
	}

	// Hand the batch to dependency analysis, which decides whether this batch is
	// the one that enrols the request and, if so, resolves what it must serialize
	// behind and promotes it. The batch is durable first: the consumer reloads it
	// by ID, so a message that overtook its own write would find nothing.
	//
	// A failure here leaves an unreachable Creating row and the retry mints
	// another. Enrolment is decided downstream, so duplicates here cost storage,
	// not correctness.
	if err := c.publishToDependencyAnalysis(ctx, batch); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		metrics.NamedCounter(c.metricsScope, opName, "batch_abandoned_creating", 1)
		return fmt.Errorf("failed to publish batch ID to dependency-analysis topic: %w", err)
	}

	c.logger.Infow("published batch to dependency-analysis topic",
		"batch_id", batch.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"topic_key", topickey.TopicKeyDependencyAnalysis,
	)

	return nil // Success - message will be acked
}

// publishToDependencyAnalysis hands the batch to the next stage, partitioned by
// its queue so analysis of one queue stays serial.
//
// The message ID is the bare batch ID, with no cause: a batch is handed over
// once in its life, so a redelivery that re-sends it is meant to be dropped.
func (c *Controller) publishToDependencyAnalysis(ctx context.Context, batch entity.Batch) error {
	payload, err := entity.BatchID{ID: batch.ID, Queue: batch.Queue}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize batch ID: %w", err)
	}

	if err := publish.Message(ctx, c.registry, topickey.TopicKeyDependencyAnalysis,
		publish.IntentID(batch.ID), payload, batch.Queue); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "batch"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
