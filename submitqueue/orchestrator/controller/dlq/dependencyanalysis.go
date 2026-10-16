// Copyright (c) 2026 Uber Technologies, Inc.
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

package dlq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corebatch "github.com/uber/submitqueue/submitqueue/orchestrator/core/batch"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

// dependencyAnalysisController is the DLQ reconciler for the dependency-analysis
// topic.
//
// It does not share the generic batch reconciler because the batch on this
// message need not own its requests. The batch stage mints a batch per
// delivery, so one request can have several batches, and dependency analysis is
// where one of them is chosen. The generic reconciler fails every member
// request, so a dead-lettered duplicate would write Error over a request whose
// chosen batch is still live, and that batch would go on to land it.
//
// So the batch's state decides:
//
//   - Creating, with a request already carried by another batch past Creating:
//     a duplicate. Nothing is touched; it stays inert in Creating, as the
//     primary controller leaves it.
//   - Creating otherwise, Created, or Failed: this batch owns its requests and
//     nothing downstream will advance it, so it is failed and fanned out.
//   - Any later state: a downstream stage took the batch over after this message
//     was written, and owns its outcome.
type dependencyAnalysisController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify dependencyAnalysisController implements consumer.Controller at compile time.
var _ consumer.Controller = (*dependencyAnalysisController)(nil)

// NewDLQDependencyAnalysisController builds the DLQ controller for the
// dependency-analysis topic. topicKey must be the DLQ topic key (typically
// TopicKey(primary)).
func NewDLQDependencyAnalysisController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores orchstorage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &dependencyAnalysisController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single dead-lettered dependency-analysis message.
func (c *dependencyAnalysisController) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	bid, err := sqmq.UnmarshalBatchID(primaryTopicKey(c.topicKey), msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode batch id from dlq payload: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, bid.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "queue_identity_errors", 1)
		return nil
	}
	if bid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty batch id")
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: bid.Queue})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}

	lastError, failureMeta := failureContext(delivery)
	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"batch_id", bid.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.logger.Warnw("dlq reconcile: batch not found, skipping", "batch_id", bid.ID)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get batch %s: %w", bid.ID, err)
	}

	switch batch.State {
	case entity.BatchStateCreating:
		carrier, err := c.findOtherBatchCarryingRequests(ctx, store, batch)
		if err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "batch_lookup_errors", 1)
			return err
		}
		if carrier != "" {
			metrics.NamedCounter(c.metricsScope, opName, "skipped_duplicate", 1)
			c.logger.Infow("dlq reconcile: batch is a duplicate, its requests are carried by another",
				"batch_id", batch.ID,
				"carried_by", carrier,
			)
			return nil
		}
	case entity.BatchStateCreated, entity.BatchStateFailed:
	default:
		metrics.NamedCounter(c.metricsScope, opName, "skipped_owned_downstream", 1)
		c.logger.Infow("dlq reconcile: batch has moved past dependency analysis, leaving it to its current stage",
			"batch_id", batch.ID,
			"state", string(batch.State),
		)
		return nil
	}

	if _, err := failBatch(ctx, store, c.registry, c.logger, batch.ID, lastError, failureMeta); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
		return err
	}
	return nil
}

// findOtherBatchCarryingRequests returns a batch other than this one that got
// past Creating with one of this batch's requests, or the empty string. It is
// the same test the primary controller uses to abandon a duplicate.
func (c *dependencyAnalysisController) findOtherBatchCarryingRequests(ctx context.Context, store orchstorage.Storage, batch entity.Batch) (string, error) {
	for _, requestID := range batch.Contains {
		batches, _, err := corebatch.FindByRequestID(ctx, store, requestID)
		if err != nil {
			return "", err
		}
		for _, other := range batches {
			if other.ID != batch.ID && other.State != entity.BatchStateCreating {
				return other.ID, nil
			}
		}
	}
	return "", nil
}

// Name returns the controller name for logging and metrics.
func (c *dependencyAnalysisController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *dependencyAnalysisController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *dependencyAnalysisController) ConsumerGroup() string {
	return c.consumerGroup
}
