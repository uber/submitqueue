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

package dlq

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// batchController is the DLQ reconciler for batch-scoped pipeline stages
// (score, speculate, build, merge, conclude). All five topics carry a
// BatchID payload, so this controller is registered five times — one per
// topic, each with the matching DLQ topic key and consumer group.
//
// On each delivery the controller decodes the BatchID, transitions the batch
// to BatchStateFailed (idempotent if already halted), and fans out by
// transitioning each member request to RequestStateError. The fan-out exists
// because conclude — which normally drives request state from batch state —
// will not run for a DLQ'd batch.
type batchController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify batchController implements consumer.Controller at compile time.
var _ consumer.Controller = (*batchController)(nil)

// NewDLQBatchController builds a DLQ controller for a batch-scoped topic.
// topicKey must be the DLQ topic key (typically TopicKey(primary)).
func NewDLQBatchController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &batchController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for a batch-scoped topic.
func (c *batchController) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	bid, err := entity.BatchIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode batch id from dlq payload: %w", err)
	}
	if bid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty batch id")
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"batch_id", bid.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := failBatch(ctx, c.store, c.registry, c.logger, bid.ID, dmeta["dlq.last_error"]); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
		return err
	}

	metrics.NamedCounter(c.metricsScope, opName, "reconciled", 1)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *batchController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *batchController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *batchController) ConsumerGroup() string {
	return c.consumerGroup
}
