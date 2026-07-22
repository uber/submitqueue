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
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// buildSignalController is the DLQ reconciler for the buildsignal topic. Its
// payload carries a BuildID, so reconciliation needs an extra hop: look up
// the Build to recover its BatchID, then fan out via failBatch.
//
// The build itself is left in whatever non-terminal state the build runner
// last reported. Fixing the build entity is not useful here — the
// pipeline's source of truth for "did this batch finish" is the batch state,
// and that is what gates the gateway response and conclude.
type buildSignalController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify buildSignalController implements consumer.Controller at compile time.
var _ consumer.Controller = (*buildSignalController)(nil)

// NewDLQBuildSignalController builds a DLQ controller for the buildsignal topic.
func NewDLQBuildSignalController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &buildSignalController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for the buildsignal topic.
func (c *buildSignalController) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	bid, err := entity.BuildIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode build id from dlq payload: %w", err)
	}
	if bid.ID == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty build id")
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"build_id", bid.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	build, err := c.store.GetBuildStore().Get(ctx, bid.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// The build was never persisted (e.g. the build controller crashed
			// before Create). There is no batch to reconcile from this signal —
			// any associated batch should be reconciled from its own DLQ.
			c.logger.Warnw("dlq reconcile: build not found, skipping",
				"build_id", bid.ID,
			)
			metrics.NamedCounter(c.metricsScope, opName, "build_not_found", 1)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, opName, "build_store_errors", 1)
		return fmt.Errorf("failed to get build %s: %w", bid.ID, err)
	}

	if build.BatchID == "" {
		// Defensive: a build without a batch is malformed and there is nothing
		// to fan out to. Log and ack so the DLQ does not grow unbounded.
		c.logger.Errorw("dlq reconcile: build has empty batch id, skipping",
			"build_id", bid.ID,
		)
		metrics.NamedCounter(c.metricsScope, opName, "build_missing_batch", 1)
		return nil
	}

	if err := failBatch(ctx, c.store, c.registry, c.logger, build.BatchID, dmeta["dlq.last_error"]); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
		return err
	}

	metrics.NamedCounter(c.metricsScope, opName, "reconciled", 1)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *buildSignalController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *buildSignalController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *buildSignalController) ConsumerGroup() string {
	return c.consumerGroup
}
