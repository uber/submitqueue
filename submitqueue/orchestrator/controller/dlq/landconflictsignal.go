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
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// landConflictSignalController is the DLQ reconciler for the
// landconflictsignal topic. Its payload carries a runway
// LandResult whose id is the request id echoed back, so
// reconciliation fails that request directly via failRequest.
type landConflictSignalController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify landConflictSignalController implements consumer.Controller at compile time.
var _ consumer.Controller = (*landConflictSignalController)(nil)

// NewDLQLandConflictSignalController builds a DLQ controller for the
// landconflictsignal topic.
func NewDLQLandConflictSignalController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &landConflictSignalController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for the landconflictsignal topic.
func (c *landConflictSignalController) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	result := &runwaymq.LandResult{}
	if err := runwaymq.Unmarshal(msg.Payload, result); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode land conflict check result from dlq payload: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: result.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", result.GetQueueName(), err)
	}

	lastError, failureMeta := failureContext(delivery)
	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", result.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := failRequest(ctx, store, c.registry, c.logger, result.Id, lastError, failureMeta); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "reconcile_errors", 1)
		return err
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *landConflictSignalController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *landConflictSignalController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *landConflictSignalController) ConsumerGroup() string {
	return c.consumerGroup
}
