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

// Package mergeconflictcheck consumes dry-run merge-conflict check requests from
// Runway's merge-conflict-check queue. A request asks whether an ordered sequence
// of merge steps can be applied cleanly onto the merge target.
//
// The controller obtains a Merger for the request's merge target, calls
// CheckMergeability, and publishes the MergeResult to the
// merge-conflict-check-signal queue. A merge conflict is an expected outcome
// (ack + publish FAILED result), not an infrastructure error.
package mergeconflictcheck

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/runway/extension/merger"
	"go.uber.org/zap"
)

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// Controller handles merge-conflict-check queue messages.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	mergerFactory merger.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Params are the parameters for creating a new merge-conflict-check controller.
type Params struct {
	TopicKey      consumer.TopicKey
	ConsumerGroup string

	MergerFactory merger.Factory
	Registry      consumer.TopicRegistry

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new merge-conflict-check controller for the runway service.
func NewController(p Params) *Controller {
	return &Controller{
		logger:        p.Logger.Named("mergeconflictcheck_controller"),
		metricsScope:  p.Scope.SubScope("mergeconflictcheck_controller"),
		mergerFactory: p.MergerFactory,
		registry:      p.Registry,
		topicKey:      p.TopicKey,
		consumerGroup: p.ConsumerGroup,
	}
}

// Process deserializes the merge request, performs a dry-run merge check, and
// publishes the result. Returns nil to ack, or an error to nack.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	request := &runwaymq.MergeRequest{}
	if err := runwaymq.Unmarshal(msg.Payload, request); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize merge request: %w", err)
	}

	c.logger.Infow("received merge-conflict-check request",
		"id", request.Id,
		"queue_name", request.QueueName,
		"step_count", len(request.Steps),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	m, err := c.mergerFactory.For(merger.Config{QueueName: request.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "factory_errors", 1)
		return fmt.Errorf("failed to create merger for queue %s: %w", request.GetQueueName(), err)
	}

	result, err := m.CheckMergeability(ctx, request)
	if err != nil {
		if !errors.Is(err, merger.ErrConflict) {
			metrics.NamedCounter(c.metricsScope, opName, "check_errors", 1)
			return fmt.Errorf("failed to check mergeability for %s: %w", request.GetId(), err)
		}
		metrics.NamedCounter(c.metricsScope, opName, "merge_conflicts", 1)
		c.logger.Infow("merge conflict detected",
			"id", request.GetId(),
			"queue_name", request.GetQueueName(),
		)
		result = &runwaymq.MergeResult{
			Id:      request.GetId(),
			Outcome: runwaypb.Outcome_FAILED,
			Reason:  err.Error(),
		}
	}

	if err := c.publish(ctx, runwaymq.TopicKeyMergeConflictCheckSignal, result, msg.PartitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish merge-conflict-check result for %s: %w", request.GetId(), err)
	}

	return nil
}

// publish serializes a MergeResult and publishes it to the given signal topic.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, result *runwaymq.MergeResult, partitionKey string) error {
	payload, err := runwaymq.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to serialize merge result: %w", err)
	}

	msg := entityqueue.NewMessage(result.GetId(), payload, partitionKey, nil)

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
	return "merge-conflict-check"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
