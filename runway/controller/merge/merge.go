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

// Package merge consumes committing merge requests from Runway's merge queue. A
// request asks Runway to apply an ordered sequence of merge steps onto the target
// branch and commit the result.
//
// It deserializes the MergeRequest off the queue, runs the configured merger,
// and publishes a MergeResult with produced revisions to the merge-signal queue.
package merge

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/runway/controller/internal/signal"
	"github.com/uber/submitqueue/runway/extension/merger"
	"go.uber.org/zap"
)

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// Controller handles merge queue messages.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	merger         merger.Merger
	registry       consumer.TopicRegistry
	topicKey       consumer.TopicKey
	signalTopicKey consumer.TopicKey
	consumerGroup  string
}

// Params are the parameters for creating a new merge controller.
type Params struct {
	TopicKey       consumer.TopicKey
	SignalTopicKey consumer.TopicKey
	ConsumerGroup  string
	Merger         merger.Merger
	Registry       consumer.TopicRegistry

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new merge controller for the runway service.
func NewController(p Params) *Controller {
	return &Controller{
		logger:         p.Logger.Named("merge_controller"),
		metricsScope:   p.Scope.SubScope("merge_controller"),
		merger:         p.Merger,
		registry:       p.Registry,
		topicKey:       p.TopicKey,
		signalTopicKey: p.SignalTopicKey,
		consumerGroup:  p.ConsumerGroup,
	}
}

// Process deserializes the merge request and logs it. Returns nil to ack, or an
// error to nack.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	request := &runwaymq.MergeRequest{}
	if err := runwaymq.Unmarshal(msg.Payload, request); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: a malformed payload will never deserialize on retry.
		return fmt.Errorf("failed to deserialize merge request: %w", err)
	}

	c.logger.Infow("received merge request",
		"id", request.Id,
		"queue_name", request.QueueName,
		"step_count", len(request.Steps),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	if c.merger == nil {
		return fmt.Errorf("merger is required")
	}

	result, err := c.merger.Merge(ctx, request)
	if errors.Is(err, merger.ErrConflict) {
		result = &runwaymq.MergeResult{
			Id:      request.Id,
			Outcome: runwaypb.Outcome_FAILED,
			Reason:  err.Error(),
		}
	} else if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "merger_errors", 1)
		return fmt.Errorf("failed to merge request: %w", err)
	}

	if err := signal.PublishMergeResult(ctx, c.registry, c.signalTopicKey, result, msg.PartitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish merge result: %w", err)
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
