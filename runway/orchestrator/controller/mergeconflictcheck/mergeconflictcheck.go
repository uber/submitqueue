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
// of merge steps applies cleanly onto the target branch without committing.
//
// Currently a parse-and-log stub: it deserializes the MergeRequest off the queue
// and logs it. The real check (attempt the merge without committing and publish a
// MergeResult to the merge-conflict-check-signal queue) is not wired yet.
package mergeconflictcheck

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// Controller handles merge-conflict-check queue messages.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Params are the parameters for creating a new merge-conflict-check controller.
type Params struct {
	TopicKey      consumer.TopicKey
	ConsumerGroup string

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new merge-conflict-check controller for the orchestrator.
func NewController(p Params) *Controller {
	return &Controller{
		logger:        p.Logger.Named("mergeconflictcheck_controller"),
		metricsScope:  p.Scope.SubScope("mergeconflictcheck_controller"),
		topicKey:      p.TopicKey,
		consumerGroup: p.ConsumerGroup,
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

	// TODO: attempt the ordered merge steps without committing and publish a
	// MergeResult to the merge-conflict-check-signal queue. For now the request
	// is only logged after parsing.
	c.logger.Infow("received merge-conflict-check request",
		"id", request.Id,
		"queue_name", request.QueueName,
		"step_count", len(request.Steps),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

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
