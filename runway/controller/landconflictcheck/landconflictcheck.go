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

// Package landconflictcheck consumes dry-run land-conflict check requests from
// Runway's land-conflict-check queue. A request asks whether an ordered sequence
// of land steps can be applied cleanly onto the land target.
//
// The controller obtains a Lander for the request's land target, calls
// CheckLandability, and publishes the LandResult to the
// land-conflict-check-signal queue. A land conflict is an expected outcome
// (ack + publish FAILED result), not an infrastructure error.
package landconflictcheck

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/runway/extension/lander"
	"go.uber.org/zap"
)

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// Controller handles land-conflict-check queue messages.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	landerFactory lander.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Params are the parameters for creating a new land-conflict-check controller.
type Params struct {
	TopicKey      consumer.TopicKey
	ConsumerGroup string

	LanderFactory lander.Factory
	Registry      consumer.TopicRegistry

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a new land-conflict-check controller for the runway service.
func NewController(p Params) *Controller {
	return &Controller{
		logger:        p.Logger.Named("landconflictcheck_controller"),
		metricsScope:  p.Scope.SubScope("landconflictcheck_controller"),
		landerFactory: p.LanderFactory,
		registry:      p.Registry,
		topicKey:      p.TopicKey,
		consumerGroup: p.ConsumerGroup,
	}
}

// Process deserializes the land request, performs a dry-run landability check, and
// publishes the result. Returns nil to ack, or an error to nack.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	request := &runwaymq.LandRequest{}
	if err := runwaymq.Unmarshal(msg.Payload, request); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize land request: %w", err)
	}

	c.logger.Infow("received land-conflict-check request",
		"id", request.Id,
		"queue_name", request.QueueName,
		"step_count", len(request.Steps),
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	m, err := c.landerFactory.For(lander.Config{QueueName: request.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "factory_errors", 1)
		return fmt.Errorf("failed to create lander for queue %s: %w", request.GetQueueName(), err)
	}

	result, err := m.CheckLandability(ctx, request)
	if err != nil {
		if !lander.IsTerminal(err) {
			metrics.NamedCounter(c.metricsScope, opName, "check_errors", 1)
			return fmt.Errorf("failed to check landability for %s: %w", request.GetId(), err)
		}
		if errors.Is(err, lander.ErrInvalidRequest) {
			metrics.NamedCounter(c.metricsScope, opName, "invalid_requests", 1)
			c.logger.Infow("invalid land request",
				"id", request.GetId(),
				"queue_name", request.GetQueueName(),
				"err", err,
			)
		} else {
			metrics.NamedCounter(c.metricsScope, opName, "land_conflicts", 1)
			c.logger.Infow("land conflict detected",
				"id", request.GetId(),
				"queue_name", request.GetQueueName(),
			)
		}
		result = &runwaymq.LandResult{
			Id:      request.GetId(),
			Outcome: runwaypb.Outcome_FAILED,
			Reason:  err.Error(),
		}
	}

	// Echo the request's queue name so the consumer can route the result by
	// queue without loading state first.
	result.QueueName = request.GetQueueName()

	if err := c.publish(ctx, runwaymq.TopicKeyLandConflictCheckSignal, result, msg.PartitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish land-conflict-check result for %s: %w", request.GetId(), err)
	}

	return nil
}

// publish serializes a LandResult and publishes it to the given signal topic.
//
// The message ID is the correlation ID with no cause: a check is answered
// once, so a redelivery that re-answers it is meant to dedup rather than tell
// the caller twice.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, result *runwaymq.LandResult, partitionKey string) error {
	payload, err := runwaymq.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to serialize land result: %w", err)
	}

	if err := publish.Message(ctx, c.registry, key, publish.IntentID(result.GetId()), payload, partitionKey); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "land-conflict-check"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
