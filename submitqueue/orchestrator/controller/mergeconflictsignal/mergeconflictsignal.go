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

// Package mergeconflictsignal consumes merge-conflict check results from runway's
// signal queue, correlates them to the request by the echoed id, and either
// advances the request to the batch stage (mergeable) or fails it (conflicted).
// Unlike buildsignal it is purely result-driven — runway pushes the result, so
// there is no poll loop or self-reschedule.
package mergeconflictsignal

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	corerequest "github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles mergeconflictsignal queue messages. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new mergeconflictsignal controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("mergeconflictsignal_controller"),
		metricsScope:  scope.SubScope("mergeconflictsignal_controller"),
		store:         store,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process consumes a runway check result and advances or fails the request.
// Returns nil to ack, or error to nack/reject.
//
// A not-mergeable verdict is an expected outcome of the check, not a failure:
// the request is driven to terminal Error inline and the message is acked. Only
// infrastructure faults — deserialize, storage, the terminal transition, and the
// batch publish — return an error and reject to the DLQ, where the request is
// reconciled to Error.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()

	// The runway result carries full data (it crosses the service boundary). Its
	// id is the request id echoed back, so correlate straight to the request.
	result := &runwaymq.MergeResult{}
	if err := runwaymq.Unmarshal(msg.Payload, result); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize merge conflict check result: %w", err)
	}

	request, err := c.store.GetRequestStore().Get(ctx, result.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to get request %s: %w", result.Id, err)
	}

	c.logger.Infow("received mergeconflict signal",
		"request_id", request.ID,
		"mergeable", result.Outcome == runwaypb.Outcome_SUCCEEDED,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	// Short-circuit halted requests: the cancel path owns driving them terminal.
	if entity.IsRequestStateHalted(request.State) {
		metrics.NamedCounter(c.metricsScope, opName, "skipped_halted", 1)
		c.logger.Infow("skipping mergeconflict signal for halted request",
			"request_id", request.ID,
			"state", string(request.State),
		)
		return nil
	}

	if result.Outcome != runwaypb.Outcome_SUCCEEDED {
		metrics.NamedCounter(c.metricsScope, opName, "not_mergeable", 1)
		c.logger.Infow("request not mergeable",
			"request_id", request.ID,
			"reason", result.Reason,
		)
		if err := c.failRequest(ctx, request, result.Reason); err != nil {
			metrics.NamedCounter(c.metricsScope, opName, "fail_errors", 1)
			return fmt.Errorf("failed to fail request %s: %w", request.ID, err)
		}
		return nil
	}

	// Advance the request to Validated now that the merge-conflict check passed.
	newVersion := request.Version + 1
	if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newVersion, entity.RequestStateValidated); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "state_errors", 1)
		return fmt.Errorf("failed to update request %s state to validated: %w", request.ID, err)
	}
	request.Version = newVersion
	request.State = entity.RequestStateValidated

	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusValidated, request.Version, "", nil)
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "log_errors", 1)
		return fmt.Errorf("failed to publish request log for %s: %w", request.ID, err)
	}

	if err := c.publishRequestID(ctx, topickey.TopicKeyBatch, request.ID, request.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to batch: %w", err)
	}

	c.logger.Infow("request validated and published to batch",
		"request_id", request.ID,
		"topic_key", topickey.TopicKeyBatch,
	)

	return nil // Success - message will be acked
}

// failRequest drives the request to terminal RequestStateError and records the
// conflict reason on the request log. A not-mergeable verdict is an expected
// terminal outcome of the check, so the request is concluded here directly.
//
// Idempotent under at-least-once delivery: a redelivery whose request is already
// in Error skips the state CAS but still publishes the log (so a prior attempt
// that flipped the state but failed before logging is repaired); a request that
// reached a different terminal state (e.g. a racing cancel) is left untouched.
func (c *Controller) failRequest(ctx context.Context, request entity.Request, reason string) error {
	switch {
	case request.State == entity.RequestStateError:
		// Idempotent retry: a prior delivery already wrote Error. Fall through to
		// the log publish.
	case entity.IsRequestStateTerminal(request.State):
		c.logger.Warnw("request already in different terminal state, skipping fail",
			"request_id", request.ID,
			"state", string(request.State),
		)
		return nil
	default:
		newVersion := request.Version + 1
		if err := c.store.GetRequestStore().UpdateState(ctx, request.ID, request.Version, newVersion, entity.RequestStateError); err != nil {
			return fmt.Errorf("failed to update request %s state to error: %w", request.ID, err)
		}
		request.Version = newVersion
		request.State = entity.RequestStateError
	}

	logEntry := entity.NewRequestLog(request.ID, entity.RequestStatusError, request.Version, reason, nil)
	if err := corerequest.PublishLog(ctx, c.registry, logEntry, request.ID); err != nil {
		return fmt.Errorf("failed to publish request log for %s: %w", request.ID, err)
	}
	return nil
}

// publishRequestID publishes a request ID to the given topic key, partitioned by queue.
func (c *Controller) publishRequestID(ctx context.Context, key consumer.TopicKey, requestID string, partitionKey string) error {
	payload, err := entity.RequestID{ID: requestID}.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request ID: %w", err)
	}

	msg := entityqueue.NewMessage(requestID, payload, partitionKey, nil)

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
	return "mergeconflictsignal"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
