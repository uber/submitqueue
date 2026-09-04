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

// Package dlq reconciles dead-lettered merge requests back to the client.
//
// Runway's inbound merge topics dead-letter a message after the primary
// controller returns a non-retryable error or exhausts retries on a retryable
// one. Runway is stateless and the sole responder on the client's correlation
// id, so a dead-lettered request that produced no signal would leave the client
// (SubmitQueue) waiting forever. Expected outcomes — conflicts and invalid
// requests — are already published as FAILED results by the primary controllers;
// this reconciler is the backstop for everything else (unexpected faults, retry
// exhaustion).
//
// On each delivery from a `{topic}_dlq` topic it decodes the same MergeRequest
// payload the primary controller consumes (the queue preserves the bytes
// verbatim), and republishes a FAILED MergeResult — echoing the correlation id
// — to the corresponding signal topic. Unlike the stovepipe/orchestrator DLQ
// reconcilers it writes no entity state (Runway has none); the signal is the
// resolution. A payload that cannot be decoded carries no correlation id to
// resolve, so it is logged and acked (dropped).
//
// Wire this controller on a dedicated consumer built with
// errs.AlwaysRetryableProcessor so a transient publish failure retries forever
// rather than dead-lettering again (the DLQ topic has no DLQ of its own).
package dlq

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"go.uber.org/zap"
)

// topicSuffix is appended to a primary topic key to derive its DLQ topic key.
// It matches DefaultSubscriptionConfig's DLQ TopicSuffix so a registered DLQ
// subscription's topic name matches the controller's TopicKey().
const topicSuffix = "_dlq"

// TopicKey returns the DLQ topic key for a primary merge topic. Exported so the
// wiring layer builds matching pairs without duplicating the suffix literal.
func TopicKey(main consumer.TopicKey) consumer.TopicKey {
	return consumer.TopicKey(string(main) + topicSuffix)
}

// Verify Controller implements consumer.Controller at compile time.
var _ consumer.Controller = (*Controller)(nil)

// Controller consumes a merge topic's dead-letter queue and republishes a
// terminal FAILED result to the corresponding signal topic.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	registry       consumer.TopicRegistry
	topicKey       consumer.TopicKey
	signalTopicKey consumer.TopicKey
	consumerGroup  string
}

// Params are the parameters for creating a new DLQ reconciler.
type Params struct {
	// TopicKey is the dead-letter topic this controller consumes
	// (typically dlq.TopicKey(<primary topic>)).
	TopicKey consumer.TopicKey
	// SignalTopicKey is the signal topic the FAILED result is published to.
	SignalTopicKey consumer.TopicKey
	ConsumerGroup  string

	Registry consumer.TopicRegistry

	Scope  tally.Scope
	Logger *zap.SugaredLogger
}

// NewController creates a DLQ reconciler for a merge topic's dead-letter queue.
func NewController(p Params) *Controller {
	return &Controller{
		logger:         p.Logger.Named("merge_dlq_controller"),
		metricsScope:   p.Scope.SubScope("merge_dlq_controller"),
		registry:       p.Registry,
		topicKey:       p.TopicKey,
		signalTopicKey: p.SignalTopicKey,
		consumerGroup:  p.ConsumerGroup,
	}
}

// Process decodes the dead-lettered merge request and republishes a FAILED
// result to the signal topic so the client's correlation id resolves. Returns
// nil to ack; an error to nack (retried indefinitely under
// AlwaysRetryableProcessor).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	const opName = "process"

	msg := delivery.Message()
	meta := delivery.Metadata()

	request := &runwaymq.MergeRequest{}
	if err := runwaymq.Unmarshal(msg.Payload, request); err != nil {
		// No correlation id is recoverable, so there is nothing to resolve for
		// the client. Log and ack (drop) rather than retry forever.
		metrics.NamedCounter(c.metricsScope, opName, "undecodable", 1)
		c.logger.Errorw("dlq reconcile: undecodable merge request, dropping",
			"err", err,
			"original_topic", meta["dlq.original_topic"],
		)
		return nil
	}

	reason := ""
	if recordedFailure, failed := delivery.Failure(); failed {
		reason = recordedFailure.Message
	}
	if reason == "" {
		reason = meta["dlq.last_error"]
	}
	if reason == "" {
		reason = "runway failed to process the merge request"
	}

	c.logger.Warnw("dlq reconcile: publishing terminal failure",
		"id", request.GetId(),
		"queue_name", request.GetQueueName(),
		"original_topic", meta["dlq.original_topic"],
		"failure_count", meta["dlq.failure_count"],
		"last_error", reason,
	)

	result := &runwaymq.MergeResult{
		Id:        request.GetId(),
		Outcome:   runwaypb.Outcome_FAILED,
		Reason:    fmt.Sprintf("dead-lettered: %s", reason),
		QueueName: request.GetQueueName(),
	}

	if err := c.publish(ctx, result, msg.PartitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish dlq failure result for %s: %w", request.GetId(), err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "reconciled", 1)
	return nil
}

// publish serializes a MergeResult and publishes it to the signal topic.
//
// Named for the dead letter, because the live handler answers the same request
// on the same topic under the bare correlation ID. Reusing that ID would let
// this terminal failure be deduplicated against an answer that was already
// sent, and the caller would go on waiting for a result nothing will produce.
func (c *Controller) publish(ctx context.Context, result *runwaymq.MergeResult, partitionKey string) error {
	payload, err := runwaymq.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to serialize merge result: %w", err)
	}

	if err := publish.Message(ctx, c.registry, c.signalTopicKey,
		publish.IntentID(result.GetId(), "dlq"), payload, partitionKey); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the dead-letter topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
