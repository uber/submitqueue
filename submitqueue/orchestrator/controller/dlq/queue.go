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
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/zap"
)

// queueController is the DLQ reconciler for the prioritize topic. Unlike
// every other primary topic, prioritize's payload carries a QueueID, not a
// request or batch identifier — the stage reconciles a whole queue's build
// budget in one pass, not a single entity. There is therefore no request or
// batch to drive to a terminal state here: the batches the failed round
// would have considered stay exactly where they were (Selected paths stay
// Selected, in-flight builds keep running).
//
// Reconciliation is therefore re-arming, not terminalizing: after logging
// the failure, the controller publishes a fresh prioritize round for the
// queue. Speculate republishes a round on every batch-level event, but a
// queue whose only pending work is waiting on prioritization (nothing
// building, no new requests arriving) may see no further events — without
// the requeue its batches would stay stranded until unrelated activity
// happened to trigger a round.
//
// The requeue cannot poison-loop on payload contents: a round carries only
// the queue name and recomputes entirely from live state, so there is no bad
// datum to replay. A round that fails persistently (e.g. storage down)
// cycles primary retry ladder -> DLQ -> requeue, throttled by the full
// ladder on every cycle and visible in DLQ metrics, and converges the first
// cycle after the fault heals. The requeued message ID is derived from the
// dead-lettered message's ID: distinct from the original round's ID so the
// queue's publish dedup does not swallow it, and deterministic per DLQ
// message so a redelivered DLQ message republishes the same ID and is
// coalesced instead of fanning out.
type queueController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify queueController implements consumer.Controller at compile time.
var _ consumer.Controller = (*queueController)(nil)

// NewDLQQueueController builds a DLQ controller for the prioritize topic.
// registry must resolve topickey.TopicKeyPrioritize — the topic the
// controller re-arms the queue on.
func NewDLQQueueController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &queueController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process logs a DLQ'd prioritize message for operator visibility, re-arms
// the queue by publishing a fresh prioritize round, and acks. See the
// queueController doc comment for why re-arming is the reconciliation here
// and why it cannot loop unboundedly.
func (c *queueController) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	qid, err := entity.QueueIDFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode queue id from dlq payload: %w", err)
	}
	if qid.Name == "" {
		metrics.NamedCounter(c.metricsScope, opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty queue name")
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received; requeueing a fresh prioritization round for queue",
		"queue", qid.Name,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := c.requeueRound(ctx, qid, msg.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to requeue prioritize round for queue %s: %w", qid.Name, err)
	}

	metrics.NamedCounter(c.metricsScope, opName, "reconciled", 1)
	return nil
}

// requeueRound publishes a fresh prioritize round for the queue. The message
// ID appends a suffix to the dead-lettered message's ID (see the
// queueController doc comment for the dedup reasoning).
func (c *queueController) requeueRound(ctx context.Context, qid entity.QueueID, dlqMsgID string) error {
	payload, err := qid.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize queue ID: %w", err)
	}

	msg := entityqueue.NewMessage(dlqMsgID+"/dlq-requeue", payload, qid.Name, nil)

	q, ok := c.registry.Queue(topickey.TopicKeyPrioritize)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyPrioritize)
	}
	topicName, ok := c.registry.TopicName(topickey.TopicKeyPrioritize)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyPrioritize)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *queueController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *queueController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *queueController) ConsumerGroup() string {
	return c.consumerGroup
}
