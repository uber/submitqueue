// Copyright (c) 2026 Uber Technologies, Inc.
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

package hook

import (
	"context"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// reconcileOp is the metric operation name shared by every emit in this file.
const reconcileOp = "reconcile"

// DLQController is the reconciler for the hook topic's dead-letter queue.
// Implements consumer.Controller.
//
// It reconciles nothing, because there is nothing it may touch: a hook never
// writes pipeline state, so a hook event that could not be delivered leaves no
// half-finished transition behind. What is lost is the side effect — a comment
// not posted, a row not exported — which only a person can decide how to
// recover. So this controller makes the loss impossible to miss and hands it
// over: it records the complete event and why it failed, counts it on a metric
// meant to page, and acks so the event does not sit in the DLQ unnoticed.
// Republishing the logged event recovers it.
//
// That is a deliberate step up from the log topic's DLQ, which warns and moves
// on. Dropping an observability row costs a gap in a read model; dropping a
// merge-failure comment costs a support ticket, and nothing else in the system
// will notice it is missing.
type DLQController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*DLQController)(nil)

// NewDLQController builds the DLQ reconciler for a host's hook topic. topicKey
// is the dead-letter key (the hook topic key plus the queue's DLQ suffix), not
// the primary one.
func NewDLQController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *DLQController {
	name := string(topicKey) + "_controller"
	return &DLQController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process records a dropped hook event and acks it.
//
// It never returns an error: a failure here would re-deliver the message
// forever, since the DLQ consumer has no DLQ of its own and treats everything as
// retryable. The record is the outcome, so the only way to fail is not to write
// one.
func (c *DLQController) Process(_ context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	// Decoding is best-effort: the event may be here precisely because it could
	// not be decoded. The raw payload is protojson, so logging it verbatim
	// preserves the whole event either way; the decoded fields only add
	// dimensions worth filtering and alerting on.
	source, eventType, eventID := unknownTagValue, unknownTagValue, ""
	event := &basehook.HookEvent{}
	if err := basehook.Unmarshal(msg.Payload, event); err == nil {
		source, eventType, eventID = event.GetSource(), event.GetType(), event.GetId()
	}

	metrics.NamedCounter(c.metricsScope, reconcileOp, "events_dropped", 1,
		metrics.NewTag("source", source),
		metrics.NewTag("event_type", eventType),
	)

	dmeta := delivery.Metadata()
	fields := []any{
		"message_id", msg.ID,
		"event_id", eventID,
		"source", source,
		"event_type", eventType,
		"event", string(msg.Payload),
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	}
	if f, ok := delivery.Failure(); ok {
		fields = append(fields, "failure", f.Message, "failure_subjects", f.Subjects, "failure_detail", f.Detail)
	}

	c.logger.Errorw("hook event dropped to dlq; republish the logged event to recover", fields...)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *DLQController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *DLQController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *DLQController) ConsumerGroup() string {
	return c.consumerGroup
}
