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

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"go.uber.org/zap"
)

// logController is the DLQ reconciler for the log topic.
//
// Unlike the other DLQ controllers, this one performs no state reconciliation.
// The log topic carries observability events (RequestLog rows). Failing to
// persist a log entry has no functional effect on the pipeline — the request
// and batch entities are unaffected — so the right action is to emit a metric
// and a warning and ack the message so it does not sit in the DLQ forever.
type logController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify logController implements consumer.Controller at compile time.
var _ consumer.Controller = (*logController)(nil)

// NewDLQLogController builds a DLQ controller for the log topic.
func NewDLQLogController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &logController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process records that a log message landed in the DLQ and acks it.
func (c *logController) Process(_ context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()
	dmeta := delivery.Metadata()
	c.logger.Warnw("log message dropped to dlq",
		"message_id", msg.ID,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)
	metrics.NamedCounter(c.metricsScope, opName, "dropped", 1)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *logController) Name() string {
	return string(c.topicKey)
}

// TopicKey returns the topic key this controller subscribes to.
func (c *logController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *logController) ConsumerGroup() string {
	return c.consumerGroup
}
