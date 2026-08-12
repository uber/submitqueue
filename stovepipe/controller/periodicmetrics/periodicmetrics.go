// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Package periodicmetrics holds the periodic-metrics queue controller. It consumes
// PeriodicMetrics messages (a queue name) and emits metrics describing that queue's
// current health, sampled from state the pipeline already persists.
//
// It is the one stage no other stage feeds: the deployment publishes to this topic
// on a schedule. That is deliberate rather than incidental. The health reported here
// degrades while nothing happens — a queue whose last-known-green commit stops
// advancing ages silently — so an observation triggered by pipeline activity would go
// quiet in exactly the outage worth alerting on. Being driven by a clock instead of by
// work gives the observation a cadence that holds while the pipeline is idle.
//
// The stage advances no entity and publishes nothing onward. It reads through the
// storage and source-control extensions and writes only metrics.
package periodicmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes PeriodicMetrics messages and observes the named queue.
// Implements consumer.Controller.
type Controller struct {
	logger         *zap.SugaredLogger
	metricsScope   tally.Scope
	stores         storage.Factory
	sourceControls sourcecontrol.Factory
	topicKey       consumer.TopicKey
	consumerGroup  string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

const (
	// _opName is the metric operation name for this stage's own handling counters.
	_opName = "periodicmetrics"

	// _opLastGreen is the metric operation name for the last-known-green
	// observation. It is named for what is measured rather than for this stage, so
	// the series an operator alerts on does not move if the stage does.
	_opLastGreen = "last_green"
)

// NewController creates a new periodic metrics controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	sourceControls sourcecontrol.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:         logger.Named("periodicmetrics_controller"),
		metricsScope:   scope.SubScope("periodicmetrics_controller"),
		stores:         stores,
		sourceControls: sourceControls,
		topicKey:       topicKey,
		consumerGroup:  consumerGroup,
	}
}

// Process observes the queue named in the delivery. Returns nil to ack (success) or
// an error to nack (retry) / reject (DLQ).
//
// Only a message that violates the payload contract is rejected; a failed observation
// acks. Nothing downstream depends on this stage, so an error would buy nothing but
// retries of a sample whose moment has passed — and since the schedule keeps producing
// messages, a persistently failing observation would fill the dead-letter queue at the
// publishing rate. Every reason an observation cannot be made is counted with the step
// that failed instead, which is where a reporting fault belongs.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	req := &stovepipemq.PeriodicMetrics{}
	if err := stovepipemq.Unmarshal(msg.Payload, req); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize periodic metrics request: %w", err)
	}

	queue := req.GetQueueName()
	if queue == "" {
		metrics.NamedCounter(c.metricsScope, _opName, "missing_queue", 1)
		// Non-retryable: the queue to observe is the whole payload.
		return fmt.Errorf("periodic metrics request has no queue name")
	}

	c.reportLastGreenAge(ctx, queue)

	metrics.NamedCounter(c.metricsScope, _opName, "observed", 1, metrics.NewTag("queue", queue))
	return nil
}

// reportLastGreenAge updates the gauge holding the current age of the queue's
// last-known-green commit. A gauge rather than a histogram because the answer is the
// latest observation, not a distribution: how stale the bookmark is *now*.
//
// Callers gate deployments on that commit, so its age is the staleness of the newest
// thing they are allowed to ship: a queue whose green bookmark stopped advancing looks
// healthy from the pipeline's perspective — nothing is failing — while the answer it
// serves silently ages.
func (c *Controller) reportLastGreenAge(ctx context.Context, queue string) {
	queueTag := metrics.NewTag("queue", queue)

	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		c.ageError(queueTag, "resolve_storage", queue, err)
		return
	}

	queueRow, err := store.GetQueueStore().Get(ctx, queue)
	if err != nil {
		c.ageError(queueTag, "get_queue", queue, err)
		return
	}

	// A queue that has never gone green has no age to report. Emitting zero
	// would read as "green as of right now", the opposite of the truth.
	if queueRow.LastGreenURI == "" {
		metrics.NamedCounter(c.metricsScope, _opLastGreen, "age_missing", 1, queueTag)
		return
	}

	sourceControl, err := c.sourceControls.For(sourcecontrol.Config{QueueName: queue})
	if err != nil {
		c.ageError(queueTag, "resolve_source_control", queue, err)
		return
	}

	info, err := sourceControl.ChangeInfo(ctx, queueRow.LastGreenURI)
	if err != nil || info.CreatedAt.IsZero() {
		c.ageError(queueTag, "get_change_info", queue, err)
		return
	}

	// A commit dated in the future means the provider's clock disagrees with
	// ours; a negative age would corrupt the series rather than describe it.
	age := time.Since(info.CreatedAt)
	if age < 0 {
		c.ageError(queueTag, "future_change", queue, nil)
		return
	}

	metrics.NamedGauge(c.metricsScope, _opLastGreen, "age_seconds", age.Seconds(), queueTag)
}

// ageError counts an observation that could not be made, tagged with the step that
// failed so a silent gauge can be told apart from a broken dependency.
func (c *Controller) ageError(queueTag metrics.Tag, step, queue string, err error) {
	metrics.NamedCounter(c.metricsScope, _opLastGreen, "age_errors", 1, queueTag, metrics.NewTag("step", step))
	c.logger.Errorw("failed to observe last green age", "queue", queue, "step", step, "error", err)
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "periodicmetrics"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
