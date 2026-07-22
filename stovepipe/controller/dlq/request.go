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
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller is the DLQ reconciler for the process stage. It is registered against the
// process topic's DLQ (see TopicKey) and, on each delivery, decodes the request id from
// the same ProcessRequest payload the primary process controller consumes, then drives
// the referenced request to a terminal failed state via failRequest.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "process_dlq"

// NewController creates a new DLQ controller for the process stage's dead-letter topic.
// topicKey is typically dlq.TopicKey(stovepipemq.TopicKeyProcess).
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_dlq_controller"),
		metricsScope:  scope.SubScope("process_dlq_controller"),
		store:         store,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reconciles a single DLQ delivery for the process topic. Returns nil to ack
// (success) or an error to nack (retry) — pair this controller only with a consumer
// wired with errs.AlwaysRetryableProcessor so a transient reconcile failure retries
// instead of dead-lettering the DLQ message itself.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, _opName, metrics.LongLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	pr := &stovepipemq.ProcessRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, pr); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Decoding the same bytes normally fails deterministically, but this error is
		// still retried: the DLQ consumer's AlwaysRetryableProcessor (see Process doc)
		// classifies every error as retryable. That is deliberate — the recoverable
		// cause is deployment skew, where a newer producer's payload shape reaches a
		// not-yet-upgraded consumer and decodes fine once the rollout completes. A
		// genuinely malformed payload exhausts the DLQ subscription's MaxAttempts
		// backstop and is dropped by the subscriber with a warning log; acking it here
		// instead would skip reconciliation silently and leave the referenced request
		// non-terminal.
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if pr.Id == "" {
		metrics.NamedCounter(c.metricsScope, _opName, "empty_id_errors", 1)
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	dmeta := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", pr.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", dmeta["dlq.original_topic"],
		"dlq_failure_count", dmeta["dlq.failure_count"],
		"dlq_last_error", dmeta["dlq.last_error"],
	)

	if err := failRequest(ctx, c.store, c.logger, pr.Id); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "reconcile_errors", 1)
		return err
	}

	metrics.NamedCounter(c.metricsScope, _opName, "reconciled", 1)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "process_dlq"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
