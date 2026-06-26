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

// Package process holds the process-stage queue controller. It consumes the
// request ids ingest publishes, reloads the Request from storage, and (in a
// future change) decides the build strategy by asking SourceControl how the new
// head relates to the queue's last-green URI. For now it is a thin consumer that
// reloads and logs the request, establishing the stage and its wiring.
package process

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes ProcessRequest messages from the process stage, reloads the
// referenced Request from storage, and logs it. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new process controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_controller"),
		metricsScope:  scope.SubScope("process_controller"),
		store:         store,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the request referenced by the delivery and logs it. Returns nil
// to ack (success) or an error to nack (retry). A not-yet-visible request is
// retryable: ingest persists and publishes, but a stale read may not see the row
// yet, so redelivery converges.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	pr := &stovepipemq.ProcessRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, pr); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize process request: %w", err)
	}

	request, err := c.store.GetRequestStore().Get(ctx, pr.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		if errors.Is(err, storage.ErrNotFound) {
			// Retryable: the request row may not be visible yet; redelivery converges.
			return errs.NewRetryableError(fmt.Errorf("request %s not found yet: %w", pr.Id, err))
		}
		return fmt.Errorf("failed to load request %s: %w", pr.Id, err)
	}

	c.logger.Infow("processing request",
		"request_id", request.ID,
		"queue", request.Queue,
		"uri", request.URI,
		"state", string(request.State),
		"version", request.Version,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "process"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
