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

package log

import (
	"context"
	"fmt"

	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/storage"
	"go.uber.org/zap"
)

// Controller handles log queue messages.
// It consumes request log entries and persists them to storage.
// Implements consumer.Controller interface for integration with the consumer.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new log controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("log_controller"),
		metricsScope:  scope.SubScope("log_controller"),
		store:         store,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process processes a log delivery from the queue.
// Deserializes the request log entry and persists it to storage.
// Returns nil to ack (success), or error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	// Deserialize request log entry
	logEntry, err := entity.RequestLogFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: malformed messages will never succeed regardless of retry count
		return fmt.Errorf("failed to deserialize request log: %w", err)
	}

	c.logger.Debugw("received request log entry",
		"request_id", logEntry.RequestID,
		"status", string(logEntry.Status),
		"request_version", logEntry.RequestVersion,
		"attempt", delivery.Attempt(),
	)

	// Persist request log to storage
	if err := c.store.GetRequestLogStore().Insert(ctx, logEntry); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return fmt.Errorf("failed to insert request log: %w", err)
	}

	return nil // Success - message will be acked
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "log"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
