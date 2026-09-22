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

package record

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const _dlqOpName = "record_dlq"

// DLQController records that record-stage work was abandoned without replaying
// any of the stage's durable writes or outbound calls.
type DLQController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*DLQController)(nil)

// NewDLQController creates a controller for abandoned record work.
func NewDLQController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *DLQController {
	name := string(topicKey) + "_controller"
	return &DLQController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		materializer:  materializer,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process retains an observable abandonment occurrence for record work that the
// primary consumer could not finish. Deterministic poison is acknowledged;
// failures reading durable state or retaining history are returned for retry.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	rec := &stovepipemq.Record{}
	if err := stovepipemq.Unmarshal(msg.Payload, rec); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding malformed record dlq message",
			"message_id", msg.ID,
			"error", err,
		)
		return nil
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rec.GetQueueName()); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding record dlq message with invalid queue identity",
			"message_id", msg.ID,
			"request_id", rec.GetId(),
			"error", err,
		)
		return nil
	}
	if rec.GetId() == "" {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "empty_id_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding record dlq message with empty request id",
			"message_id", msg.ID,
			"queue", rec.GetQueueName(),
		)
		return nil
	}

	store, err := c.stores.For(storage.Config{QueueName: rec.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding record dlq message for unresolvable queue",
			"message_id", msg.ID,
			"request_id", rec.GetId(),
			"queue", rec.GetQueueName(),
			"error", err,
		)
		return nil
	}

	request, err := store.GetRequestStore().Get(ctx, rec.GetId())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, _dlqOpName, "request_not_found", 1, metrics.TagsFromContext(ctx)...)
			c.logger.Errorw("discarding record dlq message for missing request",
				"message_id", msg.ID,
				"request_id", rec.GetId(),
				"queue", rec.GetQueueName(),
			)
			return nil
		}
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "request_store_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to load request %s for record dlq: %w", rec.GetId(), err)
	}
	originalFailure, hasFailure := delivery.Failure()
	log := requestlog.NewRequestEventLog(request, entity.RequestEventRecordAbandoned, "repository", nil)
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "history_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to retain abandoned record work for request %s: %w", request.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, _dlqOpName, "requests_abandoned", 1, metrics.TagsFromContext(ctx)...)
	fields := []any{
		"message_id", msg.ID,
		"request_id", request.ID,
		"queue", request.Queue,
		"request_state", request.State,
		"history_event", entity.RequestEventRecordAbandoned,
		"attempt", delivery.Attempt(),
	}
	if hasFailure {
		fields = append(fields,
			"failure", originalFailure.Message,
			"failure_subjects", originalFailure.Subjects,
			"failure_detail", originalFailure.Detail,
		)
	}
	c.logger.Errorw("abandoned record work after retaining failure history", fields...)
	return nil
}

// Name returns the controller's name.
func (c *DLQController) Name() string { return string(c.topicKey) }

// TopicKey returns the controller's topic key.
func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }

// ConsumerGroup returns the controller's consumer group.
func (c *DLQController) ConsumerGroup() string { return c.consumerGroup }
