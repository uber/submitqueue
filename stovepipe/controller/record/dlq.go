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
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// DLQController reconciles record work without replaying known promotion failures.
type DLQController struct {
	requestRecorder
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*DLQController)(nil)

// NewDLQController creates a controller for record dead-letter reconciliation.
func NewDLQController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	sourceControl sourcecontrol.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *DLQController {
	name := string(topicKey) + "_controller"
	return &DLQController{
		requestRecorder: newRequestRecorder(logger, scope, materializer, sourceControl, registry, name),
		stores:          stores,
		topicKey:        topicKey,
		consumerGroup:   consumerGroup,
	}
}

// Process reconstructs the record stage's durable effects. It never replays a
// known promotion failure; a newly encountered promotion failure is retained in
// request history and acknowledged rather than retried on the DLQ.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	rec := &stovepipemq.Record{}
	if err := stovepipemq.Unmarshal(msg.Payload, rec); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to deserialize record: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rec.GetQueueName()); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	store, err := c.stores.For(storage.Config{QueueName: rec.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rec.GetQueueName(), err)
	}
	request, err := loadRequest(ctx, store, rec.GetId())
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	if rec.GetQueueName() != "" && rec.GetQueueName() != request.Queue {
		metrics.NamedCounter(c.metricsScope, _opName, "queue_mismatch", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", rec.GetQueueName(), request.Queue, request.ID)
	}

	if originalFailure, failed := delivery.Failure(); failed && isPromotionFailure(originalFailure) {
		return c.abandonPromotion(ctx, delivery, store, request, originalFailure, "dead_lettered")
	}

	err = c.recordRequest(ctx, store, request)
	if err == nil {
		return nil
	}

	currentFailure := errs.Attribution(err)
	if !isPromotionFailure(currentFailure) {
		return err
	}
	return c.abandonPromotion(ctx, delivery, store, request, currentFailure, "reconciliation_failed")
}

func isPromotionFailure(f failure.Failure) bool {
	stage, ok := f.Detail[failureDetailKeyRecordStage].(string)
	return ok && stage == failureRecordStagePromotion
}

func (c *DLQController) abandonPromotion(
	ctx context.Context,
	delivery consumer.Delivery,
	store storage.Storage,
	request entity.Request,
	f failure.Failure,
	reason string,
) error {
	if err := c.persistPromotionFailedHistory(ctx, store, request); err != nil {
		return promotionFailure(fmt.Errorf("failed to persist promotion failure history: %w", err))
	}

	msg := delivery.Message()
	metrics.NamedCounter(c.metricsScope, _opName, "promotions_abandoned", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("reason", reason))...,
	)
	c.logger.Errorw("abandoned promotion from record dlq",
		"queue", msg.Tenant,
		"message_id", msg.ID,
		"attempt", delivery.Attempt(),
		"reason", reason,
		"error", f.Message,
	)
	return nil
}

func (c *DLQController) persistPromotionFailedHistory(ctx context.Context, store storage.Storage, request entity.Request) error {
	log := requestlog.NewRequestEventLog(request, entity.RequestEventPromotionFailed, "repository", nil)
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record promotion failure for request %s: %w", request.ID, err)
	}
	return nil
}

// Name returns the controller's name.
func (c *DLQController) Name() string { return string(c.topicKey) }

// TopicKey returns the controller's topic key.
func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }

// ConsumerGroup returns the controller's consumer group.
func (c *DLQController) ConsumerGroup() string { return c.consumerGroup }
