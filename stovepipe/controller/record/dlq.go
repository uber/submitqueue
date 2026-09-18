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

	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// DLQController abandons promotion failures while leaving all other record
// reconciliation failures under the DLQ consumer's normal retry policy.
type DLQController struct {
	controller *Controller
}

var _ consumer.Controller = (*DLQController)(nil)

// NewDLQController wraps a record controller with promotion-specific DLQ policy.
func NewDLQController(controller *Controller) *DLQController {
	return &DLQController{controller: controller}
}

// Process never replays a known promotion failure. If reconciliation reaches
// promotion for a failure originally raised by another stage, it makes that one
// attempt and acknowledges a promotion failure rather than retrying it on the DLQ.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) error {
	if originalFailure, failed := delivery.Failure(); failed && isPromotionFailure(originalFailure) {
		return c.abandonPromotion(ctx, delivery, originalFailure, "dead_lettered")
	}

	err := c.controller.Process(ctx, delivery)
	if err == nil {
		return nil
	}

	currentFailure := errs.Attribution(err)
	if !isPromotionFailure(currentFailure) {
		return err
	}
	return c.abandonPromotion(ctx, delivery, currentFailure, "reconciliation_failed")
}

func isPromotionFailure(f failure.Failure) bool {
	stage, ok := f.Detail[failureDetailKeyRecordStage].(string)
	return ok && stage == failureRecordStagePromotion
}

func (c *DLQController) abandonPromotion(ctx context.Context, delivery consumer.Delivery, f failure.Failure, reason string) error {
	if err := c.persistPromotionFailedHistory(ctx, delivery); err != nil {
		return promotionFailure(fmt.Errorf("failed to persist promotion failure history: %w", err))
	}

	msg := delivery.Message()
	metrics.NamedCounter(c.controller.metricsScope, _opName, "promotions_abandoned", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("reason", reason))...,
	)
	c.controller.logger.Errorw("abandoned promotion from record dlq",
		"queue", msg.Tenant,
		"message_id", msg.ID,
		"attempt", delivery.Attempt(),
		"reason", reason,
		"error", f.Message,
	)
	return nil
}

func (c *DLQController) persistPromotionFailedHistory(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	rec := &stovepipemq.Record{}
	if err := stovepipemq.Unmarshal(msg.Payload, rec); err != nil {
		return fmt.Errorf("failed to deserialize record: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rec.GetQueueName()); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	store, err := c.controller.stores.For(storage.Config{QueueName: rec.GetQueueName()})
	if err != nil {
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rec.GetQueueName(), err)
	}
	request, err := c.controller.loadRequest(ctx, store, rec.GetId())
	if err != nil {
		return err
	}
	log := requestlog.NewRequestEventLog(request, entity.RequestEventPromotionFailed, "repository", nil)
	if err := c.controller.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to record promotion failure for request %s: %w", request.ID, err)
	}
	return nil
}

// Name returns the wrapped record controller's name.
func (c *DLQController) Name() string { return c.controller.Name() }

// TopicKey returns the wrapped record controller's topic key.
func (c *DLQController) TopicKey() consumer.TopicKey { return c.controller.TopicKey() }

// ConsumerGroup returns the wrapped record controller's consumer group.
func (c *DLQController) ConsumerGroup() string { return c.controller.ConsumerGroup() }
