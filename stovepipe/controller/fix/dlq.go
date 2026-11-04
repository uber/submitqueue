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

package fix

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// DLQController retains repair submission outcomes without submitting more work.
type DLQController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	topicKey      consumer.TopicKey
	consumerGroup string
}

// NewDLQController creates repair history recovery. Hosts must use unlimited,
// always-retryable DLQ processing so a history outage cannot discard its retry trigger.
func NewDLQController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, materializer requestlog.Materializer, topicKey consumer.TopicKey, consumerGroup string) *DLQController {
	return &DLQController{
		logger: logger.Named("fix_dlq_controller"), metricsScope: scope.SubScope("fix_dlq_controller"),
		stores: stores, materializer: materializer, topicKey: topicKey, consumerGroup: consumerGroup,
	}
}

func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process", metrics.StorageLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()
	msg := delivery.Message()
	fix := &mq.FixRequest{}
	if err := mq.Unmarshal(msg.Payload, fix); err != nil {
		c.logger.Errorw("discarding malformed repair recovery", "error", err)
		return nil
	}
	if err := entityqueue.ValidatePayloadQueue(msg, fix.GetQueueName()); err != nil || fix.GetId() == "" {
		c.logger.Errorw("discarding repair recovery with invalid identity", "error", err)
		return nil
	}
	store, err := c.stores.For(storage.Config{QueueName: fix.GetQueueName()})
	if err != nil {
		c.logger.Errorw("discarding repair recovery for an unresolvable queue", "queue", fix.GetQueueName(), "error", err)
		return nil
	}
	request, err := loadRequestForBuild(ctx, store, fix.GetQueueName(), fix.GetId())
	if err != nil {
		if storage.IsNotFound(err) || errs.IsUserError(err) {
			c.logger.Errorw("discarding repair recovery without a matching request", "build_id", fix.GetId(), "error", err)
			return nil
		}
		return err
	}
	observation, eligible, err := findIssueToFix(ctx, store.GetBuildIssueStore(), store.GetIssueStateStore(), fix.GetId())
	if err != nil {
		return fmt.Errorf("load repair decision for recovery: %w", err)
	}
	if observation.FixID != "" {
		return persistFixSubmittedLog(ctx, c.materializer, store, request, fix.GetId(), observation)
	}
	if !eligible {
		return persistFixSubmissionDecision(ctx, c.materializer, store, request, fix.GetId(),
			entity.RequestEventFixSubmissionSkipped, "no_eligible_issue")
	}
	// A provider can have accepted work even when no ID was returned. Preserve
	// that uncertainty and leave repair associations and issue status alone.
	return persistFixSubmissionDecision(ctx, c.materializer, store, request, fix.GetId(),
		entity.RequestEventFixSubmissionAbandoned, "acceptance_unknown")
}

func (c *DLQController) Name() string                { return string(c.topicKey) }
func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *DLQController) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*DLQController)(nil)
