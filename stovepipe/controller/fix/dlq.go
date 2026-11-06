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

	"errors"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// DLQController retains repair submission outcomes without submitting more work.
type DLQController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	executors     fixer.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

// NewDLQController creates repair history recovery. Hosts must use unlimited,
// always-retryable DLQ processing so a history outage cannot discard its retry trigger.
func NewDLQController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, materializer requestlog.Materializer, executors fixer.Factory, topicKey consumer.TopicKey, consumerGroup string) *DLQController {
	return &DLQController{
		logger: logger.Named("fix_dlq_controller"), metricsScope: scope.SubScope("fix_dlq_controller"),
		stores: stores, materializer: materializer, executors: executors, topicKey: topicKey, consumerGroup: consumerGroup,
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
	registrations, err := c.executors.For(fixer.Config{QueueName: fix.GetQueueName()})
	if err != nil {
		return err
	}
	if err := validateRegistrations(registrations); err != nil {
		return err
	}
	observations, eligible, err := collectIssues(ctx, store, fix.GetId())
	if err != nil {
		return fmt.Errorf("load repair decisions: %w", err)
	}
	open := map[entity.OpenIssue]bool{}
	for _, issue := range eligible {
		open[issue] = true
	}
	for _, registration := range registrations {
		for _, observation := range observations {
			issue := entity.OpenIssue{TaskID: observation.TaskID, UnitID: observation.UnitID, FirstSeenBuildID: fix.GetId()}
			result, err := store.GetIssueRepairStore().Get(ctx, issue, registration.ID)
			if errors.Is(err, storage.ErrNotFound) {
				if !open[issue] {
					continue
				}
				result = entity.IssueRepair{Issue: issue, ExecutorID: registration.ID}
			} else if err != nil {
				return err
			}
			if result.FixID == "" && result.SkipReason == "" {
				if err := persistRepairEventLog(ctx, c.materializer, store, request, result, entity.RequestEventFixSubmissionAbandoned, "acceptance_unknown"); err != nil {
					return err
				}
				continue
			}
			if err := persistRepairSubmissionLog(ctx, c.materializer, store, request, result); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *DLQController) Name() string                { return string(c.topicKey) }
func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *DLQController) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*DLQController)(nil)
