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

package projectanalysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// DLQController retains incomplete project analysis without repeating provider calls.
type DLQController struct {
	logger                 *zap.SugaredLogger
	scope                  tally.Scope
	stores                 storage.Factory
	requestLogMaterializer requestlog.Materializer
	registry               consumer.TopicRegistry
	topicKey               consumer.TopicKey
	consumerGroup          string
}

func NewDLQController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	requestLogMaterializer requestlog.Materializer,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *DLQController {
	name := string(topicKey) + "_controller"
	return &DLQController{
		logger:                 logger.Named(name),
		scope:                  scope.SubScope(name),
		stores:                 stores,
		requestLogMaterializer: requestLogMaterializer,
		registry:               registry,
		topicKey:               topicKey,
		consumerGroup:          consumerGroup,
	}
}

// Process acknowledges unusable messages and retries failures retaining history or publishing.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.scope, "process", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()
	analysis := &stovepipemq.ProjectAnalysis{}
	if err := stovepipemq.Unmarshal(msg.Payload, analysis); err != nil {
		c.logger.Errorw("discarding malformed project analysis dlq message", "message_id", msg.ID, "error", err)
		return nil
	}
	if err := entityqueue.ValidatePayloadQueue(msg, analysis.GetQueueName()); err != nil {
		c.logger.Errorw("discarding project analysis dlq message with invalid queue identity", "message_id", msg.ID, "error", err)
		return nil
	}
	if err := id.Validate(analysis.GetId()); err != nil {
		c.logger.Errorw("discarding project analysis dlq message with invalid request ID", "message_id", msg.ID, "error", err)
		return nil
	}
	if msg.PartitionKey != analysis.GetId() {
		c.logger.Errorw("discarding project analysis dlq message from the wrong partition", "message_id", msg.ID, "partition", msg.PartitionKey)
		return nil
	}
	store, err := c.stores.For(storage.Config{QueueName: analysis.GetQueueName()})
	if err != nil {
		c.logger.Errorw("discarding project analysis dlq message for an unresolvable queue",
			"message_id", msg.ID, "queue", analysis.GetQueueName(), "error", err)
		return nil
	}
	request, err := store.GetRequestStore().Get(ctx, analysis.GetId())
	if errors.Is(err, storage.ErrNotFound) {
		c.logger.Errorw("discarding project analysis dlq message for a missing request",
			"message_id", msg.ID, "queue", analysis.GetQueueName(), "request_id", analysis.GetId())
		return nil
	}
	if err != nil {
		return fmt.Errorf("load project analysis dlq request: %w", err)
	}
	if request.Queue != analysis.GetQueueName() {
		c.logger.Errorw("discarding project analysis dlq message for another queue",
			"message_id", msg.ID, "queue", analysis.GetQueueName(), "request_id", request.ID)
		return nil
	}
	switch request.State {
	case entity.RequestStateSucceeded, entity.RequestStateFailed:
	case entity.RequestStateCancelled:
		return publishRecord(ctx, c.registry, request)
	case entity.RequestStateSuperseded:
		return nil
	default:
		c.logger.Errorw("discarding project analysis dlq message for a nonterminal request",
			"message_id", msg.ID, "queue", request.Queue, "request_id", request.ID, "state", request.State)
		return nil
	}

	if request.TerminalBuildID != "" {
		projectIssuesRecordedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectIssuesRecorded, request.TerminalBuildID, nil)
		retained, err := store.GetRequestLogStore().Get(ctx, request.ID, projectIssuesRecordedLog.ID)
		if err == nil {
			if err := c.requestLogMaterializer.PersistLog(ctx, store, retained); err != nil {
				return err
			}
			return publishRecord(ctx, c.registry, request)
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("read completed project analysis during recovery: %w", err)
		}
	}

	projectAnalysisAbandonedLog := requestlog.NewRequestEventLog(request, entity.RequestEventProjectAnalysisAbandoned, "projects", nil)
	if err := c.requestLogMaterializer.PersistLog(ctx, store, projectAnalysisAbandonedLog); err != nil {
		return fmt.Errorf("retain abandoned project analysis: %w", err)
	}
	fields := []any{"message_id", msg.ID, "queue", request.Queue, "request_id", request.ID, "build_id", request.TerminalBuildID}
	if original, ok := delivery.Failure(); ok {
		fields = append(fields, "failure", original.Message, "failure_subjects", original.Subjects, "failure_detail", original.Detail)
	}
	c.logger.Errorw("project analysis abandoned; retaining partial results", fields...)
	metrics.NamedCounter(c.scope, "process", "analysis_abandoned", 1, metrics.TagsFromContext(ctx)...)
	return publishRecord(ctx, c.registry, request)
}

func (c *DLQController) Name() string                { return string(c.topicKey) }
func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *DLQController) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*DLQController)(nil)
