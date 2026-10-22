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

package buildanalysis

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

const _dlqOpName = "buildanalysis_dlq"

// DLQController records incomplete analysis without rerunning detection or
// changing the validation outcome.
type DLQController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	topicKey      consumer.TopicKey
	consumerGroup string
}

// NewDLQController creates a controller for incomplete build analysis.
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

// Process retains incomplete-analysis history and acknowledges unusable messages.
// State-read and history-write failures are returned for DLQ recovery retry.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	analysis := &stovepipemq.BuildAnalysis{}
	if err := stovepipemq.Unmarshal(msg.Payload, analysis); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding malformed analysis dlq message",
			"message_id", msg.ID,
			"error", err,
		)
		return nil
	}

	if err := entityqueue.ValidatePayloadQueue(msg, analysis.GetQueueName()); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding analysis dlq message with invalid queue identity",
			"message_id", msg.ID,
			"build_id", analysis.GetId(),
			"error", err,
		)
		return nil
	}

	if analysis.GetId() == "" {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "empty_id_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding analysis dlq message with empty build id",
			"message_id", msg.ID,
			"queue", analysis.GetQueueName(),
		)
		return nil
	}

	store, err := c.stores.For(storage.Config{QueueName: analysis.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding analysis dlq message for unresolvable queue",
			"message_id", msg.ID,
			"build_id", analysis.GetId(),
			"queue", analysis.GetQueueName(),
			"error", err,
		)
		return nil
	}

	build, err := store.GetBuildStore().Get(ctx, analysis.GetId())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, _dlqOpName, "build_not_found", 1, metrics.TagsFromContext(ctx)...)
			c.logger.Errorw("discarding analysis dlq message for missing build",
				"message_id", msg.ID, "build_id", analysis.GetId(), "queue", analysis.GetQueueName())
			return nil
		}

		return fmt.Errorf("failed to load build %s for analysis dlq: %w", analysis.GetId(), err)
	}

	if build.RequestID == "" {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "empty_request_id", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding analysis dlq message for build without a request",
			"message_id", msg.ID, "build_id", analysis.GetId())
		return nil
	}

	request, err := store.GetRequestStore().Get(ctx, build.RequestID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			metrics.NamedCounter(c.metricsScope, _dlqOpName, "request_not_found", 1, metrics.TagsFromContext(ctx)...)
			c.logger.Errorw("discarding analysis dlq message for missing request",
				"message_id", msg.ID, "build_id", analysis.GetId(), "request_id", build.RequestID)
			return nil
		}

		return fmt.Errorf("failed to load request %s for analysis dlq: %w", build.RequestID, err)
	}

	if request.Queue != analysis.GetQueueName() {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Errorw("discarding analysis dlq message for request in another queue",
			"message_id", msg.ID, "request_id", request.ID, "queue", request.Queue,
			"message_queue", analysis.GetQueueName())
		return nil
	}

	log := requestlog.NewRequestEventLog(request, entity.RequestEventBuildAnalysisAbandoned, analysis.GetId(),
		map[string]string{requestlog.MetadataKeyBuildID: analysis.GetId()})
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		metrics.NamedCounter(c.metricsScope, _dlqOpName, "history_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to retain incomplete build analysis for request %s: %w", request.ID, err)
	}

	originalFailure, hasFailure := delivery.Failure()
	metrics.NamedCounter(c.metricsScope, _dlqOpName, "analysis_abandoned", 1, metrics.TagsFromContext(ctx)...)
	fields := []any{
		"message_id", msg.ID,
		"request_id", request.ID,
		"build_id", analysis.GetId(),
		"queue", request.Queue,
		"request_state", request.State,
		"history_event", entity.RequestEventBuildAnalysisAbandoned,
		"attempt", delivery.Attempt(),
	}
	if hasFailure {
		fields = append(fields,
			"failure", originalFailure.Message,
			"failure_subjects", originalFailure.Subjects,
			"failure_detail", originalFailure.Detail,
		)
	}
	c.logger.Errorw("build analysis could not complete; issue observations may be incomplete", fields...)
	return nil
}

func (c *DLQController) Name() string { return string(c.topicKey) }

func (c *DLQController) TopicKey() consumer.TopicKey { return c.topicKey }

func (c *DLQController) ConsumerGroup() string { return c.consumerGroup }

var _ consumer.Controller = (*DLQController)(nil)
