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
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// buildController reconciles build-stage dead letters by failing the request
// whose build could not be triggered, persisted, or handed to the poll loop.
type buildController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*buildController)(nil)

const _buildOpName = "build_dlq"

// NewDLQBuildController creates a reconciler for the build dead-letter topic.
func NewDLQBuildController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	topicKey consumer.TopicKey,
	consumerGroup string,
) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &buildController{
		logger:        logger.Named(name),
		metricsScope:  scope.SubScope(name),
		stores:        stores,
		materializer:  materializer,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process drives the request named by a dead-lettered BuildRequest to failed.
func (c *buildController) Process(ctx context.Context, delivery consumer.Delivery) error {
	buildRequest := &stovepipemq.BuildRequest{}
	if err := stovepipemq.Unmarshal(delivery.Message().Payload, buildRequest); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to decode dlq payload: %w", err)
	}
	if buildRequest.Id == "" {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "empty_id_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("build dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(storage.Config{QueueName: buildRequest.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to resolve storage for queue %q: %w", buildRequest.GetQueueName(), err)
	}

	metadata := delivery.Metadata()
	c.logger.Warnw("dlq message received",
		"request_id", buildRequest.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", metadata["dlq.original_topic"],
		"dlq_failure_count", metadata["dlq.failure_count"],
		"dlq_last_error", metadata["dlq.last_error"],
	)

	if err := failRequest(ctx, store, c.materializer, c.logger, buildRequest.Id, entity.RequestOutcomeReasonProcessingFailed); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "reconcile_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}
	metrics.NamedCounter(c.metricsScope, _buildOpName, "reconciled", 1, metrics.TagsFromContext(ctx)...)
	return nil
}

// Name returns the controller name.
func (c *buildController) Name() string { return string(c.topicKey) }

// TopicKey returns the dead-letter topic key.
func (c *buildController) TopicKey() consumer.TopicKey { return c.topicKey }

// ConsumerGroup returns the offset-tracking group.
func (c *buildController) ConsumerGroup() string { return c.consumerGroup }
