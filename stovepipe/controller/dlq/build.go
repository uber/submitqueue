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
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// BuildController reconciles build-stage dead letters by failing the request
// whose build could not be triggered, persisted, or handed to the poll loop.
type BuildController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*BuildController)(nil)

const _buildOpName = "build_dlq"

// NewBuildController creates a reconciler for the build dead-letter topic.
func NewBuildController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *BuildController {
	return &BuildController{
		logger:        logger.Named("build_dlq_controller"),
		metricsScope:  scope.SubScope("build_dlq_controller"),
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process drives the request named by a dead-lettered BuildRequest to failed.
func (c *BuildController) Process(ctx context.Context, delivery consumer.Delivery) error {
	request := &stovepipemq.BuildRequest{}
	if err := stovepipemq.Unmarshal(delivery.Message().Payload, request); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "deserialize_errors", 1)
		return fmt.Errorf("failed to decode build dlq payload: %w", err)
	}
	if request.Id == "" {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "empty_id_errors", 1)
		return fmt.Errorf("build dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(storage.Config{QueueName: request.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "storage_resolve_errors", 1)
		return fmt.Errorf("failed to resolve storage for queue %q: %w", request.GetQueueName(), err)
	}

	metadata := delivery.Metadata()
	c.logger.Warnw("build dlq message received",
		"request_id", request.Id,
		"attempt", delivery.Attempt(),
		"dlq_original_topic", metadata["dlq.original_topic"],
		"dlq_failure_count", metadata["dlq.failure_count"],
		"dlq_last_error", metadata["dlq.last_error"],
	)

	if err := failRequest(ctx, store, c.logger, request.Id); err != nil {
		metrics.NamedCounter(c.metricsScope, _buildOpName, "reconcile_errors", 1)
		return err
	}
	metrics.NamedCounter(c.metricsScope, _buildOpName, "reconciled", 1)
	return nil
}

// Name returns the controller name.
func (c *BuildController) Name() string { return "build_dlq" }

// TopicKey returns the dead-letter topic key.
func (c *BuildController) TopicKey() consumer.TopicKey { return c.topicKey }

// ConsumerGroup returns the offset-tracking group.
func (c *BuildController) ConsumerGroup() string { return c.consumerGroup }
