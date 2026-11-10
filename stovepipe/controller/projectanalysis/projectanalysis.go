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

// Package projectanalysis owns terminal project processing.
package projectanalysis

import (
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	projectissuescore "github.com/uber/submitqueue/stovepipe/core/projectissues"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/extension/projectissues"
	"github.com/uber/submitqueue/stovepipe/extension/projectstatus"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

type Controller struct {
	logger            *zap.SugaredLogger
	scope             tally.Scope
	stores            storage.Factory
	materializer      requestlog.Materializer
	issueMaterializer projectissuescore.Materializer
	resolvers         projectstatus.ResolverFactory
	attributers       projectissues.AttributerFactory
	registry          consumer.TopicRegistry
	topicKey          consumer.TopicKey
	consumerGroup     string
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	resolvers projectstatus.ResolverFactory,
	attributers projectissues.AttributerFactory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:            logger.Named("projectanalysis_controller"),
		scope:             scope.SubScope("projectanalysis_controller"),
		stores:            stores,
		materializer:      materializer,
		issueMaterializer: projectissuescore.NewMaterializer(scope),
		resolvers:         resolvers,
		attributers:       attributers,
		registry:          registry,
		topicKey:          topicKey,
		consumerGroup:     consumerGroup,
	}
}

func decodeProjectAnalysis(msg entityqueue.Message) (*stovepipemq.ProjectAnalysis, error) {
	analysis := &stovepipemq.ProjectAnalysis{}
	if err := stovepipemq.Unmarshal(msg.Payload, analysis); err != nil {
		return nil, fmt.Errorf("deserialize project analysis: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, analysis.GetQueueName()); err != nil {
		return nil, fmt.Errorf("invalid project analysis identity: %w", err)
	}
	if err := id.Validate(analysis.GetId()); err != nil {
		return nil, fmt.Errorf("invalid project analysis request ID: %w", err)
	}
	if msg.PartitionKey != analysis.GetId() {
		return nil, fmt.Errorf("project analysis must be partitioned by request ID")
	}
	return analysis, nil
}

func (c *Controller) Name() string                { return "projectanalysis" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }
