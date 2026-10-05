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

package controller

import (
	"context"
	"fmt"
	"sort"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queueconfig"
	"go.uber.org/zap"
)

type ListQueuesController interface {
	// ListQueues includes queues with no requests, ordered by name ascending.
	ListQueues(ctx context.Context) ([]entity.QueueConfig, error)
}

var _ ListQueuesController = (*listQueuesController)(nil)

type listQueuesController struct {
	logger       *zap.SugaredLogger
	metricsScope tally.Scope
	queueConfigs queueconfig.Store
}

func NewListQueuesController(logger *zap.SugaredLogger, scope tally.Scope, queueConfigs queueconfig.Store) ListQueuesController {
	return &listQueuesController{
		logger:       logger,
		metricsScope: scope.SubScope("list_queues_controller"),
		queueConfigs: queueConfigs,
	}
}

func (c *listQueuesController) ListQueues(ctx context.Context) (_ []entity.QueueConfig, retErr error) {
	op := metrics.Begin(c.metricsScope, "list_queues", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	configuredQueues, err := c.queueConfigs.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list configured queues: %w", err)
	}
	queues := append([]entity.QueueConfig{}, configuredQueues...)
	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

	c.logger.Debugw("configured queues listed", "queue_count", len(queues))
	return queues, nil
}
