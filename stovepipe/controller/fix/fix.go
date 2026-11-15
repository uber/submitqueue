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

// Package fix submits build-scoped repair work.
package fix

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const buildIssuePageSize = 100

type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	materializer  requestlog.Materializer
	executors     fixer.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	executors fixer.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("fix_controller"),
		metricsScope:  scope.SubScope("fix_controller"),
		stores:        stores,
		materializer:  materializer,
		executors:     executors,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

func (c *Controller) processBuild(ctx context.Context, queueName, buildID string) error {
	store, err := c.stores.For(storage.Config{QueueName: queueName})
	if err != nil {
		return fmt.Errorf("resolve repair storage: %w", err)
	}
	request, err := loadRequestForBuild(ctx, store, queueName, buildID)
	if err != nil {
		return err
	}
	registrations, err := c.executors.For(fixer.Config{QueueName: queueName})
	if err != nil {
		return fmt.Errorf("resolve repair executors: %w", err)
	}
	if err := validateRegistrations(registrations); err != nil {
		return err
	}
	observations, issues, err := collectIssues(ctx, store, buildID)
	if err != nil {
		return err
	}
	var failures []error
	for _, registration := range registrations {
		if err := c.submitIssues(ctx, store, request, buildID, registration, observations, issues); err != nil {
			c.logger.Errorw("repair executor failed", "queue", queueName, "build_id", buildID, "executor_id", registration.ID, "error", err)
			failures = append(failures, fmt.Errorf("executor %q: %w", registration.ID, err))
		}
	}
	return errs.Group(failures...)
}

func validateRegistrations(registrations []fixer.Registration) error {
	seen := map[string]bool{}
	for _, registration := range registrations {
		if registration.ID == "" || len(registration.ID) > 128 || registration.Executor == nil || seen[registration.ID] {
			return fmt.Errorf("repair executors require unique nonempty IDs (at most 128 bytes) and implementations")
		}
		seen[registration.ID] = true
	}
	return nil
}

func (c *Controller) Name() string { return "fix" }

func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }

func (c *Controller) ConsumerGroup() string { return c.consumerGroup }

func collectIssues(ctx context.Context, store storage.Storage, buildID string) ([]entity.BuildIssue, []entity.OpenIssue, error) {
	bounds := storage.BuildIssueRange{Limit: buildIssuePageSize}
	var observations []entity.BuildIssue
	var eligible []entity.OpenIssue
	for {
		page, err := store.GetBuildIssueStore().List(ctx, buildID, bounds)
		if err != nil {
			return nil, nil, err
		}
		if len(page) == 0 {
			return observations, eligible, nil
		}
		for _, record := range page {
			observations = append(observations, record)
			issue, err := store.GetIssueStateStore().Get(ctx, record.TaskID, record.UnitID)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			if issue.Status == entity.IssueStatusOpen && issue.FirstSeenBuildID == buildID {
				eligible = append(eligible, entity.OpenIssue{TaskID: record.TaskID, UnitID: record.UnitID, FirstSeenBuildID: buildID})
			}
		}
		last := page[len(page)-1]
		bounds.After = storage.BuildIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
	}
}
