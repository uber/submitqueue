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
	"github.com/uber/submitqueue/platform/metrics"
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
	topicKey      consumer.TopicKey
	consumerGroup string
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	materializer requestlog.Materializer,
	executors fixer.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("fix_controller"),
		metricsScope:  scope.SubScope("fix_controller"),
		stores:        stores,
		materializer:  materializer,
		executors:     executors,
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

func (c *Controller) submitIssues(ctx context.Context, store storage.Storage, request entity.Request, buildID string,
	registration fixer.Registration, observations []entity.BuildIssue, eligible []entity.OpenIssue) error {
	repairs := store.GetIssueRepairStore()
	pending := make([]entity.OpenIssue, 0, len(eligible))
	eligibleSet := make(map[entity.OpenIssue]bool, len(eligible))
	for _, issue := range eligible {
		eligibleSet[issue] = true
	}
	var failures []error
	// Restore history for durable submissions even if their issues have since closed.
	for _, observation := range observations {
		issue := entity.OpenIssue{TaskID: observation.TaskID, UnitID: observation.UnitID, FirstSeenBuildID: buildID}
		result, err := repairs.Get(ctx, issue, registration.ID)
		if errors.Is(err, storage.ErrNotFound) {
			if !eligibleSet[issue] {
				continue
			}
			result = entity.IssueRepair{Issue: issue, ExecutorID: registration.ID, Version: 1}
			err = repairs.Create(ctx, result)
			if errors.Is(err, storage.ErrAlreadyExists) {
				result, err = repairs.Get(ctx, issue, registration.ID)
			}
		}
		if err != nil {
			return err
		}
		if result.FixID == "" && result.SkipReason == "" {
			if eligibleSet[issue] {
				pending = append(pending, issue)
			}
			continue
		}
		if err := persistRepairSubmissionLog(ctx, c.materializer, store, request, result); err != nil {
			failures = append(failures, err)
		}
	}
	if len(pending) == 0 {
		return errs.Group(failures...)
	}
	op := metrics.Begin(c.metricsScope, "submit", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	submissions, submitErr := registration.Executor.Submit(ctx, append([]entity.OpenIssue(nil), pending...))
	op.Complete(submitErr)
	if submitErr != nil {
		failures = append(failures, submitErr)
	}
	allowed := make(map[entity.OpenIssue]bool, len(pending))
	for _, issue := range pending {
		allowed[issue] = true
	}
	seen := make(map[entity.OpenIssue]bool, len(submissions))
	for _, submission := range submissions {
		if !allowed[submission.Issue] || seen[submission.Issue] || (submission.FixID == "") == (submission.SkipReason == "") || len(submission.FixID) > 255 || len(submission.SkipReason) > 255 {
			failures = append(failures, fmt.Errorf("invalid or duplicate repair submission for task %q unit %q", submission.Issue.TaskID, submission.Issue.UnitID))
			continue
		}
		seen[submission.Issue] = true
		record, err := persistRepairSubmission(ctx, repairs, registration.ID, submission)
		if err != nil {
			failures = append(failures, err)
			if record.FixID == "" && record.SkipReason == "" {
				continue
			}
		}
		if err := persistRepairSubmissionLog(ctx, c.materializer, store, request, record); err != nil {
			failures = append(failures, err)
		}
	}
	if len(seen) < len(pending) && submitErr == nil {
		failures = append(failures, fmt.Errorf("repair executor omitted %d pending items", len(pending)-len(seen)))
	}
	return errs.Group(failures...)
}

func persistRepairSubmission(ctx context.Context, repairs storage.IssueRepairStore, executorID string, submission fixer.Submission) (entity.IssueRepair, error) {
	for {
		if err := ctx.Err(); err != nil {
			return entity.IssueRepair{}, err
		}
		current, err := repairs.Get(ctx, submission.Issue, executorID)
		if err != nil {
			return entity.IssueRepair{}, err
		}
		if current.FixID != "" || current.SkipReason != "" {
			if current.FixID != submission.FixID || current.SkipReason != submission.SkipReason {
				return current, fmt.Errorf("executor returned conflicting repair disposition")
			}
			return current, nil
		}
		replacement := current
		replacement.FixID, replacement.SkipReason = submission.FixID, submission.SkipReason
		newVersion := current.Version + 1
		if err := repairs.Update(ctx, replacement, current.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return entity.IssueRepair{}, err
		}
		replacement.Version = newVersion
		return replacement, nil
	}
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
