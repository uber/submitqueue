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
	"errors"
	"fmt"
	"math"

	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/fixer"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func (c *Controller) submitIssues(ctx context.Context, store storage.Storage, request entity.Request, buildID string,
	registration fixer.Registration, observations []entity.BuildIssue, eligible []entity.OpenIssue) error {
	pending, recorded, prepareErr := prepareIssueRepairs(ctx, store.GetIssueRepairStore(), buildID, registration.ID, observations, eligible)
	historyErr := c.persistRepairHistory(ctx, store, request, recorded)
	if prepareErr != nil || len(pending) == 0 {
		return errs.Group(prepareErr, historyErr)
	}

	op := metrics.Begin(c.metricsScope, "submit", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	submissions, submitErr := registration.Executor.Submit(ctx, append([]entity.OpenIssue(nil), pending...))
	op.Complete(submitErr)
	recordErr := c.recordRepairSubmissions(ctx, store, request, registration.ID, pending, submissions, submitErr == nil)
	if submitErr != nil {
		submitErr = fmt.Errorf("submit pending repairs: %w", submitErr)
	}
	return errs.Group(historyErr, submitErr, recordErr)
}

func prepareIssueRepairs(ctx context.Context, repairs storage.IssueRepairStore, buildID, executorID string,
	observations []entity.BuildIssue, eligible []entity.OpenIssue) ([]entity.OpenIssue, []entity.IssueRepair, error) {
	pending := make([]entity.OpenIssue, 0, len(eligible))
	var recorded []entity.IssueRepair
	eligibleSet := make(map[entity.OpenIssue]bool, len(eligible))
	for _, issue := range eligible {
		eligibleSet[issue] = true
	}
	// Restore durable history even for issues that have since closed.
	for _, observation := range observations {
		issue := entity.OpenIssue{TaskID: observation.TaskID, UnitID: observation.UnitID, FirstSeenBuildID: buildID}
		result, err := repairs.Get(ctx, issue, executorID)
		if errors.Is(err, storage.ErrNotFound) {
			if !eligibleSet[issue] {
				continue
			}
			result = entity.IssueRepair{Issue: issue, ExecutorID: executorID, Version: 1}
			err = repairs.Create(ctx, result)
			if errors.Is(err, storage.ErrAlreadyExists) {
				result, err = repairs.Get(ctx, issue, executorID)
			}
			if err != nil {
				return nil, recorded, fmt.Errorf("ensure pending repair for task %q unit %q: %w", issue.TaskID, issue.UnitID, err)
			}
		}
		if err != nil {
			return nil, recorded, fmt.Errorf("load repair for task %q unit %q: %w", issue.TaskID, issue.UnitID, err)
		}
		if result.FixID != "" || result.SkipReason != "" {
			recorded = append(recorded, result)
		} else if eligibleSet[issue] {
			pending = append(pending, issue)
		}
	}
	return pending, recorded, nil
}

func (c *Controller) recordRepairSubmissions(ctx context.Context, store storage.Storage, request entity.Request, executorID string,
	pending []entity.OpenIssue, submissions []fixer.Submission, requireAll bool) error {
	allowed := make(map[entity.OpenIssue]bool, len(pending))
	for _, issue := range pending {
		allowed[issue] = true
	}
	var failures []error
	var recorded []entity.IssueRepair
	seen := make(map[entity.OpenIssue]bool, len(submissions))
	for _, submission := range submissions {
		var validationErr error
		switch {
		case !allowed[submission.Issue]:
			validationErr = fmt.Errorf("issue was not submitted")
		case seen[submission.Issue]:
			validationErr = fmt.Errorf("duplicate receipt")
		case (submission.FixID == "") == (submission.SkipReason == ""):
			validationErr = fmt.Errorf("receipt must contain exactly one of fix ID or skip reason")
		case len(submission.FixID) > 255 || len(submission.SkipReason) > 255:
			validationErr = fmt.Errorf("fix ID and skip reason must be at most 255 bytes")
		}
		if validationErr != nil {
			failures = append(failures, fmt.Errorf("invalid repair submission for task %q unit %q: %w", submission.Issue.TaskID, submission.Issue.UnitID, validationErr))
			continue
		}
		seen[submission.Issue] = true
		record, err := persistRepairSubmission(ctx, store.GetIssueRepairStore(), executorID, submission)
		if err != nil {
			failures = append(failures, fmt.Errorf("record repair submission for task %q unit %q: %w", submission.Issue.TaskID, submission.Issue.UnitID, err))
		}
		// A conflicting receipt still returns the durable outcome for history recovery.
		if record.FixID != "" || record.SkipReason != "" {
			recorded = append(recorded, record)
		}
	}
	if requireAll && len(seen) < len(pending) {
		failures = append(failures, fmt.Errorf("repair executor omitted %d pending items", len(pending)-len(seen)))
	}
	failures = append(failures, c.persistRepairHistory(ctx, store, request, recorded))
	return errs.Group(failures...)
}

func (c *Controller) persistRepairHistory(ctx context.Context, store storage.Storage, request entity.Request, recorded []entity.IssueRepair) error {
	var failures []error
	for _, repair := range recorded {
		if err := persistRepairSubmissionLog(ctx, c.materializer, store, request, repair); err != nil {
			failures = append(failures, fmt.Errorf("persist repair history for task %q unit %q: %w", repair.Issue.TaskID, repair.Issue.UnitID, err))
		}
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
			return entity.IssueRepair{}, fmt.Errorf("reload repair: %w", err)
		}
		if current.FixID != "" || current.SkipReason != "" {
			if current.FixID != submission.FixID || current.SkipReason != submission.SkipReason {
				return current, fmt.Errorf("executor returned conflicting repair disposition")
			}
			return current, nil
		}
		if current.Version == math.MaxInt32 {
			return entity.IssueRepair{}, fmt.Errorf("repair version exhausted")
		}
		replacement := current
		replacement.FixID, replacement.SkipReason = submission.FixID, submission.SkipReason
		newVersion := current.Version + 1
		if err := repairs.Update(ctx, replacement, current.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return entity.IssueRepair{}, fmt.Errorf("update repair: %w", err)
		}
		replacement.Version = newVersion
		return replacement, nil
	}
}
