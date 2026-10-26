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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
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
		return fmt.Errorf("failed to resolve repair storage: %w", err)
	}
	observation, found, err := selectRepairObservation(ctx, store.GetBuildIssueStore(), store.GetOpenIssueStore(), buildID)
	if err != nil {
		return fmt.Errorf("failed to select an issue for build %q: %w", buildID, err)
	}
	if !found {
		return nil
	}
	if observation.FixID != "" {
		return c.persistRepairSubmittedLog(ctx, store, queueName, buildID, observation)
	}

	executor, err := c.executors.For(fixer.Config{QueueName: queueName})
	if err != nil {
		return fmt.Errorf("failed to resolve repair executor: %w", err)
	}

	op := metrics.Begin(c.metricsScope, "submit", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	issue := entity.OpenIssue{TaskID: observation.TaskID, UnitID: observation.UnitID, FirstSeenBuildID: buildID}
	fixID, err := executor.Submit(ctx, issue)
	if err == nil && fixID == "" {
		err = fmt.Errorf("repair executor returned an empty fix ID")
	}
	op.Complete(err)
	if err != nil {
		c.logger.Errorw("repair submission failed",
			"queue", queueName, "build_id", buildID,
			"task_id", issue.TaskID, "unit_id", issue.UnitID,
			"fix_id", fixID, "error", err)
		if fixID == "" {
			return fmt.Errorf("failed to submit repair for build %q: %w", buildID, err)
		}
	} else {
		c.logger.Infow("repair submitted",
			"queue", queueName, "build_id", buildID,
			"task_id", issue.TaskID, "unit_id", issue.UnitID, "fix_id", fixID)
	}

	// Submission precedes persistence; a crash or write failure can allow duplicates.
	updated := observation
	updated.FixID = fixID
	observations := store.GetBuildIssueStore()
	if err := observations.Update(ctx, buildID, updated, observation.Version, observation.Version+1); err != nil {
		if !errors.Is(err, storage.ErrVersionMismatch) {
			return fmt.Errorf("failed to associate repair with build issue: %w", err)
		}
		current, readErr := observations.Get(ctx, buildID, observation.TaskID, observation.UnitID)
		if readErr != nil {
			return fmt.Errorf("failed to reload build issue after concurrent update: %w", readErr)
		}
		if current.FixID == "" {
			return err
		}
		updated = current
		c.logger.Warnw("preserving previously recorded repair association",
			"queue", queueName, "build_id", buildID,
			"task_id", issue.TaskID, "unit_id", issue.UnitID,
			"fix_id", current.FixID, "submitted_fix_id", fixID)
	}
	return c.persistRepairSubmittedLog(ctx, store, queueName, buildID, updated)
}

func (c *Controller) Name() string { return "fix" }

func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }

func (c *Controller) ConsumerGroup() string { return c.consumerGroup }

func selectRepairObservation(ctx context.Context, observations storage.BuildIssueStore, openIssues storage.OpenIssueStore, buildID string) (entity.BuildIssue, bool, error) {
	bounds := storage.BuildIssueRange{Limit: buildIssuePageSize}
	var candidate entity.BuildIssue
	found := false
	for {
		page, err := observations.List(ctx, buildID, bounds)
		if err != nil {
			return entity.BuildIssue{}, false, err
		}
		if len(page) == 0 {
			return candidate, found, nil
		}
		for _, record := range page {
			if record.FixID != "" {
				return record, true, nil
			}
			if found {
				continue
			}
			issue, err := openIssues.Get(ctx, record.TaskID, record.UnitID)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return entity.BuildIssue{}, false, err
			}
			if issue.FirstSeenBuildID == buildID {
				candidate, found = record, true
			}
		}
		last := page[len(page)-1]
		bounds.After = storage.BuildIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
	}
}

func (c *Controller) persistRepairSubmittedLog(ctx context.Context, store storage.Storage, queueName, buildID string, observation entity.BuildIssue) error {
	build, err := store.GetBuildStore().Get(ctx, buildID)
	if err != nil {
		return fmt.Errorf("failed to load repaired build %s: %w", buildID, err)
	}
	request, err := store.GetRequestStore().Get(ctx, build.RequestID)
	if err != nil {
		return fmt.Errorf("failed to load repaired request %s: %w", build.RequestID, err)
	}
	if request.Queue != queueName {
		return fmt.Errorf("request queue %q does not match repair queue %q", request.Queue, queueName)
	}
	// Distinct repair associations need distinct occurrences; hashing bounds the composite ID.
	identity := sha256.Sum256([]byte(fmt.Sprintf("%q/%q/%q/%q", buildID, observation.TaskID, observation.UnitID, observation.FixID)))
	log := requestlog.NewRequestEventLog(request, entity.RequestEventRepairSubmitted, hex.EncodeToString(identity[:]),
		map[string]string{
			requestlog.MetadataKeyBuildID: buildID,
			requestlog.MetadataKeyFixID:   observation.FixID,
			requestlog.MetadataKeyTaskID:  observation.TaskID,
			requestlog.MetadataKeyUnitID:  observation.UnitID,
		})
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("failed to retain submitted repair for request %s: %w", request.ID, err)
	}
	return nil
}
