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

// Package issuetrack maintains issue state and lifecycle history from build issues.
package issuetrack

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller retains issue transitions and their queue-scoped progress.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
}

func NewController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, topicKey consumer.TopicKey, consumerGroup string) *Controller {
	return &Controller{
		logger:        logger.Named("issuetrack_controller"),
		metricsScope:  scope.SubScope("issuetrack_controller"),
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

func (c *Controller) applyPlannedChange(ctx context.Context, store storage.Storage, progress entity.IssueTrackingState) (checkpoint entity.IssueTrackingState, retErr error) {
	op := metrics.Begin(c.metricsScope, "apply_change", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := ctx.Err(); err != nil {
		return entity.IssueTrackingState{}, err
	}
	if err := progress.Validate(); err != nil {
		return entity.IssueTrackingState{}, err
	}
	if !progress.HasPlannedChange() {
		return entity.IssueTrackingState{}, fmt.Errorf("issue tracking has no saved change")
	}
	if progress.Version == math.MaxInt32 {
		return entity.IssueTrackingState{}, fmt.Errorf("issue tracking version exhausted")
	}

	change := progress.PlannedChange
	target := entity.IssueState{
		TaskID:           progress.CursorTaskID,
		UnitID:           progress.CursorUnitID,
		Status:           change.Status,
		FirstSeenBuildID: change.FirstSeenBuildID,
		Version:          change.ExpectedVersion + 1,
	}
	issues := store.GetIssueStateStore()
	var err error
	if change.ExpectedVersion == 0 {
		err = issues.Create(ctx, target)
	} else {
		err = issues.Update(ctx, target, change.ExpectedVersion, target.Version)
	}
	if errors.Is(err, storage.ErrAlreadyExists) || errors.Is(err, storage.ErrVersionMismatch) {
		retained, readErr := issues.Get(ctx, progress.CursorTaskID, progress.CursorUnitID)
		if readErr != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("read planned issue change result: %w", readErr)
		}
		// A retry may finish history, but must never rebase the planned change onto a newer version.
		if retained != target {
			return entity.IssueTrackingState{}, fmt.Errorf("planned issue change has been superseded: %w", storage.ErrVersionMismatch)
		}
	} else if err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("apply planned issue change: %w", err)
	}

	event := entity.IssueEventOpened
	if change.Status == entity.IssueStatusClosed {
		event = entity.IssueEventResolved
	}
	log := entity.IssueLog{TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID, BuildID: progress.BuildID, Event: event, TimestampMs: change.TimestampMs}
	if err := store.GetIssueLogStore().Create(ctx, log); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return entity.IssueTrackingState{}, fmt.Errorf("record issue transition: %w", err)
	}

	next := progress
	next.PlannedChange = entity.PlannedIssueChange{}
	newVersion := progress.Version + 1
	if err := store.GetIssueTrackingStore().Update(ctx, next, progress.Version, newVersion); err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("complete planned issue change: %w", err)
	}
	next.Version = newVersion
	return next, nil
}

func (c *Controller) Name() string                { return "issuetrack" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }
