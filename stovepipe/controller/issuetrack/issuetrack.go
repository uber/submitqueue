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
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

type issueKey struct{ task, unit string }

// Controller retains issue transitions and their queue-scoped progress.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
	nowMs         func() int64
}

func NewController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, topicKey consumer.TopicKey, consumerGroup string) *Controller {
	return &Controller{
		logger:        logger.Named("issuetrack_controller"),
		metricsScope:  scope.SubScope("issuetrack_controller"),
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		nowMs:         func() int64 { return time.Now().UnixMilli() },
	}
}

func (c *Controller) processIssue(ctx context.Context, store storage.Storage, progress entity.IssueTrackingState, key issueKey, reported bool) (entity.IssueTrackingState, error) {
	if err := ctx.Err(); err != nil {
		return entity.IssueTrackingState{}, err
	}
	if !reported {
		_, err := store.GetBuildIssueStore().Get(ctx, progress.BuildID, key.task, key.unit)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return entity.IssueTrackingState{}, fmt.Errorf("check reported issue: %w", err)
		}
		reported = err == nil
	}

	if !reported && progress.Phase != entity.IssueTrackingPhaseFinalizing {
		return entity.IssueTrackingState{}, fmt.Errorf("refresh cursor requires a reported issue")
	}

	desired := entity.IssueStatusClosed
	if reported {
		desired = entity.IssueStatusOpen
	}

	issues := store.GetIssueStateStore()
	current, err := issues.Get(ctx, key.task, key.unit)
	missing := errors.Is(err, storage.ErrNotFound)
	if err != nil && !missing {
		return entity.IssueTrackingState{}, fmt.Errorf("read issue state: %w", err)
	}

	if missing && !reported {
		return entity.IssueTrackingState{}, fmt.Errorf("closing cursor requires an existing issue")
	}

	if !missing && current.LastProcessedBuildID != progress.BuildID {
		previous, err := loader.ByID(ctx, current.LastProcessedBuildID, store.GetBuildStore().Get, "last processed build")
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		order, err := entity.CompareRequestID(progress.RequestID, previous.RequestID)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		if order < 0 {
			return entity.IssueTrackingState{}, fmt.Errorf("issue already processed a newer source: %w", storage.ErrVersionMismatch)
		}
	}

	next := progress
	next.CursorTaskID, next.CursorUnitID = key.task, key.unit
	if !missing && current.LastProcessedBuildID == progress.BuildID && current.Status == desired {
		if key == (issueKey{progress.CursorTaskID, progress.CursorUnitID}) {
			return next, c.recordHistory(ctx, store, current)
		}
		return next, nil
	}

	// Read the issue before this CAS, including when resuming a cursor. A
	// takeover must finish that key before advancing, invalidating this row guard.
	next, err = saveProgress(ctx, store.GetIssueTrackingStore(), next)
	if err != nil {
		return entity.IssueTrackingState{}, err
	}

	if !missing {
		if current.LastProcessedBuildID == progress.BuildID {
			if err := c.recordHistory(ctx, store, current); err != nil {
				return entity.IssueTrackingState{}, err
			}
		}
		if current.Version == math.MaxInt32 {
			return entity.IssueTrackingState{}, fmt.Errorf("issue state version exhausted")
		}
	}

	target := current
	target.TaskID, target.UnitID = key.task, key.unit
	target.LastProcessedBuildID = progress.BuildID
	target.Status = desired
	target.Version = current.Version + 1
	if missing || current.Status != desired {
		target.LastChangedBuildID = progress.BuildID
		target.ChangedAtMs = c.nowMs()
		if desired == entity.IssueStatusOpen {
			target.FirstSeenBuildID = progress.BuildID
		}
	}

	if missing {
		err = issues.Create(ctx, target)
	} else {
		err = issues.Update(ctx, target, current.Version, target.Version)
	}
	if errors.Is(err, storage.ErrAlreadyExists) {
		return entity.IssueTrackingState{}, fmt.Errorf("issue was concurrently created: %w", storage.ErrVersionMismatch)
	}
	if err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("apply issue cursor: %w", err)
	}

	if missing || current.Status != desired {
		return next, c.recordHistory(ctx, store, target)
	}
	return next, nil
}

func (c *Controller) recordHistory(ctx context.Context, store storage.Storage, issue entity.IssueState) error {
	event := entity.IssueEventOpened
	if issue.Status == entity.IssueStatusClosed {
		event = entity.IssueEventResolved
	}

	log := entity.IssueLog{TaskID: issue.TaskID, UnitID: issue.UnitID, BuildID: issue.LastChangedBuildID, Event: event, TimestampMs: issue.ChangedAtMs}
	if err := store.GetIssueLogStore().Create(ctx, log); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("record issue transition: %w", err)
	}
	return nil
}

func saveProgress(ctx context.Context, store storage.IssueTrackingStore, next entity.IssueTrackingState) (entity.IssueTrackingState, error) {
	if next.Version == math.MaxInt32 {
		return entity.IssueTrackingState{}, fmt.Errorf("issue tracking version exhausted")
	}
	newVersion := next.Version + 1
	if err := store.Update(ctx, next, next.Version, newVersion); err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("checkpoint issue tracking: %w", err)
	}
	next.Version = newVersion
	return next, nil
}

func (c *Controller) Name() string                { return "issuetrack" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }
