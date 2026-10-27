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
	"sort"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Each delivery scans at most this many distinct task/unit keys, including closed records.
const _issueBatchSize = 100

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

func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	notification := &stovepipemq.IssueTrack{}
	if err := stovepipemq.Unmarshal(msg.Payload, notification); err != nil {
		return fmt.Errorf("deserialize issue tracking notification: %w", err)
	}
	queue := notification.GetQueueName()
	if err := entityqueue.ValidatePayloadQueue(msg, queue); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	if msg.ID == "" || queue == "" || notification.GetBuildId() == "" {
		return fmt.Errorf("issue tracking requires message, queue, and build identifiers")
	}
	if msg.PartitionKey != queue {
		return fmt.Errorf("issue tracking must be partitioned by queue %q", queue)
	}
	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve issue tracking storage: %w", err)
	}
	return c.trackBuild(ctx, delivery, store, notification, msg.ID)
}

func (c *Controller) trackBuild(ctx context.Context, delivery consumer.Delivery, store storage.Storage, notification *stovepipemq.IssueTrack, notificationID string) error {
	queue := notification.GetQueueName()
	build, request, err := loadBuildAndRequest(ctx, store, queue, notification.GetBuildId())
	if err != nil {
		return err
	}

	tracking := store.GetIssueTrackingStore()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		progress, err := tracking.Get(ctx)
		if errors.Is(err, storage.ErrNotFound) {
			progress = entity.IssueTrackingState{}
		} else if err != nil {
			return fmt.Errorf("load issue tracking progress: %w", err)
		}

		if progress.Phase != entity.IssueTrackingPhaseRefreshing && progress.Phase != entity.IssueTrackingPhaseFinalizing {
			if request.TerminalBuildID != "" && request.TerminalBuildID != build.ID {
				return nil
			}
			if progress.Version != 0 {
				if progress.NotificationID == notificationID {
					return nil
				}
				order, err := entity.CompareRequestID(request.ID, progress.RequestID)
				if err != nil {
					return err
				}
				if order < 0 || (order == 0 && progress.Phase == entity.IssueTrackingPhaseFinalized) {
					c.logger.Debugw("ignoring superseded issue tracking notification",
						"queue", queue, "build_id", build.ID, "request_id", request.ID, "current_request_id", progress.RequestID)
					return nil
				}
			}

			if progress.Version == math.MaxInt32 {
				return fmt.Errorf("issue tracking version exhausted")
			}
			next := entity.IssueTrackingState{
				RequestID:      request.ID,
				BuildID:        build.ID,
				NotificationID: notificationID,
				Phase:          entity.IssueTrackingPhaseRefreshing,
				Version:        progress.Version + 1,
			}
			if notification.GetFinalize() {
				if !build.Status.IsTerminal() || request.TerminalBuildID != build.ID {
					return fmt.Errorf("final issue tracking requires the request's terminal build")
				}
				next.Phase = entity.IssueTrackingPhaseFinalizing
			}

			if progress.Version == 0 {
				err = tracking.Create(ctx, next)
			} else {
				err = tracking.Update(ctx, next, progress.Version, next.Version)
			}
			if errors.Is(err, storage.ErrAlreadyExists) || errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			if err != nil {
				return fmt.Errorf("start issue tracking pass: %w", err)
			}
			progress = next
		}

		checkpoint, err := c.reconcileBatch(ctx, store, queue, progress)
		if err != nil {
			return err
		}
		if checkpoint.NotificationID != notificationID ||
			checkpoint.Phase == entity.IssueTrackingPhaseRefreshing ||
			checkpoint.Phase == entity.IssueTrackingPhaseFinalizing {
			delivery.Hold(0)
		}
		return nil
	}
}

func (c *Controller) reconcileBatch(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState) (checkpoint entity.IssueTrackingState, retErr error) {
	op := metrics.Begin(c.metricsScope, "batch", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	limit := _issueBatchSize
	if progress.HasPlannedChange() {
		next, err := c.applyPlannedChange(ctx, store, progress)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		progress = next
		limit--
	}

	build, request, err := loadBuildAndRequest(ctx, store, queue, progress.BuildID)
	if err != nil {
		return entity.IssueTrackingState{}, err
	}
	switch progress.Phase {
	case entity.IssueTrackingPhaseRefreshing, entity.IssueTrackingPhaseFinalizing:
	default:
		return entity.IssueTrackingState{}, fmt.Errorf("issue batch requires a refreshing or finalizing pass")
	}
	if request.ID != progress.RequestID {
		return entity.IssueTrackingState{}, fmt.Errorf("issue batch does not match its source request")
	}
	finalizing := progress.Phase == entity.IssueTrackingPhaseFinalizing
	if finalizing && (!build.Status.IsTerminal() || request.TerminalBuildID != build.ID) {
		return entity.IssueTrackingState{}, fmt.Errorf("final issue tracking requires the request's terminal build")
	}

	buildIssues, err := store.GetBuildIssueStore().List(ctx, build.ID, storage.BuildIssueRange{
		After: storage.BuildIssueCursor{TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID},
		Limit: limit,
	})
	if err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("list build issues: %w", err)
	}

	var states []entity.IssueState
	// Cancellation may have skipped validation, so absence cannot close an existing issue.
	if finalizing && build.Status != entity.BuildStatusCancelled {
		states, err = store.GetIssueStateStore().List(ctx, storage.IssueStateRange{
			After: storage.IssueStateCursor{TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID},
			Limit: limit,
		})
		if err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("list issue states: %w", err)
		}
	}

	reported := make(map[issueKey]bool, len(buildIssues)+len(states))
	for _, issue := range states {
		reported[issueKey{issue.TaskID, issue.UnitID}] = false
	}
	for _, issue := range buildIssues {
		reported[issueKey{issue.TaskID, issue.UnitID}] = true
	}
	keys := make([]issueKey, 0, len(reported))
	for key := range reported {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].task != keys[j].task {
			return keys[i].task < keys[j].task
		}
		return keys[i].unit < keys[j].unit
	})
	complete := len(buildIssues) < limit && len(states) < limit && len(keys) <= limit
	if len(keys) > limit {
		keys = keys[:limit]
	}

	cursorChanged := false
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return entity.IssueTrackingState{}, err
		}
		change, err := c.planIssueChange(ctx, store, progress, key, reported[key])
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		if change == (entity.PlannedIssueChange{}) {
			progress.CursorTaskID, progress.CursorUnitID = key.task, key.unit
			cursorChanged = true
			continue
		}

		next := progress
		next.CursorTaskID, next.CursorUnitID = key.task, key.unit
		next.PlannedChange = change
		progress, err = saveProgress(ctx, store.GetIssueTrackingStore(), next)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		progress, err = c.applyPlannedChange(ctx, store, progress)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		cursorChanged = false
	}

	if complete {
		next := progress
		next.Phase = entity.IssueTrackingPhaseRefreshed
		if finalizing {
			next.Phase = entity.IssueTrackingPhaseFinalized
		}
		return saveProgress(ctx, store.GetIssueTrackingStore(), next)
	}
	if cursorChanged {
		return saveProgress(ctx, store.GetIssueTrackingStore(), progress)
	}

	return progress, nil
}

func (c *Controller) planIssueChange(ctx context.Context, store storage.Storage, progress entity.IssueTrackingState, key issueKey, reported bool) (entity.PlannedIssueChange, error) {
	if !reported {
		_, err := store.GetBuildIssueStore().Get(ctx, progress.BuildID, key.task, key.unit)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return entity.PlannedIssueChange{}, fmt.Errorf("check build issue before closing: %w", err)
		}
		reported = err == nil
	}

	desired := entity.IssueStatusClosed
	if reported {
		desired = entity.IssueStatusOpen
	}

	current, err := store.GetIssueStateStore().Get(ctx, key.task, key.unit)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return entity.PlannedIssueChange{}, fmt.Errorf("read issue state: %w", err)
	}
	if (errors.Is(err, storage.ErrNotFound) && desired == entity.IssueStatusClosed) || current.Status == desired {
		return entity.PlannedIssueChange{}, nil
	}

	firstSeen := current.FirstSeenBuildID
	if desired == entity.IssueStatusOpen {
		firstSeen = progress.BuildID
	}
	change := entity.PlannedIssueChange{
		Status:           desired,
		FirstSeenBuildID: firstSeen,
		ExpectedVersion:  current.Version,
		TimestampMs:      c.nowMs(),
	}
	if err := change.Validate(); err != nil {
		return entity.PlannedIssueChange{}, err
	}
	return change, nil
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

func saveProgress(ctx context.Context, store storage.IssueTrackingStore, next entity.IssueTrackingState) (entity.IssueTrackingState, error) {
	if next.Version == math.MaxInt32 || (next.HasPlannedChange() && next.Version == math.MaxInt32-1) {
		return entity.IssueTrackingState{}, fmt.Errorf("issue tracking version exhausted")
	}
	newVersion := next.Version + 1
	if err := store.Update(ctx, next, next.Version, newVersion); err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("checkpoint issue tracking: %w", err)
	}
	next.Version = newVersion
	return next, nil
}

func loadBuildAndRequest(ctx context.Context, store storage.Storage, queue, buildID string) (entity.Build, entity.Request, error) {
	build, err := loader.ByID(ctx, buildID, store.GetBuildStore().Get, "build")
	if err != nil {
		return entity.Build{}, entity.Request{}, err
	}
	request, err := loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
	if err != nil {
		return entity.Build{}, entity.Request{}, err
	}
	if request.Queue != queue {
		return entity.Build{}, entity.Request{}, fmt.Errorf("issue tracking request belongs to another queue")
	}
	if err := id.Validate(request.ID); err != nil {
		return entity.Build{}, entity.Request{}, fmt.Errorf("invalid issue tracking request ID: %w", err)
	}
	return build, request, nil
}

func (c *Controller) Name() string                { return "issuetrack" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*Controller)(nil)
