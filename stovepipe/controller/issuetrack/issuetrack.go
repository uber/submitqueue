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

// Package issuetrack applies build observations to operational open issues and lifecycle history.
// All deliveries for one queue must share its partition and consumer group.
package issuetrack

import (
	"context"
	"errors"
	"fmt"
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

// Each read is bounded; every pass still traverses the entire relevant key range.
const _issueBatchSize = 100

// Controller is the queue-serialized writer of open issues and issue transitions.
// Accepted pending work is replayed before a newer source can change issue state.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	topicKey      consumer.TopicKey
	consumerGroup string
	nowMs         func() int64
}

func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("issuetrack_controller"),
		metricsScope:  scope.SubScope("issuetrack_controller"),
		stores:        stores,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
		nowMs:         func() int64 { return time.Now().UnixMilli() },
	}
}

func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, "process", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()
	notification := &stovepipemq.IssueTrack{}
	if err := stovepipemq.Unmarshal(msg.Payload, notification); err != nil {
		return fmt.Errorf("deserialize issue tracking notification: %w", err)
	}

	queue := notification.GetQueueName()
	if err := entityqueue.ValidatePayloadQueue(msg, queue); err != nil {
		return fmt.Errorf("invalid message identity: %w", err)
	}
	if queue == "" || notification.GetBuildId() == "" {
		return fmt.Errorf("issue tracking requires queue and build identifiers")
	}
	if msg.PartitionKey != queue {
		return fmt.Errorf("issue tracking must be partitioned by queue %q", queue)
	}

	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve issue tracking storage: %w", err)
	}

	build, request, err := loadBuildAndRequest(ctx, store, queue, notification.GetBuildId())
	if err != nil {
		return err
	}
	if request.TerminalBuildID != "" && request.TerminalBuildID != build.ID {
		return nil
	}

	return c.reconcileBuild(ctx, store, build, request, notification.GetFinalize())
}

func (c *Controller) reconcileBuild(ctx context.Context, store storage.Storage, build entity.Build, request entity.Request, finalize bool) error {
	trackingStore := store.GetIssueTrackingStore()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		progress, err := trackingStore.Get(ctx)
		if errors.Is(err, storage.ErrNotFound) {
			progress = entity.IssueTrackingState{}
		} else if err != nil {
			return fmt.Errorf("load issue tracking progress: %w", err)
		}

		action, err := decideReconciliation(progress, build, request, finalize)
		if err != nil {
			return err
		}
		if action == _ignoreNotification {
			c.logger.Debugw("ignoring superseded issue analysis", "queue", request.Queue,
				"build_id", build.ID, "request_id", request.ID, "current_request_id", progress.RequestID)
			return nil
		}

		pending := progress
		if action == _startRefresh || action == _startFinalization {
			pending = entity.IssueTrackingState{
				RequestID: request.ID,
				BuildID:   build.ID,
				Phase:     entity.IssueTrackingPhaseRefreshing,
				Version:   progress.Version + 1,
			}
			if action == _startFinalization {
				pending.Phase = entity.IssueTrackingPhaseFinalizing
			}

			if progress.Version == 0 {
				err = trackingStore.Create(ctx, pending)
				if errors.Is(err, storage.ErrAlreadyExists) {
					continue
				}
			} else {
				err = trackingStore.Update(ctx, pending, progress.Version, pending.Version)
				if errors.Is(err, storage.ErrVersionMismatch) {
					continue
				}
			}
			if err != nil {
				return fmt.Errorf("start issue reconciliation: %w", err)
			}
		}

		if err := c.applyReconciliation(ctx, store, request.Queue, pending); err != nil {
			return err
		}
		if action != _resumeAndRecheckNotification {
			return nil
		}
	}
}

type reconciliationAction int

const (
	_ignoreNotification reconciliationAction = iota
	_resumeAndFinishNotification
	_resumeAndRecheckNotification
	_startRefresh
	_startFinalization
)

// A zero progress value represents a queue with no accepted reconciliation.
func decideReconciliation(progress entity.IssueTrackingState, build entity.Build, request entity.Request, finalize bool) (reconciliationAction, error) {
	if progress.IsPending() {
		if progress.RequestID != request.ID || progress.BuildID != build.ID {
			return _resumeAndRecheckNotification, nil
		}
		if finalize && progress.Phase == entity.IssueTrackingPhaseRefreshing {
			return _resumeAndRecheckNotification, nil
		}
		return _resumeAndFinishNotification, nil
	}

	if progress.Version != 0 {
		order, err := entity.CompareRequestID(request.ID, progress.RequestID)
		if err != nil {
			return _ignoreNotification, err
		}
		if order < 0 || (order == 0 && progress.Phase == entity.IssueTrackingPhaseFinalized) {
			return _ignoreNotification, nil
		}
	}

	if finalize {
		if err := validateFinalBuild(build, request); err != nil {
			return _ignoreNotification, err
		}
		return _startFinalization, nil
	}
	return _startRefresh, nil
}

func (c *Controller) applyReconciliation(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState) error {
	build, request, err := loadBuildAndRequest(ctx, store, queue, progress.BuildID)
	if err != nil {
		return err
	}
	if request.ID != progress.RequestID {
		return fmt.Errorf("pending issue reconciliation belongs to another request")
	}
	finalizing := progress.Phase == entity.IssueTrackingPhaseFinalizing
	if finalizing {
		if err := validateFinalBuild(build, request); err != nil {
			return err
		}
	}

	if err := c.openBuildIssues(ctx, store, build.ID); err != nil {
		return err
	}

	if finalizing && build.Status != entity.BuildStatusCancelled {
		if err := c.resolveMissingIssues(ctx, store, build.ID); err != nil {
			return err
		}
	}

	completed := progress
	completed.Phase = entity.IssueTrackingPhaseRefreshed
	if finalizing {
		completed.Phase = entity.IssueTrackingPhaseFinalized
	}

	newVersion := progress.Version + 1
	if err := store.GetIssueTrackingStore().Update(ctx, completed, progress.Version, newVersion); err != nil {
		return fmt.Errorf("complete issue reconciliation: %w", err)
	}
	return nil
}

func (c *Controller) openBuildIssues(ctx context.Context, store storage.Storage, buildID string) error {
	bounds := storage.BuildIssueRange{Limit: _issueBatchSize}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		issues, err := store.GetBuildIssueStore().List(ctx, buildID, bounds)
		if err != nil {
			return fmt.Errorf("list build observations: %w", err)
		}
		if len(issues) == 0 {
			return nil
		}

		for _, issue := range issues {
			if err := c.openObservedIssue(ctx, store, buildID, issue); err != nil {
				return err
			}
		}

		last := issues[len(issues)-1]
		bounds.After = storage.BuildIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
	}
}

func (c *Controller) openObservedIssue(ctx context.Context, store storage.Storage, buildID string, issue entity.BuildIssue) error {
	openStore := store.GetOpenIssueStore()
	openIssue, err := openStore.Get(ctx, issue.TaskID, issue.UnitID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("read open issue: %w", err)
	}

	if errors.Is(err, storage.ErrNotFound) {
		openIssue = entity.OpenIssue{
			TaskID:           issue.TaskID,
			UnitID:           issue.UnitID,
			FirstSeenBuildID: buildID,
		}
		err = openStore.Create(ctx, openIssue)
		if errors.Is(err, storage.ErrAlreadyExists) {
			openIssue, err = openStore.Get(ctx, issue.TaskID, issue.UnitID)
		}
		if err != nil {
			return fmt.Errorf("open observed issue: %w", err)
		}
	}

	if openIssue.FirstSeenBuildID == buildID {
		return c.recordIssueEvent(ctx, store, buildID, entity.IssueEventOpened, issue.TaskID, issue.UnitID)
	}
	return nil
}

func (c *Controller) resolveMissingIssues(ctx context.Context, store storage.Storage, buildID string) error {
	bounds := storage.OpenIssueRange{Limit: _issueBatchSize}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		issues, err := store.GetOpenIssueStore().List(ctx, bounds)
		if err != nil {
			return fmt.Errorf("list open issues: %w", err)
		}
		if len(issues) == 0 {
			return nil
		}

		for _, issue := range issues {
			if err := c.resolveIssueIfAbsent(ctx, store, buildID, issue); err != nil {
				return err
			}
		}

		last := issues[len(issues)-1]
		bounds.After = storage.OpenIssueCursor{TaskID: last.TaskID, UnitID: last.UnitID}
	}
}

func (c *Controller) resolveIssueIfAbsent(ctx context.Context, store storage.Storage, buildID string, issue entity.OpenIssue) error {
	_, err := store.GetBuildIssueStore().Get(ctx, buildID, issue.TaskID, issue.UnitID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("check retained build observation: %w", err)
	}

	// Record resolution before deletion so a failed history write leaves the issue retryable.
	if err := c.recordIssueEvent(ctx, store, buildID, entity.IssueEventResolved, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	if err := store.GetOpenIssueStore().Delete(ctx, issue); err != nil {
		return fmt.Errorf("resolve open issue: %w", err)
	}
	return nil
}

func (c *Controller) recordIssueEvent(ctx context.Context, store storage.Storage, buildID string, event entity.IssueEvent, taskID, unitID string) error {
	log := entity.IssueLog{
		TaskID:      taskID,
		UnitID:      unitID,
		BuildID:     buildID,
		Event:       event,
		TimestampMs: c.nowMs(),
	}
	if err := store.GetIssueLogStore().Create(ctx, log); err != nil && !errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("record issue transition: %w", err)
	}
	return nil
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

func validateFinalBuild(build entity.Build, request entity.Request) error {
	if !build.Status.IsTerminal() || request.TerminalBuildID != build.ID {
		return fmt.Errorf("final issue reconciliation requires the request's terminal build")
	}
	return nil
}

func (c *Controller) Name() string                { return "issuetrack" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*Controller)(nil)
