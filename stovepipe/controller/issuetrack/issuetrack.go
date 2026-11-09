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
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/fixrequest"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	"github.com/uber/submitqueue/stovepipe/core/issuelog"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

const (
	// Each delivery processes at most this many issue keys, including a revisited cursor and closed records.
	_issueBatchSize       = 100
	_analysisWaitMs int64 = 30000
)

type issueKey struct{ task, unit string }

// Controller retains issue transitions and their queue-scoped progress.
type Controller struct {
	logger              *zap.SugaredLogger
	metricsScope        tally.Scope
	stores              storage.Factory
	materializer        issuelog.Materializer
	registry            consumer.TopicRegistry
	requestMaterializer requestlog.Materializer
	topicKey            consumer.TopicKey
	consumerGroup       string
	nowMs               func() int64
}

func NewController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, materializer requestlog.Materializer, registry consumer.TopicRegistry, topicKey consumer.TopicKey, consumerGroup string) *Controller {
	return &Controller{
		logger:              logger.Named("issuetrack_controller"),
		metricsScope:        scope.SubScope("issuetrack_controller"),
		stores:              stores,
		materializer:        issuelog.NewMaterializer(scope),
		registry:            registry,
		requestMaterializer: materializer,
		topicKey:            topicKey,
		consumerGroup:       consumerGroup,
		nowMs:               func() int64 { return time.Now().UnixMilli() },
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
	if msg.ID == "" {
		return fmt.Errorf("issue tracking message ID must not be empty")
	}
	if notification.GetRequestId() == "" {
		return fmt.Errorf("issue tracking requires a request ID")
	}
	if msg.PartitionKey != queue {
		return fmt.Errorf("issue tracking must be partitioned by queue %q", queue)
	}
	store, err := c.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		return fmt.Errorf("resolve issue tracking storage: %w", err)
	}
	return c.trackRequest(ctx, delivery, store, notification, msg.ID)
}

func (c *Controller) trackRequest(ctx context.Context, delivery consumer.Delivery, store storage.Storage, notification *stovepipemq.IssueTrack, notificationID string) error {
	queue := notification.GetQueueName()
	tracking := store.GetIssueTrackingStore()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		request, err := loader.ByID(ctx, notification.GetRequestId(), store.GetRequestStore().Get, "tracked request")
		if err != nil {
			return err
		}
		if request.Queue != queue {
			return fmt.Errorf("tracked request belongs to another queue")
		}
		if err := id.Validate(request.ID); err != nil {
			return err
		}
		progress, err := tracking.Get(ctx)
		if errors.Is(err, storage.ErrNotFound) {
			progress = entity.IssueTrackingState{}
		} else if err != nil {
			return fmt.Errorf("load issue tracking progress: %w", err)
		}
		// Finish the previous validation's completion history before replacing its checkpoint.
		if err := persistIssuesReconciledLog(ctx, c.requestMaterializer, store, queue, progress); err != nil {
			return err
		}
		if progress.Version != 0 {
			order, err := entity.CompareRequestID(request.ID, progress.RequestID)
			if err != nil {
				return err
			}
			if order < 0 {
				return nil
			}
			switch progress.Phase {
			case entity.IssueTrackingPhaseAbandoning:
				delivery.Hold(_recoveryDelayMs)
				return nil
			case entity.IssueTrackingPhaseFinalized, entity.IssueTrackingPhaseAbandoned:
				if order == 0 {
					return nil
				}
			default:
				if order > 0 {
					delivery.Hold(_analysisWaitMs)
					return nil
				}
			}
		}

		buildID := request.BuildID
		if request.TerminalBuildID != "" {
			buildID = request.TerminalBuildID
		}
		if buildID == "" {
			if request.State == entity.RequestStateSuperseded {
				return nil
			}
			if !request.State.IsTerminal() {
				delivery.Hold(_analysisWaitMs)
				return nil
			}
		}
		if request.State.IsTerminal() && request.TerminalBuildID == "" {
			return fmt.Errorf("cannot track request %s: request ended without a build outcome", request.ID)
		}
		event, err := terminalAnalysisEvent(ctx, store, request, buildID)
		if err != nil {
			return err
		}
		if event == entity.RequestEventBuildAnalysisAbandoned {
			return fmt.Errorf("cannot track request %s: build analysis was abandoned", request.ID)
		}
		finalize := event == entity.RequestEventBuildAnalysisCompleted
		// Completion may have been recorded after the request read above.
		if finalize && request.TerminalBuildID != buildID {
			request, err = loader.ByID(ctx, request.ID, store.GetRequestStore().Get, "completed request")
			if err != nil {
				return err
			}
		}
		build, err := loader.ByID(ctx, buildID, store.GetBuildStore().Get, "tracked build")
		if err != nil {
			return err
		}
		if build.RequestID != request.ID {
			return fmt.Errorf("associated build belongs to another tracking request")
		}

		switch progress.Phase {
		case entity.IssueTrackingPhaseRefreshing, entity.IssueTrackingPhaseFinalizing:
			// Resume the saved pass before starting a newer refresh or finalization.
		default:
			if progress.Version == math.MaxInt32 {
				return fmt.Errorf("issue tracking version exhausted")
			}
			if finalize {
				if err := requireTerminalBuild(build, request); err != nil {
					return err
				}
			}
			next := entity.IssueTrackingState{
				RequestID:      request.ID,
				BuildID:        build.ID,
				NotificationID: notificationID,
				Phase:          entity.IssueTrackingPhaseRefreshing,
				Finalize:       finalize,
				Version:        progress.Version + 1,
			}
			if progress.RequestID != request.ID {
				log := requestlog.NewRequestEventLog(request, entity.RequestEventIssueTrackingStarted, "", nil)
				if err := c.requestMaterializer.PersistLog(ctx, store, log); err != nil {
					return fmt.Errorf("record start of request issue results: %w", err)
				}
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
			if progress.RequestID != request.ID {
				c.logger.Infow("tracking validation", "queue", queue, "request_id", request.ID, "build_id", build.ID, "notification_id", notificationID)
			}
			progress = next
			if finalize {
				c.logger.Infow("starting issue finalization", "queue", queue, "request_id", request.ID, "build_id", build.ID)
			}
		}
		checkpoint, err := c.reconcileBatch(ctx, store, queue, progress)
		if err != nil {
			return err
		}
		if err := persistIssuesReconciledLog(ctx, c.requestMaterializer, store, queue, checkpoint); err != nil {
			return err
		}
		switch checkpoint.Phase {
		case entity.IssueTrackingPhaseRefreshing, entity.IssueTrackingPhaseFinalizing:
			delivery.Hold(0)
		case entity.IssueTrackingPhaseRefreshed:
			delivery.Hold(_analysisWaitMs)
		}
		return nil
	}
}

func (c *Controller) reconcileBatch(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState) (checkpoint entity.IssueTrackingState, retErr error) {
	op := metrics.Begin(c.metricsScope, "batch", metrics.LongLatencyBuckets, metrics.TagsFromContext(ctx)...)
	defer func() { op.Complete(retErr) }()

	if err := progress.Validate(); err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("invalid issue tracking progress: %w", err)
	}

	limit := _issueBatchSize

	build, request, err := loadBuildAndRequest(ctx, store, queue, progress.BuildID)
	if err != nil {
		return entity.IssueTrackingState{}, err
	}
	switch progress.Phase {
	case entity.IssueTrackingPhaseRefreshing, entity.IssueTrackingPhaseFinalizing:
	default:
		return entity.IssueTrackingState{}, fmt.Errorf("issue batch requires an active pass")
	}
	if request.ID != progress.RequestID {
		return entity.IssueTrackingState{}, fmt.Errorf("issue batch does not match its source request")
	}
	finalizing := progress.Phase == entity.IssueTrackingPhaseFinalizing
	if progress.Finalize {
		if err := requireTerminalBuild(build, request); err != nil {
			return entity.IssueTrackingState{}, err
		}
	}

	if finalizing && build.Status == entity.BuildStatusCancelled {
		return entity.IssueTrackingState{}, fmt.Errorf("canceled builds cannot close missing issues")
	}

	if progress.CursorTaskID != "" {
		next, err := c.processIssue(ctx, store, queue, progress, issueKey{progress.CursorTaskID, progress.CursorUnitID}, false)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		progress = next
		limit--
	}

	var keys []issueKey
	if finalizing {
		states, err := store.GetIssueStateStore().List(ctx, storage.IssueStateRange{
			After: storage.IssueStateCursor{TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID},
			Limit: limit,
		})
		if err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("list issue states: %w", err)
		}
		for _, issue := range states {
			keys = append(keys, issueKey{issue.TaskID, issue.UnitID})
		}
	} else {
		issues, err := store.GetBuildIssueStore().List(ctx, build.ID, storage.BuildIssueRange{
			After: storage.BuildIssueCursor{TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID},
			Limit: limit,
		})
		if err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("list build issues: %w", err)
		}
		for _, issue := range issues {
			keys = append(keys, issueKey{issue.TaskID, issue.UnitID})
		}
	}

	cursorNeedsCheckpoint := false
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return entity.IssueTrackingState{}, err
		}
		previousVersion := progress.Version
		progress, err = c.processIssue(ctx, store, queue, progress, key, !finalizing)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		cursorNeedsCheckpoint = progress.Version == previousVersion
	}

	if len(keys) < limit {
		if !finalizing && progress.CursorTaskID != "" {
			// The checkpoint distinguishes refreshes of one long-lived tracking job.
			repairID := fmt.Sprintf("%s/%d", progress.NotificationID, progress.Version)
			if err := fixrequest.Publish(ctx, c.registry, queue, progress.BuildID, repairID); err != nil {
				return entity.IssueTrackingState{}, fmt.Errorf("publish build repair request: %w", err)
			}
		}
		next := progress
		switch {
		case finalizing:
			next.Phase = entity.IssueTrackingPhaseFinalized
		case !progress.Finalize:
			next.Phase = entity.IssueTrackingPhaseRefreshed
		case progress.HasTaskWideIssues || build.Status == entity.BuildStatusCancelled:
			next.Phase = entity.IssueTrackingPhaseFinalized
		default:
			next.Phase = entity.IssueTrackingPhaseFinalizing
			next.CursorTaskID, next.CursorUnitID = "", ""
		}
		checkpoint, err := saveProgress(ctx, store.GetIssueTrackingStore(), next)
		if err == nil && checkpoint.Phase == entity.IssueTrackingPhaseFinalized {
			c.logger.Infow("completed issue finalization", "queue", queue, "request_id", checkpoint.RequestID, "build_id", checkpoint.BuildID,
				"has_task_wide_issues", checkpoint.HasTaskWideIssues, "status", build.Status)
		}
		return checkpoint, err
	}
	if cursorNeedsCheckpoint {
		return saveProgress(ctx, store.GetIssueTrackingStore(), progress)
	}

	return progress, nil
}

func (c *Controller) processIssue(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState, key issueKey, reported bool) (entity.IssueTrackingState, error) {
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
		return entity.IssueTrackingState{}, fmt.Errorf("only finalizing passes can close missing issues")
	}
	if reported && key.unit == "" && progress.Phase == entity.IssueTrackingPhaseRefreshing {
		progress.HasTaskWideIssues = true
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
			if err := c.recordIssueTransition(ctx, store, queue, current); err != nil {
				return entity.IssueTrackingState{}, err
			}
		}
		return next, persistReportedIssue(ctx, c.materializer, store, progress, current)
	}

	// Read the issue before this CAS, including when resuming a cursor. A
	// takeover must finish that key before advancing, invalidating this row guard.
	next, err = saveProgress(ctx, store.GetIssueTrackingStore(), next)
	if err != nil {
		return entity.IssueTrackingState{}, err
	}

	if !missing {
		if current.LastProcessedBuildID == progress.BuildID {
			if err := c.recordIssueTransition(ctx, store, queue, current); err != nil {
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
		if err := c.recordIssueTransition(ctx, store, queue, target); err != nil {
			return entity.IssueTrackingState{}, err
		}
	}
	return next, persistReportedIssue(ctx, c.materializer, store, progress, target)
}

func (c *Controller) recordIssueTransition(ctx context.Context, store storage.Storage, queue string, issue entity.IssueState) error {
	if err := persistIssueHistory(ctx, c.materializer, store, issue); err != nil {
		return err
	}
	if issue.Status == entity.IssueStatusOpen {
		log := entity.IssueLog{TaskID: issue.TaskID, UnitID: issue.UnitID, BuildID: issue.LastChangedBuildID, Event: entity.IssueEventOpened, TimestampMs: issue.ChangedAtMs}
		event := hookevent.NewIssueOpened(queue, log)
		if err := hook.Publish(ctx, c.registry, queue, event, event.GetId()); err != nil {
			return fmt.Errorf("publish issue opening: %w", err)
		}
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

func requireTerminalBuild(build entity.Build, request entity.Request) error {
	if !build.Status.IsTerminal() {
		return fmt.Errorf("final issue tracking requires a terminal build")
	}
	if request.TerminalBuildID != build.ID {
		return fmt.Errorf("final issue tracking requires the request's terminal build")
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

func (c *Controller) Name() string                { return "issuetrack" }
func (c *Controller) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *Controller) ConsumerGroup() string       { return c.consumerGroup }

var _ consumer.Controller = (*Controller)(nil)
