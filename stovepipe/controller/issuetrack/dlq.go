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

package issuetrack

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/id"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Avoid spinning the primary consumer while recovery retries storage or history writes.
const _recoveryDelayMs int64 = 30000

// DLQController abandons incomplete issue tracking while retaining accepted issue changes.
type DLQController struct {
	controller   *Controller
	materializer requestlog.Materializer
}

func NewDLQController(logger *zap.SugaredLogger, scope tally.Scope, stores storage.Factory, materializer requestlog.Materializer, topicKey consumer.TopicKey, consumerGroup string) *DLQController {
	return &DLQController{
		controller:   NewController(logger, scope, stores, materializer, consumer.TopicRegistry{}, topicKey, consumerGroup),
		materializer: materializer,
	}
}

// Process stops a failed pass without traversing its remaining issues.
// Recovery errors require an always-retryable DLQ consumer; unusable messages are logged and acknowledged.
func (c *DLQController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	notification := &stovepipemq.IssueTrack{}
	if err := stovepipemq.Unmarshal(msg.Payload, notification); err != nil {
		c.controller.logger.Errorw("discarding malformed issue tracking dlq message", "message_id", msg.ID, "error", err)
		return nil
	}
	queue := notification.GetQueueName()
	if err := entityqueue.ValidatePayloadQueue(msg, queue); err != nil {
		c.controller.logger.Errorw("discarding issue tracking dlq message with invalid queue identity", "message_id", msg.ID, "error", err)
		return nil
	}
	if msg.ID == "" || queue == "" || notification.GetBuildId() == "" || msg.PartitionKey != queue {
		c.controller.logger.Errorw("discarding issue tracking dlq message with invalid identity", "message_id", msg.ID, "queue", queue)
		return nil
	}

	store, err := c.controller.stores.For(storage.Config{QueueName: queue})
	if err != nil {
		c.controller.logger.Errorw("discarding issue tracking dlq message for unresolvable queue", "queue", queue, "message_id", msg.ID, "error", err)
		return nil
	}
	build, err := loader.ByID(ctx, notification.GetBuildId(), store.GetBuildStore().Get, "build")
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.controller.logger.Errorw("discarding issue tracking dlq message for missing build", "message_id", msg.ID, "error", err)
			return nil
		}
		return err
	}
	request, err := loader.ByID(ctx, build.RequestID, store.GetRequestStore().Get, "request")
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.controller.logger.Errorw("discarding issue tracking dlq message for missing request", "message_id", msg.ID, "error", err)
			return nil
		}
		return err
	}
	if request.Queue != queue {
		c.controller.logger.Errorw("discarding issue tracking dlq message for request in another queue", "message_id", msg.ID, "request_id", request.ID)
		return nil
	}
	if err := id.Validate(request.ID); err != nil {
		c.controller.logger.Errorw("discarding issue tracking dlq message with invalid request id", "message_id", msg.ID, "error", err)
		return nil
	}

	progress, err := c.abandonPass(ctx, store, request, notification, msg.ID)
	if err != nil {
		return err
	}
	if progress.Phase != entity.IssueTrackingPhaseAbandoned {
		return nil
	}

	originalFailure, hasFailure := delivery.Failure()
	fields := []any{"queue", queue, "message_id", msg.ID, "build_id", progress.BuildID, "request_id", progress.RequestID, "notification_id", progress.NotificationID}
	if hasFailure {
		fields = append(fields, "failure", originalFailure.Message, "failure_subjects", originalFailure.Subjects, "failure_detail", originalFailure.Detail)
	}
	c.controller.logger.Errorw("issue tracking abandoned; issue state may be incomplete", fields...)
	metrics.NamedCounter(c.controller.metricsScope, "dlq", "tracking_abandoned", 1, metrics.TagsFromContext(ctx)...)
	if progress.NotificationID != msg.ID {
		delivery.Hold(0)
	}
	return nil
}

func (c *DLQController) abandonPass(ctx context.Context, store storage.Storage, request entity.Request, notification *stovepipemq.IssueTrack, notificationID string) (entity.IssueTrackingState, error) {
	tracking := store.GetIssueTrackingStore()
	progress, err := tracking.Get(ctx)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return entity.IssueTrackingState{}, fmt.Errorf("load issue tracking recovery: %w", err)
	}
	if err == nil {
		if err := progress.Validate(); err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("invalid issue tracking recovery: %w", err)
		}
		if err := persistIssuesReconciledLog(ctx, c.materializer, store, request.Queue, progress); err != nil {
			return entity.IssueTrackingState{}, err
		}
		if progress.NotificationID == notificationID {
			switch progress.Phase {
			case entity.IssueTrackingPhaseRefreshed, entity.IssueTrackingPhaseFinalized:
				return progress, nil
			case entity.IssueTrackingPhaseAbandoned:
				return progress, c.recordAbandonment(ctx, store, request.Queue, progress)
			}
		} else {
			order, err := entity.CompareRequestID(request.ID, progress.RequestID)
			if err != nil {
				return entity.IssueTrackingState{}, err
			}
			if order < 0 || (order == 0 && progress.Finalize) {
				return entity.IssueTrackingState{}, nil
			}
			switch progress.Phase {
			case entity.IssueTrackingPhaseRefreshing, entity.IssueTrackingPhaseFinalizing, entity.IssueTrackingPhaseAbandoning:
				if order == 0 {
					return entity.IssueTrackingState{}, nil
				}
			default:
				progress = entity.IssueTrackingState{Version: progress.Version}
			}
		}
	}

	if progress.NotificationID == "" {
		if request.TerminalBuildID != "" && request.TerminalBuildID != notification.GetBuildId() {
			return entity.IssueTrackingState{}, nil
		}
		progress.RequestID, progress.BuildID, progress.NotificationID = request.ID, notification.GetBuildId(), notificationID
		progress.Finalize = notification.GetFinalize()
	}
	progress.Phase = entity.IssueTrackingPhaseAbandoning
	if progress.Version == 0 {
		progress.Version = 1
		if err := tracking.Create(ctx, progress); err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("start issue tracking recovery: %w", err)
		}
	} else {
		progress, err = saveProgress(ctx, tracking, progress)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
	}

	return c.finishAbandonment(ctx, store, request.Queue, progress)
}

func (c *DLQController) finishAbandonment(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState) (entity.IssueTrackingState, error) {
	if progress.CursorTaskID != "" {
		issues := store.GetIssueStateStore()
		current, err := issues.Get(ctx, progress.CursorTaskID, progress.CursorUnitID)
		missing := errors.Is(err, storage.ErrNotFound)
		if err != nil && !missing {
			return entity.IssueTrackingState{}, fmt.Errorf("read recovery cursor: %w", err)
		}
		if current.Version == math.MaxInt32 {
			return entity.IssueTrackingState{}, fmt.Errorf("issue state version exhausted")
		}

		// As in normal application, the post-read progress CAS must precede the issue CAS.
		progress, err = saveProgress(ctx, store.GetIssueTrackingStore(), progress)
		if err != nil {
			return entity.IssueTrackingState{}, err
		}
		if missing {
			current = entity.IssueState{
				TaskID: progress.CursorTaskID, UnitID: progress.CursorUnitID,
				Status: entity.IssueStatusOpen, FirstSeenBuildID: progress.BuildID,
				LastProcessedBuildID: progress.BuildID, LastChangedBuildID: progress.BuildID,
				ChangedAtMs: c.controller.nowMs(), Version: 1,
			}
			err = issues.Create(ctx, current)
		} else {
			newVersion := current.Version + 1
			err = issues.Update(ctx, current, current.Version, newVersion)
			if err == nil {
				current.Version = newVersion
			}
		}
		if errors.Is(err, storage.ErrAlreadyExists) {
			return entity.IssueTrackingState{}, fmt.Errorf("recovery cursor was concurrently created: %w", storage.ErrVersionMismatch)
		}
		if err != nil {
			return entity.IssueTrackingState{}, fmt.Errorf("fence recovery cursor: %w", err)
		}
		if err := c.controller.recordHistory(ctx, store, current); err != nil {
			return entity.IssueTrackingState{}, err
		}
	}

	if err := c.recordAbandonment(ctx, store, queue, progress); err != nil {
		return entity.IssueTrackingState{}, err
	}
	progress.Phase = entity.IssueTrackingPhaseAbandoned
	return saveProgress(ctx, store.GetIssueTrackingStore(), progress)
}

func (c *DLQController) recordAbandonment(ctx context.Context, store storage.Storage, queue string, progress entity.IssueTrackingState) error {
	request := entity.Request{ID: progress.RequestID, Queue: queue}
	// Notification IDs can exceed the request log's 255-character key.
	identity := sha256.Sum256([]byte(progress.NotificationID))
	log := requestlog.NewRequestEventLog(request, entity.RequestEventIssueTrackingAbandoned, fmt.Sprintf("%x", identity),
		map[string]string{requestlog.MetadataKeyBuildID: progress.BuildID})
	if err := c.materializer.PersistLog(ctx, store, log); err != nil {
		return fmt.Errorf("record incomplete issue tracking: %w", err)
	}
	return nil
}

func (c *DLQController) Name() string                { return string(c.controller.topicKey) }
func (c *DLQController) TopicKey() consumer.TopicKey { return c.controller.topicKey }
func (c *DLQController) ConsumerGroup() string       { return c.controller.consumerGroup }

var _ consumer.Controller = (*DLQController)(nil)
