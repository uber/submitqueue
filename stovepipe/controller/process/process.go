// Copyright (c) 2025 Uber Technologies, Inc.
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

// Package process holds the process-stage queue controller. It consumes request
// ids from ingest, reloads the Request from storage, coalesces older heads, and
// admits the latest head when a build slot is open. Build queue publish lands in
// a follow-up PR.
package process

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	platformhook "github.com/uber/submitqueue/platform/hook"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	"github.com/uber/submitqueue/stovepipe/core/hookevent"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes ProcessRequest messages from the process stage, reloads the
// referenced Request from storage, coalesces older heads, and admits the latest when
// a slot is open. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	queueConfigs  queueconfig.Store
	sourceControl sourcecontrol.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "process"

// NewController creates a new process controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	queueConfigs queueconfig.Store,
	sourceControl sourcecontrol.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_controller"),
		metricsScope:  scope.SubScope("process_controller"),
		stores:        stores,
		queueConfigs:  queueConfigs,
		sourceControl: sourceControl,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the request referenced by the delivery, coalesces older heads,
// and admits the latest when a slot is open. Returns nil to ack (success) or an error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	pr := &stovepipemq.ProcessRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, pr); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize process request: %w", err)
	}
	store, err := c.stores.For(storage.Config{QueueName: pr.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_resolve_errors", 1, metrics.TagsFromContext(ctx)...)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", pr.GetQueueName(), err)
	}

	request, err := c.loadRequest(ctx, store, pr.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return err
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if pr.GetQueueName() != "" && pr.GetQueueName() != request.Queue {
		metrics.NamedCounter(c.metricsScope, _opName, "queue_mismatch", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", pr.GetQueueName(), request.Queue, request.ID)
	}

	switch request.State {
	case entity.RequestStateProcessing:
		// Announce here as well as at admit: this is the only path a redelivery
		// takes once the transition is durable, so an admit that failed after
		// persisting would otherwise lose the start event for good. The event id
		// is derived from the transition, so a repeat carries the id the first
		// attempt would have and a consumer deduplicates on it.
		if err := c.publishHookEvent(ctx, request, hookevent.NewValidationRepositoryStarted(request)); err != nil {
			return err
		}
		if err := c.publishBuild(ctx, request.ID, request.Queue); err != nil {
			metrics.NamedCounter(c.metricsScope, _opName, "publish_errors", 1, metrics.TagsFromContext(ctx)...)
			return fmt.Errorf("failed to publish request %s to build: %w", request.ID, err)
		}
		return nil
	case entity.RequestStateSuperseded, entity.RequestStateSucceeded, entity.RequestStateFailed, entity.RequestStateCancelled:
		// Terminal: a newer head preempted this request, or its build already finished.
		// A stale redelivery has nothing left to do.
		return nil
	case entity.RequestStateAccepted:
		return c.processAccepted(ctx, store, delivery, request)
	default:
		c.logger.Warnw("ignored request in unexpected state",
			"request_id", request.ID,
			"queue", request.Queue,
			"state", string(request.State),
		)
		return nil
	}
}

// processAccepted coalesces older heads against queue.latest_request_id, then admits
// the latest head when a build slot is available. The delivery is threaded down so a
// closed gate can hold it.
func (c *Controller) processAccepted(ctx context.Context, store storage.Storage, delivery consumer.Delivery, request entity.Request) error {
	queueRow, err := c.loadQueue(ctx, store, request.Queue)
	if err != nil {
		if !errs.IsRetryable(err) {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		}
		return err
	}

	if queueRow.LatestRequestID == "" {
		c.logger.Infow("latest head awaiting queue.latest_request_id stamp from ingest",
			"request_id", request.ID,
			"queue", request.Queue,
			"uri", request.URI,
		)
		return nil
	}

	superseded, err := c.coalesce(ctx, store, request, queueRow.LatestRequestID)
	if err != nil || superseded {
		return err
	}

	cfg, err := c.queueConfigs.Get(ctx, request.Queue)
	if err != nil {
		// TODO(queueconfig): decide retryability when a real config store lands — is a
		// missing queue "drop" (non-retryable) or "retry until configured"?
		return fmt.Errorf("failed to load queue config for %s: %w", request.Queue, err)
	}

	return c.admitLatestHead(ctx, store, delivery, request, queueRow, cfg)
}

// coalesce supersedes request when a newer head exists (RFC process step 5), returning
// true so the caller acks. It returns false when request is still the latest head and
// should proceed to the gate. Superseding consumes no build slot.
func (c *Controller) coalesce(ctx context.Context, store storage.Storage, request entity.Request, latestRequestID string) (bool, error) {
	cmp, err := entity.CompareRequestID(request.Queue, request.ID, latestRequestID)
	if err != nil {
		return false, fmt.Errorf("failed to compare request ids for queue %s: %w", request.Queue, err)
	}
	if cmp >= 0 {
		return false, nil
	}
	if err := c.supersedeRequest(ctx, store, request); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1, metrics.TagsFromContext(ctx)...)
		return false, err
	}
	metrics.NamedCounter(c.metricsScope, _opName, "superseded", 1, metrics.TagsFromContext(ctx)...)
	c.logger.Infow("superseded request for newer head",
		"request_id", request.ID,
		"queue", request.Queue,
		"latest_request_id", latestRequestID,
	)
	return true, nil
}

// admitLatestHead runs the gate-then-admit workflow for the latest head: claim a build
// slot, mark the request processing, and publish it to build. Every queue-row reload
// re-runs coalesce-then-gate, so a slot is never spent on a now-stale head; a closed gate
// defers by holding the delivery (redeliver after the gate wait delay) rather than failing.
func (c *Controller) admitLatestHead(ctx context.Context, store storage.Storage, delivery consumer.Delivery, request entity.Request, queueRow entity.Queue, cfg entity.QueueConfig) error {
	var sc sourcecontrol.SourceControl
	var strategy entity.BuildStrategy
	var baseURI string
	var err error

	for {
		if queueRow.InFlightCount >= cfg.MaxConcurrent {
			return c.holdForBuildSlot(ctx, delivery, request, queueRow.InFlightCount, cfg.GateWaitDelayMs)
		}

		if queueRow.LastGreenURI != "" && sc == nil {
			sc, err = c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
			if err != nil {
				metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
					metrics.TagsFromContext(ctx, metrics.NewTag("stage", "resolve"))...,
				)
				return fmt.Errorf("failed to resolve source control for queue %s: %w", request.Queue, err)
			}
		}

		strategy, baseURI, err = c.deriveBuildStrategy(ctx, sc, queueRow, request)
		if err != nil {
			return err
		}

		err = c.claimBuildSlot(ctx, store, &queueRow)
		if err == nil {
			break
		}
		if !errors.Is(err, storage.ErrVersionMismatch) {
			return err
		}
		// claimBuildSlot reloaded queueRow. Re-coalesce: supersede if a newer head arrived,
		// otherwise loop to re-check the gate.
		superseded, err := c.coalesce(ctx, store, request, queueRow.LatestRequestID)
		if err != nil || superseded {
			return err
		}
	}

	transitioned, err := c.markProcessing(ctx, store, &request, strategy, baseURI)
	if err != nil {
		// Slot claimed but never admitted: release best-effort so the slot isn't leaked
		// (a redelivery would find the gate closed by its own claim and nothing decrements it).
		c.releaseBuildSlot(ctx, store, request.Queue)
		return err
	}
	if !transitioned {
		// Lost the admit race: another delivery advanced this request. Release and skip.
		c.releaseBuildSlot(ctx, store, request.Queue)
		return nil
	}

	if err := c.publishHookEvent(ctx, request, hookevent.NewValidationRepositoryStarted(request)); err != nil {
		return err
	}

	if err := c.publishBuild(ctx, request.ID, request.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "publish_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to publish request %s to build: %w", request.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, _opName, "admitted", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("strategy", string(request.BuildStrategy)))...,
	)
	c.logger.Infow("admitted request to build",
		"request_id", request.ID,
		"queue", request.Queue,
		"uri", request.URI,
		"build_strategy", string(request.BuildStrategy),
		"base_uri", request.BaseURI,
	)
	return nil
}

// deriveBuildStrategy chooses the validation scope and baseline from the queue's last-known-good commit.
// The caller resolves source control once and persists the returned values only after successfully claiming a build slot.
func (c *Controller) deriveBuildStrategy(ctx context.Context, sc sourcecontrol.SourceControl, queueRow entity.Queue, request entity.Request) (strategy entity.BuildStrategy, baseURI string, err error) {
	if queueRow.LastGreenURI == "" {
		return entity.BuildStrategyFull, "", nil
	}

	isAncestor, err := sc.IsAncestor(ctx, queueRow.LastGreenURI, request.URI)
	if err != nil {
		if sourcecontrol.IsNotFound(err) {
			metrics.NamedCounter(c.metricsScope, _opName, "strategy_fallbacks", 1,
				metrics.TagsFromContext(ctx, metrics.NewTag("reason", "unknown_ancestry"))...,
			)
			c.logger.Warnw("last-green URI is not in request history; using full build",
				"queue", request.Queue,
				"last_green_uri", queueRow.LastGreenURI,
				"request_uri", request.URI,
			)
			return entity.BuildStrategyFull, "", nil
		}
		metrics.NamedCounter(c.metricsScope, _opName, "source_control_errors", 1,
			metrics.TagsFromContext(ctx, metrics.NewTag("stage", "ancestry"))...,
		)
		return entity.BuildStrategyUnknown, "", fmt.Errorf("failed to check ancestry for queue %s: %w", request.Queue, err)
	}

	if isAncestor {
		return entity.BuildStrategyIncrementalSinceGreen, queueRow.LastGreenURI, nil
	}
	return entity.BuildStrategyFull, "", nil
}

// claimBuildSlot CAS-increments queue.in_flight_count by one. On version mismatch it
// reloads queueRow and returns ErrVersionMismatch so the caller can retry.
func (c *Controller) claimBuildSlot(ctx context.Context, store storage.Storage, queueRow *entity.Queue) error {
	queueStore := store.GetQueueStore()

	updated := *queueRow
	updated.InFlightCount = queueRow.InFlightCount + 1
	newVersion := queueRow.Version + 1
	if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			got, getErr := queueStore.Get(ctx, queueRow.Name)
			if getErr != nil {
				return fmt.Errorf("failed to reload queue %s after version mismatch: %w", queueRow.Name, getErr)
			}
			*queueRow = got
			return storage.ErrVersionMismatch
		}
		return fmt.Errorf("failed to claim build slot for queue %s: %w", queueRow.Name, err)
	}
	updated.Version = newVersion
	*queueRow = updated
	return nil
}

// markProcessing CAS-marks request accepted→processing and persists the strategy chosen after the
// build slot was claimed. It reapplies the values after a request reload so an accepted concurrent
// update cannot discard them. transitioned is true only when this call performed the CAS; false
// means a concurrent writer already advanced the request past accepted, so the caller must release
// its claimed slot.
func (c *Controller) markProcessing(ctx context.Context, store storage.Storage, request *entity.Request, strategy entity.BuildStrategy, baseURI string) (transitioned bool, err error) {
	reqStore := store.GetRequestStore()

	for {
		if request.State != entity.RequestStateAccepted {
			return false, nil
		}

		updated := *request
		updated.State = entity.RequestStateProcessing
		updated.BuildStrategy = strategy
		updated.BaseURI = baseURI
		newVersion := request.Version + 1
		if err := reqStore.Update(ctx, updated, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				got, getErr := reqStore.Get(ctx, request.ID)
				if getErr != nil {
					return false, fmt.Errorf("failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				*request = got
				continue
			}
			return false, fmt.Errorf("failed to mark request %s processing: %w", request.ID, err)
		}
		updated.Version = newVersion
		*request = updated
		return true, nil
	}
}

// releaseBuildSlot CAS-decrements queue.in_flight_count to compensate a slot claimed but never
// admitted. It decrements relatively (preserving a concurrent record decrement) and retries on
// version conflicts. Best-effort: it only logs on a hard failure, since the caller is unwinding.
func (c *Controller) releaseBuildSlot(ctx context.Context, store storage.Storage, queueName string) {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, queueName)
		if err != nil {
			c.logger.Errorw("failed to release claimed build slot",
				"queue", queueName,
				"error", err,
			)
			return
		}
		if queueRow.InFlightCount <= 0 {
			return
		}

		updated := queueRow
		updated.InFlightCount = queueRow.InFlightCount - 1
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			c.logger.Errorw("failed to release claimed build slot",
				"queue", queueName,
				"error", err,
			)
			return
		}
		metrics.NamedCounter(c.metricsScope, _opName, "slot_released", 1, metrics.TagsFromContext(ctx)...)
		return
	}
}

// supersedeRequest transitions a request from accepted to superseded, retrying on version conflicts.
func (c *Controller) supersedeRequest(ctx context.Context, store storage.Storage, request entity.Request) error {
	reqStore := store.GetRequestStore()

	for {
		if request.State != entity.RequestStateAccepted {
			return nil
		}

		updated := request
		updated.State = entity.RequestStateSuperseded
		newVersion := request.Version + 1
		if err := reqStore.Update(ctx, updated, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				got, getErr := reqStore.Get(ctx, request.ID)
				if getErr != nil {
					return fmt.Errorf("failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				request = got
				continue
			}
			return fmt.Errorf("failed to supersede request %s: %w", request.ID, err)
		}
		return nil
	}
}

// holdForBuildSlot holds the delivery so the same ProcessRequest redelivers after
// delayMs and the gate is re-checked, without burning MaxAttempts — the partition
// (keyed by queue name) waits with it. delayMs must be positive: a non-positive
// hold would redeliver immediately and hot-loop the gate check.
func (c *Controller) holdForBuildSlot(ctx context.Context, delivery consumer.Delivery, request entity.Request, inFlightCount int32, delayMs int64) error {
	if delayMs <= 0 {
		metrics.NamedCounter(c.metricsScope, _opName, "config_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("requires a positive gate wait delay for queue %s, got %dms", request.Queue, delayMs)
	}

	delivery.Hold(delayMs)
	c.logger.Infow("holding latest head awaiting build slot",
		"request_id", request.ID,
		"queue", request.Queue,
		"uri", request.URI,
		"in_flight_count", inFlightCount,
		"delay_ms", delayMs,
	)
	return nil
}

// loadRequest returns the request for id.
func (c *Controller) loadRequest(ctx context.Context, store storage.Storage, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, store.GetRequestStore().Get, "request")
}

// loadQueue returns the queue row for name.
func (c *Controller) loadQueue(ctx context.Context, store storage.Storage, name string) (entity.Queue, error) {
	return loader.ByID(ctx, name, store.GetQueueStore().Get, "queue")
}

// publishBuild publishes the admitted request ID to the build stage. The build
// controller reloads the Request from storage to read its immutable strategy
// and baseline.
//
// The request ID is the message ID with no cause: a request is admitted to
// build once, so a redelivery that re-admits it is meant to dedup away.
func (c *Controller) publishBuild(ctx context.Context, id, queue string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id, QueueName: queue})
	if err != nil {
		return fmt.Errorf("failed to serialize build request: %w", err)
	}

	if err := publish.Message(ctx, c.registry, stovepipemq.TopicKeyBuild, publish.IntentID(id), payload, id); err != nil {
		return fmt.Errorf("failed to publish build request: %w", err)
	}
	return nil
}

// publishHookEvent announces a lifecycle transition on the hook topic.
//
// Published only once the transition is durable: the payload names the request
// rather than snapshotting it, so a hook that reloads it must not find a request
// whose strategy and baseline are still unwritten.
//
// Partitioning by request id matches the process topic's own, carrying
// per-request ordering across the seam.
func (c *Controller) publishHookEvent(ctx context.Context, request entity.Request, event *basehook.HookEvent) error {
	if err := platformhook.Publish(ctx, c.registry, event, request.ID); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "hook_errors", 1, metrics.TagsFromContext(ctx)...)
		return fmt.Errorf("failed to announce %s for request %s: %w", event.GetType(), request.ID, err)
	}

	metrics.NamedCounter(c.metricsScope, _opName, "hook_events_published", 1,
		metrics.TagsFromContext(ctx, metrics.NewTag("event_type", event.GetType()))...,
	)
	c.logger.Debugw("announced validation event",
		"queue", request.Queue,
		"request_id", request.ID,
		"uri", request.URI,
		"event_type", event.GetType(),
		"event_id", event.GetId(),
	)
	return nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "process"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
