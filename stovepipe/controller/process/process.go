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
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
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
	store         storage.Storage
	queueConfigs  queueconfig.Store
	sourceControl sourcecontrol.Factory
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
	store storage.Storage,
	queueConfigs queueconfig.Store,
	sourceControl sourcecontrol.Factory,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_controller"),
		metricsScope:  scope.SubScope("process_controller"),
		store:         store,
		queueConfigs:  queueConfigs,
		sourceControl: sourceControl,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the request referenced by the delivery, coalesces older heads,
// and admits the latest when a slot is open. Returns nil to ack (success) or an error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	op := metrics.Begin(c.metricsScope, _opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	pr := &stovepipemq.ProcessRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, pr); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize process request: %w", err)
	}

	request, err := c.loadRequest(ctx, pr.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	switch request.State {
	case entity.RequestStateProcessing:
		// TODO: re-publish to build once the build stage lands (RFC process algorithm, step 3).
		return nil
	case entity.RequestStateSuperseded:
		return nil
	case entity.RequestStateAccepted:
		return c.processAccepted(ctx, request)
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
// the latest head when a build slot is available.
func (c *Controller) processAccepted(ctx context.Context, request entity.Request) error {
	queueRow, err := c.loadQueue(ctx, request.Queue)
	if err != nil {
		if !errs.IsRetryable(err) {
			metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
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

	superseded, err := c.coalesce(ctx, request, queueRow.LatestRequestID)
	if err != nil || superseded {
		return err
	}

	cfg, err := c.queueConfigs.Get(ctx, request.Queue)
	if err != nil {
		// TODO(queueconfig): decide retryability when a real config store lands — is a
		// missing queue "drop" (non-retryable) or "retry until configured"?
		return fmt.Errorf("ProcessController failed to load queue config for %s: %w", request.Queue, err)
	}

	return c.admitLatestHead(ctx, request, queueRow, cfg.MaxConcurrent)
}

// coalesce supersedes request when a newer head exists (RFC process step 5), returning
// true so the caller acks. It returns false when request is still the latest head and
// should proceed to the gate. Superseding consumes no build slot.
func (c *Controller) coalesce(ctx context.Context, request entity.Request, latestRequestID string) (bool, error) {
	cmp, err := entity.CompareRequestID(request.Queue, request.ID, latestRequestID)
	if err != nil {
		return false, fmt.Errorf("ProcessController failed to compare request ids for queue %s: %w", request.Queue, err)
	}
	if cmp >= 0 {
		return false, nil
	}
	if err := c.supersedeRequest(ctx, request); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return false, err
	}
	metrics.NamedCounter(c.metricsScope, _opName, "superseded", 1)
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
// defers (acks) rather than failing.
func (c *Controller) admitLatestHead(ctx context.Context, request entity.Request, queueRow entity.Queue, maxConcurrent int32) error {
	for {
		if queueRow.InFlightCount >= maxConcurrent {
			// TODO: re-enqueue the request via PublishAfter on the process topic with GateWaitDelayMs.
			c.logger.Infow("latest head awaiting build slot",
				"request_id", request.ID,
				"queue", request.Queue,
				"uri", request.URI,
				"in_flight_count", queueRow.InFlightCount,
			)
			return nil
		}

		strategy, baseURI, err := c.deriveBuildStrategy(ctx, queueRow, request)
		if err != nil {
			return err
		}

		err = c.claimBuildSlot(ctx, &queueRow)
		if err == nil {
			request.BuildStrategy = strategy
			request.BaseURI = baseURI
			break
		}
		if !errors.Is(err, storage.ErrVersionMismatch) {
			return err
		}
		// claimBuildSlot reloaded queueRow. Re-coalesce: supersede if a newer head arrived,
		// otherwise loop to re-check the gate.
		superseded, err := c.coalesce(ctx, request, queueRow.LatestRequestID)
		if err != nil || superseded {
			return err
		}
	}

	transitioned, err := c.markProcessing(ctx, &request)
	if err != nil {
		// Slot claimed but never admitted: release best-effort so the slot isn't leaked
		// (a redelivery would find the gate closed by its own claim and nothing decrements it).
		c.releaseBuildSlot(ctx, request.Queue)
		return err
	}
	if !transitioned {
		// Lost the admit race: another delivery advanced this request. Release and skip.
		c.releaseBuildSlot(ctx, request.Queue)
		return nil
	}

	// TODO(build-publish): publish BuildRequest to the build stage here.

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
// The caller persists the returned values only after successfully claiming a build slot.
func (c *Controller) deriveBuildStrategy(ctx context.Context, queueRow entity.Queue, request entity.Request) (strategy entity.BuildStrategy, baseURI string, err error) {
	if queueRow.LastGreenURI == "" {
		return entity.BuildStrategyFull, "", nil
	}

	sc, err := c.sourceControl.For(sourcecontrol.Config{QueueName: request.Queue})
	if err != nil {
		return entity.BuildStrategyUnknown, "", fmt.Errorf("ProcessController failed to resolve source control for queue %s: %w", request.Queue, err)
	}

	isAncestor, err := sc.IsAncestor(ctx, queueRow.LastGreenURI, request.URI)
	if err != nil {
		if sourcecontrol.IsNotFound(err) {
			c.logger.Warnw("last-green URI is not in request history; using full build",
				"queue", request.Queue,
				"last_green_uri", queueRow.LastGreenURI,
				"request_uri", request.URI,
			)
			return entity.BuildStrategyFull, "", nil
		}
		return entity.BuildStrategyUnknown, "", fmt.Errorf("ProcessController failed to check ancestry for queue %s: %w", request.Queue, err)
	}

	if isAncestor {
		return entity.BuildStrategyIncrementalSinceGreen, queueRow.LastGreenURI, nil
	}
	return entity.BuildStrategyFull, "", nil
}

// claimBuildSlot CAS-increments queue.in_flight_count by one. On version mismatch it
// reloads queueRow and returns ErrVersionMismatch so the caller can retry.
func (c *Controller) claimBuildSlot(ctx context.Context, queueRow *entity.Queue) error {
	queueStore := c.store.GetQueueStore()

	updated := *queueRow
	updated.InFlightCount = queueRow.InFlightCount + 1
	newVersion := queueRow.Version + 1
	if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			got, getErr := queueStore.Get(ctx, queueRow.Name)
			if getErr != nil {
				return fmt.Errorf("ProcessController failed to reload queue %s after version mismatch: %w", queueRow.Name, getErr)
			}
			*queueRow = got
			return storage.ErrVersionMismatch
		}
		return fmt.Errorf("ProcessController failed to claim build slot for queue %s: %w", queueRow.Name, err)
	}
	updated.Version = newVersion
	*queueRow = updated
	return nil
}

// markProcessing CAS-marks request accepted→processing, persisting BuildStrategy and BaseURI
// already set by the admit workflow. Retries on version conflicts. transitioned is true only
// when this call performed the CAS; false means a concurrent writer already advanced the
// request past accepted (a lost admit race), so the caller must release its claimed slot.
func (c *Controller) markProcessing(ctx context.Context, request *entity.Request) (transitioned bool, err error) {
	reqStore := c.store.GetRequestStore()

	for {
		if request.State != entity.RequestStateAccepted {
			return false, nil
		}

		updated := *request
		updated.State = entity.RequestStateProcessing
		newVersion := request.Version + 1
		if err := reqStore.Update(ctx, updated, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				got, getErr := reqStore.Get(ctx, request.ID)
				if getErr != nil {
					return false, fmt.Errorf("ProcessController failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				*request = got
				continue
			}
			return false, fmt.Errorf("ProcessController failed to mark request %s processing: %w", request.ID, err)
		}
		updated.Version = newVersion
		*request = updated
		return true, nil
	}
}

// releaseBuildSlot CAS-decrements queue.in_flight_count to compensate a slot claimed but never
// admitted. It decrements relatively (preserving a concurrent record decrement) and retries on
// version conflicts. Best-effort: it only logs on a hard failure, since the caller is unwinding.
func (c *Controller) releaseBuildSlot(ctx context.Context, queueName string) {
	queueStore := c.store.GetQueueStore()

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
		metrics.NamedCounter(c.metricsScope, _opName, "slot_released", 1)
		return
	}
}

// supersedeRequest transitions a request from accepted to superseded, retrying on version conflicts.
func (c *Controller) supersedeRequest(ctx context.Context, request entity.Request) error {
	reqStore := c.store.GetRequestStore()

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
					return fmt.Errorf("ProcessController failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				request = got
				continue
			}
			return fmt.Errorf("ProcessController failed to supersede request %s: %w", request.ID, err)
		}
		return nil
	}
}

// loadRequest returns the request for id. A not-yet-visible row is retryable.
func (c *Controller) loadRequest(ctx context.Context, id string) (entity.Request, error) {
	got, err := c.store.GetRequestStore().Get(ctx, id)
	if err == nil {
		return got, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return entity.Request{}, errs.NewRetryableError(fmt.Errorf("request %s not found yet: %w", id, err))
	}
	return entity.Request{}, fmt.Errorf("ProcessController failed to load request %s: %w", id, err)
}

// loadQueue returns the queue row for name. A not-yet-visible row is retryable.
func (c *Controller) loadQueue(ctx context.Context, name string) (entity.Queue, error) {
	got, err := c.store.GetQueueStore().Get(ctx, name)
	if err == nil {
		return got, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return entity.Queue{}, errs.NewRetryableError(fmt.Errorf("queue %s not found yet: %w", name, err))
	}
	return entity.Queue{}, fmt.Errorf("ProcessController failed to load queue %s: %w", name, err)
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
