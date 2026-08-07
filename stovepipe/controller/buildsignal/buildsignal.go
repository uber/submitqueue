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

// Package buildsignal holds the buildsignal-stage queue controller. It consumes
// BuildSignal messages (a build id), polls the build-runner until the build
// reaches a terminal status, persists that status, and — once terminal —
// releases the queue's build slot, projects the outcome onto the request, and
// publishes the request id to record. See
// doc/rfc/stovepipe/steps/buildsignal.md.
package buildsignal

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/core/loader"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Poll delays for non-terminal statuses. Vars (not consts) so tests can
// shorten them; the server always uses the defaults.
//
// TODO: move these behind a queueconfig-style extension so operators can
// tune poll cadence per queue without a code change.
var (
	// PollDelayAcceptedMs is the delay before the next Status call while the
	// build is queued by the runner but has not started executing.
	PollDelayAcceptedMs int64 = 5000
	// PollDelayRunningMs is the delay before the next Status call while the
	// build is executing.
	PollDelayRunningMs int64 = 2000
)

// Controller consumes BuildSignal messages, polls the build-runner toward a
// terminal status, persists the result, and either holds the delivery until
// the next poll or releases the queue's build slot, projects the outcome onto
// the request, and publishes the request id to record. Implements
// consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        storage.Factory
	buildRunners  buildrunner.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// _opName is the metric operation name shared by every emit in this file.
const _opName = "buildsignal"

// NewController creates a new buildsignal controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	stores storage.Factory,
	buildRunners buildrunner.Factory,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("buildsignal_controller"),
		metricsScope:  scope.SubScope("buildsignal_controller"),
		stores:        stores,
		buildRunners:  buildRunners,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the build referenced by the delivery, polls its runner for
// the latest status, persists a real transition, and either holds the delivery
// until the next poll or, once terminal, releases the queue's build slot,
// projects the outcome onto the request, and publishes the request id to
// record. Returns nil to ack (success) or an error to nack (retry) / reject
// (DLQ).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()

	sig := &stovepipemq.BuildSignal{}
	if err := stovepipemq.Unmarshal(msg.Payload, sig); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize build signal: %w", err)
	}

	store, err := c.stores.For(storage.Config{QueueName: sig.GetQueueName()})
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_resolve_errors", 1)
		// Non-retryable: a missing or unresolvable queue is a malformed message.
		return fmt.Errorf("failed to resolve storage for queue %q: %w", sig.GetQueueName(), err)
	}

	build, err := c.loadBuild(ctx, store, sig.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	request, err := c.loadRequest(ctx, store, build.RequestID)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	// The payload's queue must match the request's authoritative queue; a
	// mismatch is a malformed message. Non-retryable — reject to the DLQ.
	if sig.GetQueueName() != "" && sig.GetQueueName() != request.Queue {
		metrics.NamedCounter(c.metricsScope, _opName, "queue_mismatch", 1)
		return fmt.Errorf("payload queue %q does not match queue %q of request %s", sig.GetQueueName(), request.Queue, request.ID)
	}

	// A request only reaches the poll loop after process admitted it, so it is
	// `processing` — or it already carries this build's outcome, on a redelivery
	// after the outcome was stamped but before record was published. Both cases
	// proceed: the redelivery re-publishes to record rather than dropping the
	// signal, and every write below is a no-op the second time.
	//
	// Anything else is terminal without a build outcome, which today means only
	// `superseded` — and process supersedes solely from `accepted` (see
	// supersedeRequest), so a superseded request can never reach this point. It
	// is guarded rather than assumed away because finishRequest would otherwise
	// release a build slot the request never claimed: superseding consumes none.
	if request.State != entity.RequestStateProcessing && !request.State.HasBuildOutcome() {
		return nil
	}

	buildRunner, err := c.buildRunners.For(buildrunner.Config{QueueName: request.Queue})
	if err != nil {
		// A queue with no registered builder is a config error.
		return fmt.Errorf("failed to resolve build runner for queue %s: %w", request.Queue, err)
	}

	status, _, err := buildRunner.Status(ctx, entity.BuildID{ID: build.ID})
	if err != nil {
		return fmt.Errorf("failed to poll status for build %s: %w", build.ID, err)
	}

	effective, err := c.reconcile(ctx, store, build, status)
	if err != nil {
		return err
	}

	if effective.IsTerminal() {
		if err := c.finishRequest(ctx, store, &request, effective); err != nil {
			return err
		}
		if err := c.publishRecord(ctx, request.ID, request.Queue); err != nil {
			return fmt.Errorf("failed to publish record for request %s: %w", request.ID, err)
		}
		c.logger.Infow("build reached terminal status",
			"build_id", build.ID,
			"request_id", request.ID,
			"status", string(effective),
			"request_state", string(request.State),
		)
		return nil
	}

	// Not terminal yet: hold the delivery so this same message redelivers
	// after the poll delay — the partition (keyed by build id) sleeps with it,
	// and the redelivery does not count toward the retry limit.
	delayMs := pollDelay(effective)
	delivery.Hold(delayMs)
	c.logger.Debugw("holding for next build status poll",
		"build_id", build.ID,
		"status", string(effective),
		"delay_ms", delayMs,
	)
	return nil
}

// finishRequest releases the queue's build slot and projects the build's
// terminal status onto the request, leaving it terminal. It is a no-op when the
// request already carries an outcome, so a redelivery neither double-releases
// the slot nor rewrites the state.
//
// The slot is released *before* the terminal write, and a failed release aborts
// the write, matching the DLQ reconciler (see stovepipe/controller/dlq/dlq.go).
// Both rules exist for the same reason: the request must not go terminal while
// still holding a slot, because a terminal request is skipped on redelivery and
// by the reconciler, so nothing would ever decrement it. Failing this way leaves
// the request non-terminal, so redelivery re-runs both steps and decrements again
// — transiently over-admitting by one until releaseBuildSlot's zero clamp
// reconverges, which is the failure mode this pipeline prefers.
func (c *Controller) finishRequest(ctx context.Context, store storage.Storage, request *entity.Request, status entity.BuildStatus) error {
	if request.State.HasBuildOutcome() {
		return nil
	}

	if err := c.releaseBuildSlot(ctx, store, request.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}

	if err := c.markOutcome(ctx, store, request, outcomeState(status)); err != nil {
		metrics.NamedCounter(c.metricsScope, _opName, "storage_errors", 1)
		return err
	}
	return nil
}

// outcomeState maps a terminal build status onto the request state that records
// it. Cancelled stays distinct from failed: what a cancellation implies for
// greenness is decided where greenness is recorded.
func outcomeState(status entity.BuildStatus) entity.RequestState {
	switch status {
	case entity.BuildStatusSucceeded:
		return entity.RequestStateSucceeded
	case entity.BuildStatusCancelled:
		return entity.RequestStateCancelled
	default:
		return entity.RequestStateFailed
	}
}

// markOutcome CAS-transitions request from processing to state, retrying on version
// conflicts. First writer wins: once any outcome is recorded a later caller leaves it
// alone, so duplicate builds for one request (which build.md accepts) cannot flip the
// verdict back and forth.
func (c *Controller) markOutcome(ctx context.Context, store storage.Storage, request *entity.Request, state entity.RequestState) error {
	reqStore := store.GetRequestStore()

	for {
		if request.State != entity.RequestStateProcessing {
			return nil
		}

		updated := *request
		updated.State = state
		newVersion := request.Version + 1
		if err := reqStore.Update(ctx, updated, request.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				got, getErr := reqStore.Get(ctx, request.ID)
				if getErr != nil {
					return fmt.Errorf("failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				*request = got
				continue
			}
			return fmt.Errorf("failed to mark request %s %s: %w", request.ID, state, err)
		}
		updated.Version = newVersion
		*request = updated
		metrics.NamedCounter(c.metricsScope, _opName, "outcomes", 1,
			metrics.NewTag("state", string(state)),
		)
		return nil
	}
}

// releaseBuildSlot CAS-decrements the queue's in_flight_count, reopening the process
// concurrency gate now that this request's build is over. It decrements relatively
// (preserving concurrent updates), clamps at zero, and retries on version conflicts.
// Unlike process's unwind-path release this is not best-effort: the caller must not
// mark the request terminal if the slot was not freed, so a hard failure is returned.
func (c *Controller) releaseBuildSlot(ctx context.Context, store storage.Storage, queueName string) error {
	queueStore := store.GetQueueStore()

	for {
		queueRow, err := queueStore.Get(ctx, queueName)
		if err != nil {
			return fmt.Errorf("failed to load queue %s to release build slot: %w", queueName, err)
		}
		if queueRow.InFlightCount <= 0 {
			return nil
		}

		updated := queueRow
		updated.InFlightCount = queueRow.InFlightCount - 1
		newVersion := queueRow.Version + 1
		if err := queueStore.Update(ctx, updated, queueRow.Version, newVersion); err != nil {
			if errors.Is(err, storage.ErrVersionMismatch) {
				continue
			}
			return fmt.Errorf("failed to release build slot for queue %s: %w", queueName, err)
		}
		metrics.NamedCounter(c.metricsScope, _opName, "slot_released", 1)
		return nil
	}
}

// reconcile persists a real status transition and returns the status that
// should drive the rest of Process: the polled status when persisted (or
// already unchanged), or the stored status when a stored terminal status is
// write-once-protected against a differing poll.
func (c *Controller) reconcile(ctx context.Context, store storage.Storage, build entity.Build, status entity.BuildStatus) (entity.BuildStatus, error) {
	if status == build.Status {
		return build.Status, nil
	}

	// Terminal is write-once: a later poll of a flaky backend must never
	// overwrite an already-committed terminal status.
	if build.Status.IsTerminal() {
		return build.Status, nil
	}

	newVersion := build.Version + 1
	updated := build
	updated.Status = status
	if err := store.GetBuildStore().Update(ctx, updated, build.Version, newVersion); err != nil {
		return "", fmt.Errorf("failed to persist status for build %s: %w", build.ID, err)
	}
	return status, nil
}

// loadBuild returns the build for id.
func (c *Controller) loadBuild(ctx context.Context, store storage.Storage, id string) (entity.Build, error) {
	return loader.ByID(ctx, id, store.GetBuildStore().Get, "build")
}

// loadRequest returns the request for id.
func (c *Controller) loadRequest(ctx context.Context, store storage.Storage, id string) (entity.Request, error) {
	return loader.ByID(ctx, id, store.GetRequestStore().Get, "request")
}

// pollDelay returns the delay before the next Status call for a non-terminal status.
func pollDelay(status entity.BuildStatus) int64 {
	switch status {
	case entity.BuildStatusRunning:
		return PollDelayRunningMs
	default:
		// Accepted and any unknown non-terminal status.
		return PollDelayAcceptedMs
	}
}

// publishRecord publishes requestID to the record stage, partitioned by request
// id. The message id is the request id too, so a redelivery republishing the
// same terminal signal dedups into the original message rather than enqueuing a
// second one.
func (c *Controller) publishRecord(ctx context.Context, requestID, queue string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: requestID, QueueName: queue})
	if err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	msg := entityqueue.NewMessage(requestID, payload, requestID, nil)
	return c.publish(ctx, stovepipemq.TopicKeyRecord, msg)
}

// publish sends msg to the queue registered for key.
func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, msg entityqueue.Message) error {
	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}
	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}
	return q.Publisher().Publish(ctx, topicName, msg)
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "buildsignal"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
