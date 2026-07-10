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
// ids from ingest, reloads the Request from storage, coalesces older heads,
// admits the latest head to build, and reschedules when the concurrency gate is closed.
package process

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/platform/metrics"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes ProcessRequest messages from the process stage, reloads the
// referenced Request from storage, coalesces older heads, and admits the latest to
// build. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
	queueConfigs  queueconfig.Store
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// Verify Controller implements consumer.Controller interface at compile time.
var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new process controller.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	store storage.Storage,
	queueConfigs queueconfig.Store,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_controller"),
		metricsScope:  scope.SubScope("process_controller"),
		store:         store,
		queueConfigs:  queueConfigs,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the request referenced by the delivery, coalesces older heads,
// and admits the latest to build. Returns nil to ack (success) or an error to nack (retry).
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	pr := &stovepipemq.ProcessRequest{}
	if err := stovepipemq.Unmarshal(msg.Payload, pr); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		// Non-retryable: a malformed message will never succeed regardless of retries.
		return fmt.Errorf("failed to deserialize process request: %w", err)
	}

	request, err := c.loadRequest(ctx, pr.Id)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "storage_errors", 1)
		return err
	}

	switch request.State {
	case entity.RequestStateSuperseded:
		return nil
	case entity.RequestStateProcessing:
		return c.republishBuild(ctx, request)
	case entity.RequestStateAccepted:
		return c.processAccepted(ctx, request)
	default:
		c.logger.Infow("ignored request in unexpected state",
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
		return err
	}

	if queueRow.LatestRequestID == "" {
		c.logger.Infow("latest head awaiting ingest pointer",
			"request_id", request.ID,
			"queue", request.Queue,
			"uri", request.URI,
		)
		return nil
	}

	cmp, err := entity.CompareRequestID(request.Queue, request.ID, queueRow.LatestRequestID)
	if err != nil {
		return fmt.Errorf("ProcessController failed to compare request ids for queue %s: %w", request.Queue, err)
	}
	if cmp < 0 {
		if err := c.supersedeRequest(ctx, request); err != nil {
			return err
		}
		c.logger.Infow("superseded request for newer head",
			"request_id", request.ID,
			"queue", request.Queue,
			"latest_request_id", queueRow.LatestRequestID,
		)
		return nil
	}

	cfg, err := c.queueConfigs.Get(ctx, request.Queue)
	if err != nil {
		return fmt.Errorf("ProcessController failed to load queue config for %s: %w", request.Queue, err)
	}

	if queueRow.InFlightCount >= cfg.MaxConcurrent {
		return c.rescheduleProcess(ctx, request.ID, request.Queue, cfg.GateWaitDelayMs)
	}

	return c.admitRequest(ctx, request, queueRow, cfg.MaxConcurrent)
}

// rescheduleProcess acks the current delivery (by returning nil after success) and
// re-enqueues the same ProcessRequest after a short delay so the gate can be
// re-checked without burning MaxAttempts.
func (c *Controller) rescheduleProcess(ctx context.Context, id, queue string, delayMs int64) error {
	if err := c.publishProcess(ctx, id, queue, delayMs); err != nil {
		return fmt.Errorf("ProcessController failed to reschedule process request %s: %w", id, err)
	}
	c.logger.Infow("rescheduled latest head awaiting build slot",
		"request_id", id,
		"queue", queue,
		"delay_ms", delayMs,
	)
	return nil
}

// admitRequest admits the latest head: cold-start full build, increment in_flight_count,
// transition accepted→processing, and publish to build.
func (c *Controller) admitRequest(ctx context.Context, request entity.Request, queueRow entity.Queue, maxConcurrent int32) error {
	if err := c.incrementInFlightCount(ctx, &queueRow, maxConcurrent); err != nil {
		return err
	}

	strategy := entity.BuildStrategyFull
	if err := c.transitionToProcessing(ctx, &request, strategy, ""); err != nil {
		return err
	}

	if err := c.publishBuild(ctx, request.ID, request.Queue); err != nil {
		return err
	}

	c.logger.Infow("admitted request to build",
		"request_id", request.ID,
		"queue", request.Queue,
		"build_strategy", string(strategy),
	)
	return nil
}

// republishBuild re-publishes a processing request to build after a prior publish may have failed.
func (c *Controller) republishBuild(ctx context.Context, request entity.Request) error {
	if err := c.publishBuild(ctx, request.ID, request.Queue); err != nil {
		return err
	}
	c.logger.Infow("republished processing request to build",
		"request_id", request.ID,
		"queue", request.Queue,
	)
	return nil
}

// incrementInFlightCount CAS-increments queue.in_flight_count, retrying on version conflicts.
func (c *Controller) incrementInFlightCount(ctx context.Context, queueRow *entity.Queue, maxConcurrent int32) error {
	queueStore := c.store.GetQueueStore()

	for {
		if queueRow.InFlightCount >= maxConcurrent {
			return fmt.Errorf("ProcessController gate closed for queue %s", queueRow.Name)
		}

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
				continue
			}
			return fmt.Errorf("ProcessController failed to increment in_flight_count for queue %s: %w", queueRow.Name, err)
		}
		*queueRow = updated
		queueRow.Version = newVersion
		return nil
	}
}

// transitionToProcessing CAS-marks request accepted→processing with strategy fields,
// retrying on version conflicts.
func (c *Controller) transitionToProcessing(
	ctx context.Context,
	request *entity.Request,
	strategy entity.BuildStrategy,
	baseURI string,
) error {
	reqStore := c.store.GetRequestStore()

	for {
		if request.State != entity.RequestStateAccepted {
			return nil
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
					return fmt.Errorf("ProcessController failed to reload request %s after version mismatch: %w", request.ID, getErr)
				}
				*request = got
				continue
			}
			return fmt.Errorf("ProcessController failed to transition request %s to processing: %w", request.ID, err)
		}
		*request = updated
		request.Version = newVersion
		return nil
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

// publishProcess publishes the request ID to the process stage, partitioned by queue.
// delayMs > 0 uses PublishAfter for gate-wait reschedules.
func (c *Controller) publishProcess(ctx context.Context, id, queue string, delayMs int64) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: id})
	if err != nil {
		return fmt.Errorf("failed to serialize process request: %w", err)
	}

	msg := entityqueue.NewMessage(id, payload, queue, nil)

	q, ok := c.registry.Queue(c.topicKey)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", c.topicKey)
	}
	topicName, ok := c.registry.TopicName(c.topicKey)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", c.topicKey)
	}

	publisher := q.Publisher()
	if delayMs > 0 {
		return publisher.PublishAfter(ctx, topicName, msg, delayMs)
	}
	return publisher.Publish(ctx, topicName, msg)
}

// publishBuild publishes the request ID to the build stage, partitioned by queue.
func (c *Controller) publishBuild(ctx context.Context, id, queue string) error {
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id})
	if err != nil {
		return fmt.Errorf("failed to serialize build request: %w", err)
	}

	msg := entityqueue.NewMessage(id, payload, queue, nil)

	q, ok := c.registry.Queue(stovepipemq.TopicKeyBuild)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", stovepipemq.TopicKeyBuild)
	}
	topicName, ok := c.registry.TopicName(stovepipemq.TopicKeyBuild)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", stovepipemq.TopicKeyBuild)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish build request: %w", err)
	}
	return nil
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
