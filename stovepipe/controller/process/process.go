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
// (in later changes) gates concurrency, decides build strategy, and admits
// winners to build.
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
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/zap"
)

// Controller consumes ProcessRequest messages from the process stage, reloads the
// referenced Request from storage, and coalesces older heads. Implements consumer.Controller.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	store         storage.Storage
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
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("process_controller"),
		metricsScope:  scope.SubScope("process_controller"),
		store:         store,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process reloads the request referenced by the delivery and coalesces older heads.
// Returns nil to ack (success) or an error to nack (retry).
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
	case entity.RequestStateSuperseded, entity.RequestStateProcessing:
		return nil
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

// processAccepted coalesces older heads against queue.latest_request_id. The latest
// head is left in accepted until admit and the concurrency gate land in later PRs.
func (c *Controller) processAccepted(ctx context.Context, request entity.Request) error {
	queueRow, err := c.loadQueue(ctx, request.Queue)
	if err != nil {
		return err
	}

	if queueRow.LatestRequestID == "" {
		c.logger.Infow("latest head awaiting admit",
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

	c.logger.Infow("latest head awaiting admit",
		"request_id", request.ID,
		"queue", request.Queue,
		"uri", request.URI,
	)
	return nil
}

// supersedeRequest CAS-marks request accepted→superseded, retrying on version conflicts.
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
