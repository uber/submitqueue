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

package dlq

import (
	"context"
	"errors"
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	corerequest "github.com/uber/submitqueue/submitqueue/orchestrator/core/request"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

type startController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// NewDLQStartController reconciles failed submissions without restarting processing.
func NewDLQStartController(logger *zap.SugaredLogger, scope tally.Scope, stores orchstorage.Factory, registry consumer.TopicRegistry, topicKey consumer.TopicKey, consumerGroup string) consumer.Controller {
	return &startController{
		logger:        logger.Named("start_dlq"),
		metricsScope:  scope.SubScope("start_dlq"),
		stores:        stores,
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

func (c *startController) Process(ctx context.Context, delivery consumer.Delivery) (err error) {
	defer func() {
		if err != nil {
			metrics.NamedCounter(c.metricsScope, "process", "reconcile_errors", 1, metrics.TagsFromContext(ctx)...)
		}
	}()

	msg := delivery.Message()
	rid, err := DecodeLandRequestID(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to decode start dlq payload: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, rid.Queue); err != nil {
		metrics.NamedCounter(c.metricsScope, "process", "queue_identity_errors", 1, metrics.TagsFromContext(ctx)...)
		c.logger.Warnw("invalid start dlq queue identity", "error", err)
		return nil
	}
	if rid.ID == "" {
		return fmt.Errorf("dlq payload decoded to empty request id")
	}

	store, err := c.stores.For(orchstorage.Config{QueueName: msg.Tenant})
	if err != nil {
		return fmt.Errorf("failed to resolve storage for queue %q: %w", rid.Queue, err)
	}
	lastError, metadata := failureContext(delivery)
	c.logger.Warnw("reconciling failed start", "request_id", rid.ID, "queue", rid.Queue, "attempt", delivery.Attempt())
	request, err := store.GetRequestStore().Get(ctx, rid.ID)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		err = c.publishStartFailure(ctx, rid.ID, msg.Tenant, 0, lastError, metadata)
	case err == nil:
		err = c.failExistingStartRequest(ctx, store, request, rid, lastError, metadata)
	}
	return err
}

func (c *startController) failExistingStartRequest(ctx context.Context, store orchstorage.Storage, request entity.Request, rid entity.RequestID, lastError string, metadata map[string]string) error {
	if request.ID != rid.ID || request.Queue != rid.Queue {
		return fmt.Errorf("conflicting request identity for failed start request %s", rid.ID)
	}
	// A Start retry can fail after an earlier delivery published validation and the request joined a batch.
	// Let that batch finish instead of changing its request to Error.
	owner, err := owningBatch(ctx, store, request.ID)
	if err != nil || owner != "" {
		return err
	}
	switch request.State {
	case entity.RequestStateError:
		_, err := corerequest.TerminateRequest(ctx, store, c.registry, request.ID, entity.RequestStateError, lastError, metadata)
		return err
	case entity.RequestStateLanded, entity.RequestStateCancelled:
		return nil
	}
	updated := request
	updated.State = entity.RequestStateError
	newVersion := request.Version + 1
	// The observed version guards against a batch claim or terminal winner after the owner lookup.
	if err := store.GetRequestStore().Update(ctx, updated, request.Version, newVersion); err != nil {
		return fmt.Errorf("failed to fail start request: %w", err)
	}
	updated.Version = newVersion
	return c.publishStartFailure(ctx, updated.ID, updated.Queue, updated.Version, lastError, metadata)
}

func (c *startController) publishStartFailure(ctx context.Context, requestID, queue string, version int32, lastError string, metadata map[string]string) error {
	entry := entity.NewRequestStatusLog(queue, requestID, entity.RequestStatusError, version, lastError, metadata)
	return corerequest.PublishLog(ctx, c.registry, entry, requestID, "")
}

func (c *startController) Name() string {
	return string(c.topicKey)
}

func (c *startController) TopicKey() consumer.TopicKey {
	return c.topicKey
}

func (c *startController) ConsumerGroup() string {
	return c.consumerGroup
}
