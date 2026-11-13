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
	"fmt"

	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	corebatch "github.com/uber/submitqueue/submitqueue/orchestrator/core/batch"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	"go.uber.org/zap"
)

type concludeController struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	stores        orchstorage.Factory
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

// NewDLQConcludeController repairs terminal membership and request fan-out without changing the batch outcome.
func NewDLQConcludeController(logger *zap.SugaredLogger, scope tally.Scope, stores orchstorage.Factory, registry consumer.TopicRegistry, topicKey consumer.TopicKey, consumerGroup string) consumer.Controller {
	name := string(topicKey) + "_controller"
	return &concludeController{
		logger: logger.Named(name), metricsScope: scope.SubScope(name), stores: stores,
		registry: registry, topicKey: topicKey, consumerGroup: consumerGroup,
	}
}

func (c *concludeController) Process(ctx context.Context, delivery consumer.Delivery) error {
	msg := delivery.Message()
	bid, err := sqmq.UnmarshalBatchID(primaryTopicKey(c.topicKey), msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to decode conclude batch ID: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(msg, bid.Queue); err != nil {
		return fmt.Errorf("invalid conclude message identity: %w", err)
	}
	if bid.ID == "" {
		return fmt.Errorf("conclude dlq payload decoded to empty batch ID")
	}
	store, err := c.stores.For(orchstorage.Config{QueueName: bid.Queue})
	if err != nil {
		return fmt.Errorf("failed to resolve storage for queue %q: %w", bid.Queue, err)
	}
	batch, err := store.GetBatchStore().Get(ctx, bid.ID)
	if err != nil {
		return fmt.Errorf("failed to get conclude batch %s: %w", bid.ID, err)
	}
	if batch.Queue != bid.Queue {
		return fmt.Errorf("payload queue %q does not match queue %q of batch %s", bid.Queue, batch.Queue, batch.ID)
	}
	// A failed conclusion is not evidence of a failed merge.
	if !batch.State.IsTerminal() {
		return fmt.Errorf("cannot conclude non-terminal batch %s in state %q", batch.ID, batch.State)
	}
	if err := corebatch.EnsureRecord(ctx, store, batch); err != nil {
		return err
	}
	return corebatch.ConcludeBatchRequests(ctx, store, c.registry, c.logger, c.metricsScope, batch, msg.Metadata[topickey.MetadataKeyFailureReason])
}

func (c *concludeController) Name() string                { return string(c.topicKey) }
func (c *concludeController) TopicKey() consumer.TopicKey { return c.topicKey }
func (c *concludeController) ConsumerGroup() string       { return c.consumerGroup }
