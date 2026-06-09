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

package start

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/metrics"
	entitygit "github.com/uber/submitqueue/stovepipe/entity/git"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	entity "github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/zap"
)

// Controller handles start queue messages. It is the pipeline entry point: it
// deserializes the inbound change event and forwards a thin commit reference to
// the validate stage, partitioned by repository so a repo's commits stay ordered.
type Controller struct {
	logger        *zap.SugaredLogger
	metricsScope  tally.Scope
	registry      consumer.TopicRegistry
	topicKey      consumer.TopicKey
	consumerGroup string
}

var _ consumer.Controller = (*Controller)(nil)

// NewController creates a new start controller for the orchestrator.
func NewController(
	logger *zap.SugaredLogger,
	scope tally.Scope,
	registry consumer.TopicRegistry,
	topicKey consumer.TopicKey,
	consumerGroup string,
) *Controller {
	return &Controller{
		logger:        logger.Named("start_controller"),
		metricsScope:  scope.SubScope("start_controller"),
		registry:      registry,
		topicKey:      topicKey,
		consumerGroup: consumerGroup,
	}
}

// Process deserializes the change event and publishes a commit reference to validate.
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) (retErr error) {
	const opName = "process"

	op := metrics.Begin(c.metricsScope, opName)
	defer func() { op.Complete(retErr) }()

	msg := delivery.Message()

	event, err := entity.ChangeEventFromBytes(msg.Payload)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "deserialize_errors", 1)
		return fmt.Errorf("failed to deserialize change event: %w", err)
	}

	uri := event.URI
	partitionKey, err := partitionKey(uri)
	if err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "parse_errors", 1)
		return fmt.Errorf("failed to resolve partition key for uri=%s: %w", uri, err)
	}

	c.logger.Infow("received change event",
		"uri", uri,
		"attempt", delivery.Attempt(),
		"partition_key", msg.PartitionKey,
	)

	if err := c.publish(ctx, topickey.TopicKeyValidate, uri, partitionKey); err != nil {
		metrics.NamedCounter(c.metricsScope, opName, "publish_errors", 1)
		return fmt.Errorf("failed to publish to validate: %w", err)
	}

	c.logger.Infow("published commit to validate",
		"uri", uri,
		"topic_key", topickey.TopicKeyValidate,
	)

	return nil
}

func (c *Controller) publish(ctx context.Context, key consumer.TopicKey, uri string, partitionKey string) error {
	ref := entity.ChangeURI{URI: uri}
	payload, err := ref.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize change URI: %w", err)
	}

	msg := entityqueue.NewMessage(uri, payload, partitionKey, nil)

	q, ok := c.registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}

	topicName, ok := c.registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// partitionKey returns the owner/repo partition key for a commit URI so that all
// commits for a repository are processed in order on a single partition.
func partitionKey(uri string) (string, error) {
	parsed, err := entitygit.ParseChangeID(uri)
	if err != nil {
		return "", err
	}
	return parsed.OwnerRepo(), nil
}

// Name returns the controller name for logging and metrics.
func (c *Controller) Name() string {
	return "start"
}

// TopicKey returns the topic key this controller subscribes to.
func (c *Controller) TopicKey() consumer.TopicKey {
	return c.topicKey
}

// ConsumerGroup returns the consumer group for offset tracking.
func (c *Controller) ConsumerGroup() string {
	return c.consumerGroup
}
