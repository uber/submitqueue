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

package periodicmetrics

import (
	"context"
	"fmt"
	"time"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"go.uber.org/zap"
)

const (
	_observePeriod  = time.Minute
	_observeTimeout = 45 * time.Second
)

type task struct {
	logger        *zap.SugaredLogger
	observer      Controller
	topicKey      consumer.TopicKey
	consumerGroup string
	now           func() time.Time
}

func NewTask(logger *zap.SugaredLogger, observer Controller, topicKey consumer.TopicKey, consumerGroup string) consumer.Controller {
	return &task{logger: logger, observer: observer, topicKey: topicKey, consumerGroup: consumerGroup, now: time.Now}
}

// Schedule seeds a single recurring task. Concurrent calls for the same queue
// deduplicate; the task stays unacknowledged while held between observations.
func Schedule(ctx context.Context, registry consumer.TopicRegistry, topicKey consumer.TopicKey, queue string) error {
	if queue == "" {
		return fmt.Errorf("cannot schedule an unnamed queue")
	}
	payload, err := stovepipemq.Marshal(&stovepipemq.PeriodicMetrics{QueueName: queue})
	if err != nil {
		return fmt.Errorf("marshal periodic metrics task: %w", err)
	}
	// Deduplication already scopes the ID to the tenant, topic, and partition.
	return publish.Message(ctx, registry, topicKey, publish.MessageParams{
		Tenant:       queue,
		ID:           "periodicmetrics",
		Payload:      payload,
		PartitionKey: queue,
	})
}

func (t *task) Process(ctx context.Context, delivery consumer.Delivery) error {
	message := delivery.Message()
	request := &stovepipemq.PeriodicMetrics{}
	if err := stovepipemq.Unmarshal(message.Payload, request); err != nil {
		return fmt.Errorf("deserialize periodic metrics task: %w", err)
	}
	if err := entityqueue.ValidatePayloadQueue(message, request.GetQueueName()); err != nil {
		return fmt.Errorf("invalid periodic metrics identity: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	started := t.now()
	observeCtx, cancel := context.WithTimeout(ctx, _observeTimeout)
	defer cancel()
	if err := t.observer.Observe(observeCtx, request.GetQueueName()); err != nil {
		t.logger.Errorw("failed to observe queue health", "queue", request.GetQueueName(), "error", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep the durable task alive after observation failures; the next sample is fresh work.
	delivery.Hold(max(_observePeriod-t.now().Sub(started), 0).Milliseconds())
	return nil
}

func (t *task) Name() string                { return _metricsName }
func (t *task) TopicKey() consumer.TopicKey { return t.topicKey }
func (t *task) ConsumerGroup() string       { return t.consumerGroup }
