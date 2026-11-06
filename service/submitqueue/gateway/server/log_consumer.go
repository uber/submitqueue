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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	genericerrs "github.com/uber/submitqueue/platform/errs/generic"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	"github.com/uber/submitqueue/platform/lifecycle"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	logctrl "github.com/uber/submitqueue/submitqueue/gateway/controller/log"
	requestcore "github.com/uber/submitqueue/submitqueue/gateway/core/request"
	"go.uber.org/zap"
)

const (
	logDLQSuffix        = "_dlq"
	logDLQTopicKey      = topickey.TopicKeyLog + consumer.TopicKey(logDLQSuffix)
	logConsumerGroup    = "gateway-log"
	logDLQConsumerGroup = "gateway-log-dlq"
)

func newGatewayTopicRegistry(queue extqueue.Queue, subscriberName string) (consumer.TopicRegistry, error) {
	primary := extqueue.DefaultSubscriptionConfig(subscriberName, logConsumerGroup)
	primary.DLQ.TopicSuffix = logDLQSuffix
	return consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyStart, Name: "start", Queue: queue},
		{Key: topickey.TopicKeyCancel, Name: "cancel", Queue: queue},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue, Subscription: primary},
		{
			Key: logDLQTopicKey, Name: "log" + logDLQSuffix, Queue: queue,
			Subscription: extqueue.DLQSubscriptionConfig(subscriberName, logDLQConsumerGroup),
		},
	})
}

func newLogConsumers(logger *zap.SugaredLogger, scope tally.Scope, registry consumer.TopicRegistry, materializer *requestcore.Materializer, gate consumergate.Gate) (*lifecycle.Group, error) {
	policies := []struct {
		topic     consumer.TopicKey
		group     string
		processor errs.ErrorProcessor
	}{
		{topickey.TopicKeyLog, logConsumerGroup, errs.NewClassifierProcessor(genericerrs.Classifier, mysqlerrs.Classifier)},
		{logDLQTopicKey, logDLQConsumerGroup, errs.AlwaysRetryableProcessor},
	}
	components := make([]lifecycle.Component, 0, len(policies))
	for _, policy := range policies {
		c := consumer.New(logger, scope.SubScope("consumer"), registry, policy.processor, gate)
		if err := c.Register(logctrl.NewController(logger, scope, materializer, policy.topic, policy.group)); err != nil {
			return nil, fmt.Errorf("failed to register log controller: %w", err)
		}
		components = append(components, logConsumerLifecycle{Consumer: c})
	}
	return lifecycle.NewGroup(components...), nil
}

type logConsumerLifecycle struct {
	consumer.Consumer
}

func (c logConsumerLifecycle) Stop(ctx context.Context) error {
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, max(time.Until(deadline), 0))
	}
	return c.Consumer.Stop(timeout.Milliseconds())
}
