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

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	"github.com/uber/submitqueue/stovepipe/controller/dlq"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"go.uber.org/zap/zaptest"
)

// recordingConsumer captures what the host registers instead of subscribing.
type recordingConsumer struct {
	controllers []consumer.Controller
}

func (c *recordingConsumer) Register(controller consumer.Controller) error {
	c.controllers = append(c.controllers, controller)
	return nil
}

func (c *recordingConsumer) Start(context.Context) error { return nil }

func (c *recordingConsumer) Stop(int64) error { return nil }

// registeredControllers runs the host's registration exactly as run() does and
// returns the registry it registers against, the primary controllers, and the
// DLQ controllers.
func registeredControllers(t *testing.T) (consumer.TopicRegistry, []consumer.Controller, []consumer.Controller) {
	t.Helper()

	registry, err := newTopicRegistry(nil, "subscriber")
	require.NoError(t, err)

	logger := zaptest.NewLogger(t).Sugar()
	store := storageFactory{}
	primary := &recordingConsumer{}
	deadLetter := &recordingConsumer{}

	_, err = registerPrimaryControllers(primary, logger, tally.NoopScope, store, requestlog.NewMaterializer(tally.NoopScope), registry,
		fakeSourceControlFactory{}, fakeBuildRunnerFactory{}, hookResolver{})
	require.NoError(t, err)

	_, err = registerDLQControllers(deadLetter, logger, tally.NoopScope, store, requestlog.NewMaterializer(tally.NoopScope))
	require.NoError(t, err)

	return registry, primary.controllers, deadLetter.controllers
}

func topicKeys(controllers []consumer.Controller) []consumer.TopicKey {
	keys := make([]consumer.TopicKey, 0, len(controllers))
	for _, c := range controllers {
		keys = append(keys, c.TopicKey())
	}
	return keys
}

func TestEveryRegisteredControllerResolvesInTheTopicRegistry(t *testing.T) {
	registry, primary, deadLetter := registeredControllers(t)

	for _, c := range append(primary, deadLetter...) {
		t.Run(c.Name(), func(t *testing.T) {
			_, ok := registry.TopicName(c.TopicKey())
			assert.True(t, ok, "no topic name registered for the key the controller subscribes to")

			_, ok = registry.SubscriptionConfig(c.TopicKey(), c.ConsumerGroup())
			assert.True(t, ok, "no subscription registered for the controller's consumer group")
		})
	}
}

func TestHookStage(t *testing.T) {
	registry, primary, deadLetter := registeredControllers(t)

	t.Run("hook events are consumed", func(t *testing.T) {
		assert.Contains(t, topicKeys(primary), basehook.TopicKeyHook)
	})

	t.Run("hook events that exhaust their retries are consumed", func(t *testing.T) {
		assert.Contains(t, topicKeys(deadLetter), dlq.TopicKey(basehook.TopicKeyHook))
	})

	// The key is shared across domains, so an unqualified name would collide with
	// another domain's hook topic on a queue backend the two share.
	t.Run("the hook topics are named for this domain", func(t *testing.T) {
		for _, key := range []consumer.TopicKey{basehook.TopicKeyHook, dlq.TopicKey(basehook.TopicKeyHook)} {
			name, ok := registry.TopicName(key)
			require.True(t, ok)
			assert.True(t, strings.HasPrefix(name, "stovepipe-"), "topic %q is not domain-qualified", name)
		}
	})
}

func TestPipelineSubscriptionRetryPolicies(t *testing.T) {
	registry, err := newTopicRegistry(nil, "subscriber")
	require.NoError(t, err)

	defaultRetry := extqueue.DefaultSubscriptionConfig("subscriber", "unused").Retry
	extendedRetry := extqueue.ExtendedRetrySubscriptionConfig("subscriber", "unused").Retry

	tests := []struct {
		name          string
		topicKey      consumer.TopicKey
		consumerGroup string
		wantRetry     extqueue.RetryConfig
	}{
		{name: "process", topicKey: stovepipemq.TopicKeyProcess, consumerGroup: "stovepipe-process", wantRetry: defaultRetry},
		{name: "build", topicKey: stovepipemq.TopicKeyBuild, consumerGroup: "stovepipe-build", wantRetry: defaultRetry},
		{name: "buildsignal", topicKey: stovepipemq.TopicKeyBuildSignal, consumerGroup: "stovepipe-buildsignal", wantRetry: extendedRetry},
		{name: "record", topicKey: stovepipemq.TopicKeyRecord, consumerGroup: "stovepipe-record", wantRetry: extendedRetry},
		{name: "hook", topicKey: basehook.TopicKeyHook, consumerGroup: "stovepipe-hook", wantRetry: defaultRetry},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, ok := registry.SubscriptionConfig(tt.topicKey, tt.consumerGroup)
			require.True(t, ok)
			assert.Equal(t, tt.wantRetry, config.Retry)
		})
	}
}

func TestDLQSubscriptionRetryPoliciesRemainUnlimited(t *testing.T) {
	registry, _, deadLetter := registeredControllers(t)

	for _, controller := range deadLetter {
		t.Run(controller.Name(), func(t *testing.T) {
			config, ok := registry.SubscriptionConfig(controller.TopicKey(), controller.ConsumerGroup())
			require.True(t, ok)
			assert.Zero(t, config.Retry.MaxAttempts)
			assert.False(t, config.DLQ.Enabled)
		})
	}
}
