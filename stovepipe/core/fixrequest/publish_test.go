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

package fixrequest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"go.uber.org/mock/gomock"
)

func newTestRegistry(t *testing.T) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	queue := queuemock.NewMockQueue(ctrl)
	publisher := queuemock.NewMockPublisher(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: mq.TopicKeyFix, Name: "test-fix", Queue: queue},
	})
	require.NoError(t, err)
	return registry, publisher
}

func TestPublishBuildAndPassIdentityAndRetry(t *testing.T) {
	registry, publisher := newTestRegistry(t)
	var messages []entityqueue.Message
	publisher.EXPECT().Publish(gomock.Any(), "test-fix", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, message entityqueue.Message) error {
			messages = append(messages, message)
			if len(messages) == 1 {
				return assert.AnError
			}
			return nil
		}).Times(5)

	ctx := context.Background()
	require.ErrorIs(t, Publish(ctx, registry, "validation/main", "Build/42 ", "pass/1"), assert.AnError)
	require.NoError(t, Publish(ctx, registry, "validation/main", "Build/42 ", "pass/1"))
	require.NoError(t, Publish(ctx, registry, "validation/main", "Build/42 ", "pass/2"))
	require.NoError(t, Publish(ctx, registry, "validation/main", "Build/43", "pass/1"))
	require.NoError(t, Publish(ctx, registry, "validation/other", "Build/42 ", "pass/1"))

	assert.Equal(t, messages[0].ID, messages[1].ID)
	assert.Equal(t, messages[0].PartitionKey, messages[1].PartitionKey)
	assert.NotEqual(t, messages[1].ID, messages[2].ID)
	assert.Equal(t, messages[1].PartitionKey, messages[2].PartitionKey)
	assert.Equal(t, messages[1].Payload, messages[2].Payload)
	assert.NotEqual(t, messages[1].ID, messages[3].ID)
	assert.NotEqual(t, messages[1].Tenant, messages[4].Tenant)
	for _, message := range messages {
		payload := &mq.FixRequest{}
		require.NoError(t, mq.Unmarshal(message.Payload, payload))
		assert.Equal(t, payload.GetId(), message.PartitionKey)
		assert.Equal(t, payload.GetQueueName(), message.Tenant)
		assert.Equal(t, message.Tenant, message.Metadata[entityqueue.MetadataKeyQueueName])
	}
	assert.Equal(t, "Build/42 ", messages[0].PartitionKey)
}

func TestPublishRejectsMissingIdentity(t *testing.T) {
	for _, test := range []struct {
		name         string
		queue        string
		build        string
		notification string
	}{
		{name: "missing queue", build: "build/42", notification: "pass/1"},
		{name: "missing build", queue: "validation/main", notification: "pass/1"},
		{name: "missing notification", queue: "validation/main", build: "build/42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, _ := newTestRegistry(t)
			require.Error(t, Publish(context.Background(), registry, test.queue, test.build, test.notification))
		})
	}
}

func TestPublishReturnsConfigurationAndSerializationErrors(t *testing.T) {
	require.Error(t, Publish(context.Background(), consumer.TopicRegistry{}, "validation/main", "build/42", "pass/1"))
	registry, _ := newTestRegistry(t)
	require.Error(t, Publish(context.Background(), registry, "validation/main", string([]byte{0xff}), "pass/1"))
}
