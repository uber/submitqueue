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

package hook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	basehook "github.com/uber/submitqueue/api/base/hook"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"go.uber.org/mock/gomock"
)

const (
	testEventID      = "stovepipe/validation.repository.recorded/request/7/0"
	testPartitionKey = "request/7"
)

func testEvent() *basehook.HookEvent {
	return &basehook.HookEvent{
		Id:          testEventID,
		Source:      "stovepipe",
		Type:        "validation.repository.recorded",
		TimestampMs: 1756327200000,
	}
}

// registryWithHookTopic returns a registry whose hook topic captures whatever is
// published to it, and the slot the captured message lands in.
func registryWithHookTopic(t *testing.T, ctrl *gomock.Controller, publishErr error) (consumer.TopicRegistry, *entityqueue.Message) {
	t.Helper()

	var published entityqueue.Message
	publisher := mqmock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "domain-hook", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			published = msg
			return publishErr
		}).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: basehook.TopicKeyHook, Name: "domain-hook", Queue: queue},
	})
	require.NoError(t, err)
	return registry, &published
}

func TestPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, published := registryWithHookTopic(t, ctrl, nil)

	require.NoError(t, Publish(context.Background(), registry, testEvent(), testPartitionKey))

	// The event id is the message id, so a redelivery republishing the same
	// event dedups into the original message.
	assert.Equal(t, testEventID, published.ID)
	assert.Equal(t, testPartitionKey, published.PartitionKey)

	decoded := &basehook.HookEvent{}
	require.NoError(t, basehook.Unmarshal(published.Payload, decoded))
	assert.Equal(t, testEventID, decoded.GetId())
	assert.Equal(t, "validation.repository.recorded", decoded.GetType())
}

func TestPublish_RejectsMalformedEvent(t *testing.T) {
	tests := []struct {
		name  string
		event *basehook.HookEvent
	}{
		{name: "nil", event: nil},
		{name: "no id", event: &basehook.HookEvent{Source: "stovepipe", Type: "t"}},
		{name: "no source", event: &basehook.HookEvent{Id: testEventID, Type: "t"}},
		{name: "no type", event: &basehook.HookEvent{Id: testEventID, Source: "stovepipe"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			publisher := mqmock.NewMockPublisher(ctrl)
			queue := mqmock.NewMockQueue(ctrl)
			queue.EXPECT().Publisher().Return(publisher).AnyTimes()
			registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
				{Key: basehook.TopicKeyHook, Name: "domain-hook", Queue: queue},
			})
			require.NoError(t, err)

			// No Publish expectation: a malformed event must not reach the queue.
			require.Error(t, Publish(context.Background(), registry, tt.event, testPartitionKey))
		})
	}
}

func TestPublish_PropagatesPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry, _ := registryWithHookTopic(t, ctrl, errors.New("boom"))

	require.Error(t, Publish(context.Background(), registry, testEvent(), testPartitionKey))
}

func TestPublish_FailsWhenHookTopicIsUnregistered(t *testing.T) {
	registry, err := consumer.NewTopicRegistry(nil)
	require.NoError(t, err)

	require.Error(t, Publish(context.Background(), registry, testEvent(), testPartitionKey))
}
