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

package consumer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/core/consumer"
	extqueue "github.com/uber/submitqueue/extension/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"go.uber.org/mock/gomock"
)

func TestNewTopicRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockQ := queuemock.NewMockQueue(ctrl)

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{
				Key:   topickey.TopicKeyStart,
				Name:  "request",
				Queue: mockQ,
				Subscription: extqueue.DefaultSubscriptionConfig(
					"worker-1", "group-a",
				),
			},
		},
	)
	require.NoError(t, err)

	q, ok := registry.Queue(topickey.TopicKeyStart)
	require.True(t, ok)
	assert.Equal(t, mockQ, q)

	name, ok := registry.TopicName(topickey.TopicKeyStart)
	require.True(t, ok)
	assert.Equal(t, "request", name)

	cfg, ok := registry.SubscriptionConfig(topickey.TopicKeyStart, "group-a")
	require.True(t, ok)
	assert.Equal(t, "group-a", cfg.ConsumerGroup)
}

func TestNewTopicRegistry_InvalidTopicName(t *testing.T) {
	tests := []struct {
		name      string
		topicName string
	}{
		{
			name:      "empty name",
			topicName: "",
		},
		{
			name:      "uppercase letters",
			topicName: "InvalidTopic",
		},
		{
			name:      "dots",
			topicName: "my.topic",
		},
		{
			name:      "spaces",
			topicName: "my topic",
		},
		{
			name:      "special chars",
			topicName: "topic!@#",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{
					{Key: topickey.TopicKeyStart, Name: tt.topicName},
				},
			)
			require.Error(t, err)
		})
	}
}

func TestTopicRegistry_SubscriptionConfig(t *testing.T) {
	tests := []struct {
		name          string
		configs       []consumer.TopicConfig
		lookupKey     consumer.TopicKey
		lookupGroup   string
		expectFound   bool
		expectedGroup string
	}{
		{
			name: "found group-a",
			configs: []consumer.TopicConfig{
				{
					Key:  topickey.TopicKeyStart,
					Name: "request",
					Subscription: extqueue.DefaultSubscriptionConfig(
						"worker-1", "group-a",
					),
				},
			},
			lookupKey:     topickey.TopicKeyStart,
			lookupGroup:   "group-a",
			expectFound:   true,
			expectedGroup: "group-a",
		},
		{
			name: "not found by group",
			configs: []consumer.TopicConfig{
				{
					Key:  topickey.TopicKeyStart,
					Name: "request",
					Subscription: extqueue.DefaultSubscriptionConfig(
						"worker-1", "group-a",
					),
				},
			},
			lookupKey:   topickey.TopicKeyStart,
			lookupGroup: "nonexistent",
			expectFound: false,
		},
		{
			name: "not found by topic key",
			configs: []consumer.TopicConfig{
				{
					Key:  topickey.TopicKeyStart,
					Name: "request",
					Subscription: extqueue.DefaultSubscriptionConfig(
						"worker-1", "group-a",
					),
				},
			},
			lookupKey:   consumer.TopicKey("other"),
			lookupGroup: "group-a",
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry, err := consumer.NewTopicRegistry(tt.configs)
			require.NoError(t, err)
			config, ok := registry.SubscriptionConfig(tt.lookupKey, tt.lookupGroup)

			if !tt.expectFound {
				assert.False(t, ok)
			} else {
				require.True(t, ok)
				assert.Equal(t, tt.expectedGroup, config.ConsumerGroup)
			}
		})
	}
}

func TestTopicRegistry_Queue_PerTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockQ1 := queuemock.NewMockQueue(ctrl)
	mockQ2 := queuemock.NewMockQueue(ctrl)

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyStart, Name: "request", Queue: mockQ1},
			{Key: topickey.TopicKeyValidate, Name: "validate", Queue: mockQ2},
		},
	)
	require.NoError(t, err)

	q1, ok := registry.Queue(topickey.TopicKeyStart)
	require.True(t, ok)
	assert.Equal(t, mockQ1, q1)

	q2, ok := registry.Queue(topickey.TopicKeyValidate)
	require.True(t, ok)
	assert.Equal(t, mockQ2, q2)

	_, ok = registry.Queue(consumer.TopicKey("nonexistent"))
	assert.False(t, ok)
}

func TestTopicKey_String(t *testing.T) {
	tests := []struct {
		name     string
		key      consumer.TopicKey
		expected string
	}{
		{
			name:     "predefined topic key",
			key:      topickey.TopicKeyStart,
			expected: "start",
		},
		{
			name:     "custom topic key",
			key:      consumer.TopicKey("custom"),
			expected: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.key.String())
		})
	}
}

func TestTopicRegistry_TopicName(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockQ := queuemock.NewMockQueue(ctrl)

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyStart, Name: "my-custom-request", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	name, ok := registry.TopicName(topickey.TopicKeyStart)
	require.True(t, ok)
	assert.Equal(t, "my-custom-request", name)

	_, ok = registry.TopicName(consumer.TopicKey("nonexistent"))
	assert.False(t, ok)
}

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "valid lowercase", topic: "mytopic", wantErr: false},
		{name: "valid with numbers", topic: "topic123", wantErr: false},
		{name: "valid with underscores", topic: "my_topic_name", wantErr: false},
		{name: "valid with hyphens", topic: "my-topic", wantErr: false},
		{name: "valid mixed", topic: "abc_123-xyz", wantErr: false},
		{name: "invalid empty", topic: "", wantErr: true},
		{name: "invalid uppercase", topic: "MyTopic", wantErr: true},
		{name: "invalid dot", topic: "my.topic", wantErr: true},
		{name: "invalid space", topic: "my topic", wantErr: true},
		{name: "invalid special chars", topic: "topic!@#", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := consumer.ValidateTopicName(tt.topic)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
