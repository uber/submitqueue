package consumer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/core/consumer"
	extqueue "github.com/uber/submitqueue/extension/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
)

func TestNewTopicRegistry(t *testing.T) {
	tests := []struct {
		name    string
		configs []extqueue.SubscriptionConfig
	}{
		{
			name: "with configs",
			configs: []extqueue.SubscriptionConfig{
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a"),
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-b"),
			},
		},
		{
			name:    "nil configs",
			configs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockQ := queuemock.NewMockQueue(ctrl)

			registry := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{{Topic: consumer.TopicRequest, Queue: mockQ}},
				tt.configs,
			)

			q, ok := registry.Queue(consumer.TopicRequest)
			require.True(t, ok)
			assert.Equal(t, mockQ, q)

			if tt.configs == nil {
				_, ok := registry.SubscriptionConfig(consumer.TopicRequest, "group-a")
				assert.False(t, ok)
			}
		})
	}
}

func TestTopicRegistry_SubscriptionConfig(t *testing.T) {
	tests := []struct {
		name          string
		configs       []extqueue.SubscriptionConfig
		lookupTopic   consumer.Topic
		lookupGroup   string
		expectFound   bool
		expectedGroup string
	}{
		{
			name: "found group-a",
			configs: []extqueue.SubscriptionConfig{
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a"),
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-b"),
			},
			lookupTopic:   consumer.TopicRequest,
			lookupGroup:   "group-a",
			expectFound:   true,
			expectedGroup: "group-a",
		},
		{
			name: "found group-b",
			configs: []extqueue.SubscriptionConfig{
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a"),
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-b"),
			},
			lookupTopic:   consumer.TopicRequest,
			lookupGroup:   "group-b",
			expectFound:   true,
			expectedGroup: "group-b",
		},
		{
			name: "not found by group",
			configs: []extqueue.SubscriptionConfig{
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a"),
			},
			lookupTopic: consumer.TopicRequest,
			lookupGroup: "nonexistent",
			expectFound: false,
		},
		{
			name: "not found by topic",
			configs: []extqueue.SubscriptionConfig{
				extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a"),
			},
			lookupTopic: consumer.Topic("other"),
			lookupGroup: "group-a",
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockQ := queuemock.NewMockQueue(ctrl)

			registry := consumer.NewTopicRegistry(
				[]consumer.TopicConfig{{Topic: consumer.TopicRequest, Queue: mockQ}},
				tt.configs,
			)
			config, ok := registry.SubscriptionConfig(tt.lookupTopic, tt.lookupGroup)

			if !tt.expectFound {
				assert.False(t, ok)
			} else {
				require.True(t, ok)
				assert.Equal(t, tt.expectedGroup, config.ConsumerGroup)
			}
		})
	}
}

func TestTopicRegistry_DuplicateConfig_LastWins(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockQ := queuemock.NewMockQueue(ctrl)

	config1 := extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a")
	config1.BatchSize = 10

	config2 := extqueue.DefaultSubscriptionConfig("request", "worker-1", "group-a")
	config2.BatchSize = 50

	registry := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Topic: consumer.TopicRequest, Queue: mockQ}},
		[]extqueue.SubscriptionConfig{config1, config2},
	)

	config, ok := registry.SubscriptionConfig(consumer.TopicRequest, "group-a")
	require.True(t, ok)
	assert.Equal(t, 50, config.BatchSize)
}

func TestTopicRegistry_Queue_PerTopic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockQ1 := queuemock.NewMockQueue(ctrl)
	mockQ2 := queuemock.NewMockQueue(ctrl)

	registry := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Topic: consumer.TopicRequest, Queue: mockQ1},
			{Topic: consumer.TopicToBatch, Queue: mockQ2},
		},
		nil,
	)

	q1, ok := registry.Queue(consumer.TopicRequest)
	require.True(t, ok)
	assert.Equal(t, mockQ1, q1)

	q2, ok := registry.Queue(consumer.TopicToBatch)
	require.True(t, ok)
	assert.Equal(t, mockQ2, q2)

	_, ok = registry.Queue(consumer.Topic("nonexistent"))
	assert.False(t, ok)
}

func TestTopic_String(t *testing.T) {
	tests := []struct {
		name     string
		topic    consumer.Topic
		expected string
	}{
		{
			name:     "predefined topic",
			topic:    consumer.TopicRequest,
			expected: "request",
		},
		{
			name:     "custom topic",
			topic:    consumer.Topic("custom"),
			expected: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.topic.String())
		})
	}
}
