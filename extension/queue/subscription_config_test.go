package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionConfig_FieldsAreIndependent(t *testing.T) {
	// Create two configs and modify one to ensure they're independent
	config1 := DefaultSubscriptionConfig("worker-1", "consumer-1")
	config2 := DefaultSubscriptionConfig("worker-2", "consumer-2")

	// Modify config1
	config1.PollIntervalMs = 500
	config1.BatchSize = 100
	config1.Retry.MaxAttempts = 5
	config1.DLQ.TopicSuffix = "_failed"

	// Verify config2 is unaffected
	assert.Equal(t, "worker-2", config2.SubscriberName)
	assert.Equal(t, "consumer-2", config2.ConsumerGroup)
	assert.Equal(t, int64(100), config2.PollIntervalMs)
	assert.Equal(t, 10, config2.BatchSize)
	assert.Equal(t, 3, config2.Retry.MaxAttempts)
	assert.Equal(t, "_dlq", config2.DLQ.TopicSuffix)
}

func TestSubscriptionConfig_CustomValues(t *testing.T) {
	config := DefaultSubscriptionConfig("my-worker", "my-consumer")

	// Override with custom values (in milliseconds)
	config.PollIntervalMs = 200
	config.BatchSize = 50
	config.VisibilityTimeoutMs = 120000
	config.LeaseRenewalIntervalMs = 20000
	config.LeaseDurationMs = 60000
	config.Retry.MaxAttempts = 5
	config.Retry.InitialBackoffMs = 2000
	config.Retry.MaxBackoffMs = 60000
	config.Retry.BackoffMultiplier = 3.0
	config.DLQ.Enabled = false
	config.DLQ.TopicSuffix = "_dead"

	// Verify all custom values
	assert.Equal(t, "my-worker", config.SubscriberName)
	assert.Equal(t, "my-consumer", config.ConsumerGroup)
	assert.Equal(t, int64(200), config.PollIntervalMs)
	assert.Equal(t, 50, config.BatchSize)
	assert.Equal(t, int64(120000), config.VisibilityTimeoutMs)
	assert.Equal(t, int64(20000), config.LeaseRenewalIntervalMs)
	assert.Equal(t, int64(60000), config.LeaseDurationMs)
	assert.Equal(t, 5, config.Retry.MaxAttempts)
	assert.Equal(t, int64(2000), config.Retry.InitialBackoffMs)
	assert.Equal(t, int64(60000), config.Retry.MaxBackoffMs)
	assert.Equal(t, 3.0, config.Retry.BackoffMultiplier)
	assert.False(t, config.DLQ.Enabled)
	assert.Equal(t, "_dead", config.DLQ.TopicSuffix)
}

func TestSubscriptionConfig_DifferentConsumerGroups(t *testing.T) {
	// Test that different consumer groups get independent configs
	tests := []struct {
		subscriberName string
		consumerGroup  string
	}{
		{"worker-1", "group-A"},
		{"worker-1", "group-B"},
		{"worker-2", "group-A"},
	}

	for _, tt := range tests {
		t.Run(tt.subscriberName+"_"+tt.consumerGroup, func(t *testing.T) {
			config := DefaultSubscriptionConfig(tt.subscriberName, tt.consumerGroup)
			require.Equal(t, tt.subscriberName, config.SubscriberName)
			require.Equal(t, tt.consumerGroup, config.ConsumerGroup)
		})
	}
}
