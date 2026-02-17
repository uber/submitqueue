package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("test-consumer", "test-worker")

	assert.Equal(t, "test-consumer", cfg.ConsumerGroup)
	assert.Equal(t, "test-worker", cfg.WorkerID)
	assert.Equal(t, 100*time.Millisecond, cfg.PollInterval)
	assert.Equal(t, 10, cfg.BatchSize)
	assert.Equal(t, 60*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 10*time.Second, cfg.LeaseRenewalInterval)
	assert.Equal(t, 30*time.Second, cfg.LeaseDuration)
	assert.True(t, cfg.DLQ.Enabled)
	assert.Equal(t, 3, cfg.Retry.MaxAttempts)
	assert.Equal(t, 1*time.Second, cfg.Retry.InitialBackoff)
	assert.Equal(t, 30*time.Second, cfg.Retry.MaxBackoff)
	assert.Equal(t, 2.0, cfg.Retry.BackoffMultiplier)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid config",
			config:      DefaultConfig("test-consumer", "test-worker"),
			expectError: false,
		},
		{
			name: "empty consumer group",
			config: Config{
				ConsumerGroup:        "",
				WorkerID:             "test-worker",
				PollInterval:         100 * time.Millisecond,
				BatchSize:            10,
				VisibilityTimeout:    60 * time.Second,
				LeaseRenewalInterval: 10 * time.Second,
				LeaseDuration:        30 * time.Second,
				Retry:                DefaultConfig("dummy", "dummy").Retry,
			},
			expectError: true,
			errorMsg:    "ConsumerGroup is required",
		},
		{
			name: "empty worker ID",
			config: Config{
				ConsumerGroup:        "test",
				WorkerID:             "",
				PollInterval:         100 * time.Millisecond,
				BatchSize:            10,
				VisibilityTimeout:    60 * time.Second,
				LeaseRenewalInterval: 10 * time.Second,
				LeaseDuration:        30 * time.Second,
				Retry:                DefaultConfig("dummy", "dummy").Retry,
			},
			expectError: true,
			errorMsg:    "WorkerID is required",
		},
		{
			name: "invalid poll interval",
			config: Config{
				ConsumerGroup:        "test",
				WorkerID:             "test-worker",
				PollInterval:         0,
				BatchSize:            10,
				VisibilityTimeout:    60 * time.Second,
				LeaseRenewalInterval: 10 * time.Second,
				LeaseDuration:        30 * time.Second,
				Retry:                DefaultConfig("dummy", "dummy").Retry,
			},
			expectError: true,
			errorMsg:    "PollInterval must be positive",
		},
		{
			name: "invalid batch size",
			config: Config{
				ConsumerGroup:        "test",
				WorkerID:             "test-worker",
				PollInterval:         100 * time.Millisecond,
				BatchSize:            0,
				VisibilityTimeout:    60 * time.Second,
				LeaseRenewalInterval: 10 * time.Second,
				LeaseDuration:        30 * time.Second,
				Retry:                DefaultConfig("dummy", "dummy").Retry,
			},
			expectError: true,
			errorMsg:    "BatchSize must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
