package sql

import (
	"fmt"
	"time"
)

// Config holds configuration for SQL-based queue.
// DB connection, logger, and metrics are passed separately to NewQueue.
type Config struct {
	// ConsumerGroup identifies this consumer for offset tracking (required)
	ConsumerGroup string

	// WorkerID uniquely identifies this worker instance (required for partition leases)
	// Example: hostname, pod name, UUID, etc.
	WorkerID string

	// PollInterval is how often to poll for new messages
	PollInterval time.Duration

	// BatchSize is the number of messages to fetch per poll
	BatchSize int

	// VisibilityTimeout is how long a message is invisible after being fetched
	// If worker crashes or gets stuck, message becomes visible again after this duration
	VisibilityTimeout time.Duration

	// LeaseRenewalInterval is how often to renew partition leases
	LeaseRenewalInterval time.Duration

	// LeaseDuration is how long a lease is valid without renewal
	// Stale leases (not renewed within this duration) can be stolen by other workers
	LeaseDuration time.Duration

	// Retry configuration for message retry
	Retry RetryConfig

	// DLQ configuration
	DLQ DLQConfig
}

// RetryConfig configures message retry behavior
type RetryConfig struct {
	// MaxAttempts is the maximum number of processing attempts
	// After this many retries, message is moved to DLQ (if enabled)
	// This includes both visibility timeout retries and explicit Nack retries
	MaxAttempts int

	// InitialBackoff is the initial backoff duration for explicit Nack retries
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration
	MaxBackoff time.Duration

	// BackoffMultiplier is the multiplier for exponential backoff
	BackoffMultiplier float64
}

// DLQConfig configures dead letter queue
type DLQConfig struct {
	// Enabled enables dead letter queue
	Enabled bool
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(consumerGroup, workerID string) Config {
	return Config{
		ConsumerGroup:        consumerGroup,
		WorkerID:             workerID,
		PollInterval:         100 * time.Millisecond,
		BatchSize:            10,
		VisibilityTimeout:    60 * time.Second,
		LeaseRenewalInterval: 10 * time.Second,
		LeaseDuration:        30 * time.Second,
		Retry: RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    1 * time.Second,
			MaxBackoff:        30 * time.Second,
			BackoffMultiplier: 2.0,
		},
		DLQ: DLQConfig{
			Enabled: true,
		},
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.ConsumerGroup == "" {
		return fmt.Errorf("ConsumerGroup is required")
	}
	if c.WorkerID == "" {
		return fmt.Errorf("WorkerID is required")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("PollInterval must be positive")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("BatchSize must be positive")
	}
	if c.VisibilityTimeout <= 0 {
		return fmt.Errorf("VisibilityTimeout must be positive")
	}
	if c.LeaseRenewalInterval <= 0 {
		return fmt.Errorf("LeaseRenewalInterval must be positive")
	}
	if c.LeaseDuration <= 0 {
		return fmt.Errorf("LeaseDuration must be positive")
	}
	if c.LeaseRenewalInterval >= c.LeaseDuration {
		return fmt.Errorf("LeaseRenewalInterval must be less than LeaseDuration")
	}
	if c.Retry.MaxAttempts < 1 {
		return fmt.Errorf("Retry.MaxAttempts must be at least 1")
	}
	if c.Retry.InitialBackoff <= 0 {
		return fmt.Errorf("Retry.InitialBackoff must be positive")
	}
	if c.Retry.MaxBackoff <= 0 {
		return fmt.Errorf("Retry.MaxBackoff must be positive")
	}
	if c.Retry.BackoffMultiplier < 1.0 {
		return fmt.Errorf("Retry.BackoffMultiplier must be >= 1.0")
	}
	return nil
}
