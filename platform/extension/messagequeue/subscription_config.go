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

package messagequeue

// SubscriptionConfig holds per-subscription configuration.
// Each subscription (topic) can have its own settings for polling,
// batching, retries, and dead letter queue behavior.
type SubscriptionConfig struct {
	// SubscriberName uniquely identifies this subscriber instance for partition leases.
	// Different workers should use different names (e.g., hostname, pod name, UUID).
	// Combined with ConsumerGroup, this determines which worker owns a partition lease.
	SubscriberName string

	// ConsumerGroup identifies this consumer for offset tracking.
	// Different consumer groups maintain independent offsets.
	ConsumerGroup string

	// PollIntervalMs is how often to poll for new messages (in milliseconds).
	PollIntervalMs int64

	// BatchSize is the maximum number of messages to fetch per poll.
	BatchSize int

	// VisibilityTimeoutMs is how long a message is invisible after being fetched (in milliseconds).
	// If the worker crashes or doesn't ack/nack in time, the message becomes
	// visible again after this duration.
	VisibilityTimeoutMs int64

	// LeaseRenewalIntervalMs is how often to renew partition leases (in milliseconds).
	LeaseRenewalIntervalMs int64

	// LeaseDurationMs is how long a lease is valid without renewal (in milliseconds).
	// Stale leases (not renewed within this duration) can be stolen by other workers.
	LeaseDurationMs int64

	// Retry configures message retry behavior.
	Retry RetryConfig

	// DLQ configures dead letter queue behavior.
	DLQ DLQConfig
}

// RetryConfig configures message retry behavior.
type RetryConfig struct {
	// MaxAttempts is the maximum number of processing attempts.
	// After this many attempts, the message is moved to DLQ (if enabled).
	MaxAttempts int

	// InitialBackoffMs is the initial backoff duration for retries (in milliseconds).
	InitialBackoffMs int64

	// MaxBackoffMs is the maximum backoff duration (in milliseconds).
	MaxBackoffMs int64

	// BackoffMultiplier is the multiplier for exponential backoff.
	BackoffMultiplier float64
}

// DLQConfig configures dead letter queue behavior.
type DLQConfig struct {
	// Enabled enables dead letter queue.
	Enabled bool

	// TopicSuffix is appended to the original topic name to create the DLQ topic.
	// For example, if original topic is "orders" and suffix is "_dlq", DLQ topic will be "orders_dlq".
	TopicSuffix string
}

// DefaultSubscriptionConfig returns a SubscriptionConfig with sensible defaults.
func DefaultSubscriptionConfig(subscriberName, consumerGroup string) SubscriptionConfig {
	return SubscriptionConfig{
		SubscriberName:         subscriberName,
		ConsumerGroup:          consumerGroup,
		PollIntervalMs:         100, // 100ms
		BatchSize:              10,
		VisibilityTimeoutMs:    60000, // 60s
		LeaseRenewalIntervalMs: 10000, // 10s
		LeaseDurationMs:        30000, // 30s
		Retry: RetryConfig{
			MaxAttempts:       3,
			InitialBackoffMs:  1000,  // 1s
			MaxBackoffMs:      30000, // 30s
			BackoffMultiplier: 2.0,
		},
		DLQ: DLQConfig{
			Enabled:     true,
			TopicSuffix: "_dlq",
		},
	}
}
