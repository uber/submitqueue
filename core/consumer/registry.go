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

package consumer

import (
	"fmt"

	"github.com/uber/submitqueue/extension/queue"
)

// TopicKey identifies a pipeline stage. It is a fixed key used to
// look up queue backends, topic names, and subscription configs
// in the TopicRegistry.  The actual queue topic name is provided
// separately via TopicConfig.Name so that library consumers can
// choose their own naming conventions.
type TopicKey string

const (
	// TopicKeyRequest is the pipeline stage where new requests arrive from the gateway.
	TopicKeyRequest TopicKey = "request"
	// TopicKeyValidate is the pipeline stage where requests are published for validation.
	TopicKeyValidate TopicKey = "validate"
	// TopicKeyBatch is the pipeline stage where validated requests are published for batching.
	TopicKeyBatch TopicKey = "batch"
	// TopicKeyScore is the pipeline stage where batches are published for scoring.
	TopicKeyScore TopicKey = "score"
	// TopicKeySpeculate is the pipeline stage where scored batches are published for speculation.
	TopicKeySpeculate TopicKey = "speculate"
	// TopicKeyBuild is the pipeline stage where speculated batches are published for builds.
	TopicKeyBuild TopicKey = "build"
	// TopicKeyBuildSignal is the pipeline stage where builds are published for build signal processing.
	TopicKeyBuildSignal TopicKey = "buildsignal"
	// TopicKeyMerge is the pipeline stage where speculated batches are published for merging.
	TopicKeyMerge TopicKey = "merge"
	// TopicKeyConclude is the pipeline stage where merged requests are published for conclusion.
	TopicKeyConclude TopicKey = "conclude"
	// TopicKeyLog is the pipeline stage where per-request logs are written.
	TopicKeyLog TopicKey = "log"
)

// String returns the topic key as a string.
func (t TopicKey) String() string {
	return string(t)
}

// TopicConfig combines all configuration for a single pipeline topic:
// the fixed key, the actual queue topic name, the queue backend, and
// (optionally) subscription settings.
type TopicConfig struct {
	// Key is the fixed pipeline stage identifier.
	Key TopicKey
	// Name is the actual queue topic name (e.g. "request", "my-custom-request").
	Name string
	// Queue is the queue backend for this topic.
	Queue queue.Queue
	// Subscription is the subscription configuration for this topic.
	// Leave at zero value for publish-only topics.
	Subscription queue.SubscriptionConfig
}

// TopicRegistry provides queue, topic name, and subscription config for topics.
// Each topic can have a different queue backend and topic name.
type TopicRegistry struct {
	queues              map[TopicKey]queue.Queue
	topicNames          map[TopicKey]string
	subscriptionConfigs map[topicGroup]queue.SubscriptionConfig
}

// topicGroup identifies a topic key and consumer group pair.
type topicGroup struct {
	topicKey      TopicKey
	consumerGroup string
}

// NewTopicRegistry creates a new TopicRegistry from a list of TopicConfigs.
// Returns an error if any topic name is invalid.
func NewTopicRegistry(configs []TopicConfig) (TopicRegistry, error) {
	queues := make(map[TopicKey]queue.Queue, len(configs))
	topicNames := make(map[TopicKey]string, len(configs))
	subConfigs := make(map[topicGroup]queue.SubscriptionConfig)

	for _, cfg := range configs {
		if err := ValidateTopicName(cfg.Name); err != nil {
			return TopicRegistry{}, fmt.Errorf("invalid topic name for key %s: %w", cfg.Key, err)
		}

		queues[cfg.Key] = cfg.Queue
		topicNames[cfg.Key] = cfg.Name

		// Register subscription config if a consumer group is set.
		if cfg.Subscription.ConsumerGroup != "" {
			sub := cfg.Subscription
			key := topicGroup{
				topicKey:      cfg.Key,
				consumerGroup: sub.ConsumerGroup,
			}
			subConfigs[key] = sub
		}
	}

	return TopicRegistry{
		queues:              queues,
		topicNames:          topicNames,
		subscriptionConfigs: subConfigs,
	}, nil
}

// ValidateTopicName ensures a topic name is valid.
// Topic names must be non-empty, at most 255 characters, and contain only
// lowercase letters, numbers, underscores, and hyphens.
func ValidateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if len(name) > 255 {
		return fmt.Errorf("topic name too long (max 255 characters)")
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return fmt.Errorf("topic name must contain only lowercase letters, numbers, underscores, and hyphens")
		}
	}
	return nil
}

// Queue returns the queue backend for the given topic key.
// Returns ok=false if no queue is registered for this key.
func (r TopicRegistry) Queue(key TopicKey) (queue.Queue, bool) {
	q, ok := r.queues[key]
	return q, ok
}

// TopicName returns the actual queue topic name for the given key.
// Returns ok=false if no topic is registered for this key.
func (r TopicRegistry) TopicName(key TopicKey) (string, bool) {
	name, ok := r.topicNames[key]
	return name, ok
}

// SubscriptionConfig returns the subscription configuration for the given
// topic key and consumer group.
// Returns ok=false if no configuration is registered.
func (r TopicRegistry) SubscriptionConfig(key TopicKey, consumerGroup string) (queue.SubscriptionConfig, bool) {
	cfg, ok := r.subscriptionConfigs[topicGroup{topicKey: key, consumerGroup: consumerGroup}]
	return cfg, ok
}
