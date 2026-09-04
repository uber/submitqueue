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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/runway/controller/dlq"
)

func TestNewTopicRegistry_RetryBudgets(t *testing.T) {
	registry, err := newTopicRegistry(nil, "runway-test")
	require.NoError(t, err)

	tests := []struct {
		name          string
		topicKey      consumer.TopicKey
		consumerGroup string
		unlimited     bool
	}{
		{
			name:          "merge conflict check primary remains finite",
			topicKey:      runwaymq.TopicKeyMergeConflictCheck,
			consumerGroup: "runway-mergeconflictcheck",
		},
		{
			name:          "merge conflict check dlq is unlimited",
			topicKey:      dlq.TopicKey(runwaymq.TopicKeyMergeConflictCheck),
			consumerGroup: "runway-mergeconflictcheck-dlq",
			unlimited:     true,
		},
		{
			name:          "merge primary remains finite",
			topicKey:      runwaymq.TopicKeyMerge,
			consumerGroup: "runway-merge",
		},
		{
			name:          "merge dlq is unlimited",
			topicKey:      dlq.TopicKey(runwaymq.TopicKeyMerge),
			consumerGroup: "runway-merge-dlq",
			unlimited:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, found := registry.SubscriptionConfig(tt.topicKey, tt.consumerGroup)
			require.True(t, found)
			if tt.unlimited {
				assert.Zero(t, config.Retry.MaxAttempts)
				assert.False(t, config.DLQ.Enabled)
				return
			}
			assert.Positive(t, config.Retry.MaxAttempts)
			assert.True(t, config.DLQ.Enabled)
		})
	}
}
