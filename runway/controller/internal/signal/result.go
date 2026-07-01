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

// Package signal holds helpers for Runway result signal publication.
package signal

import (
	"context"
	"fmt"

	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
)

// PublishMergeResult publishes a Runway MergeResult to a signal topic.
func PublishMergeResult(ctx context.Context, registry consumer.TopicRegistry, topicKey consumer.TopicKey, result *runwaymq.MergeResult, partitionKey string) error {
	if result == nil {
		return fmt.Errorf("merge result is required")
	}

	payload, err := runwaymq.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to serialize merge result: %w", err)
	}

	q, ok := registry.Queue(topicKey)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topicKey)
	}

	topicName, ok := registry.TopicName(topicKey)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topicKey)
	}

	msg := entityqueue.NewMessage(result.Id, payload, partitionKey, nil)
	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
