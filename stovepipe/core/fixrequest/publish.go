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

// Package fixrequest publishes builds for repair submission.
package fixrequest

import (
	"context"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/publish"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
)

// Publish requires the opening pass's issue state and history to be durable.
// The notification ID must be stable across retries and distinct between passes.
func Publish(ctx context.Context, registry consumer.TopicRegistry, queueName, buildID, notificationID string) error {
	if queueName == "" || buildID == "" || notificationID == "" {
		return fmt.Errorf("repair request requires a queue, build ID, and notification ID")
	}

	payload, err := mq.Marshal(&mq.FixRequest{Id: buildID, QueueName: queueName})
	if err != nil {
		return fmt.Errorf("failed to serialize repair request: %w", err)
	}

	return publish.Message(ctx, registry, mq.TopicKeyFix, publish.MessageParams{
		Tenant:       queueName,
		ID:           publish.IntentID(buildID, notificationID),
		Payload:      payload,
		PartitionKey: buildID,
	})
}
