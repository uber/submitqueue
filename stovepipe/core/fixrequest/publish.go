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

// Publish requires the build and its open issues to be persisted first.
// Replays after publication errors must call Publish again.
func Publish(ctx context.Context, registry consumer.TopicRegistry, queueName, buildID string) error {
	if queueName == "" || buildID == "" {
		return fmt.Errorf("repair request requires a queue and build ID")
	}

	payload, err := mq.Marshal(&mq.FixRequest{Id: buildID, QueueName: queueName})
	if err != nil {
		return fmt.Errorf("failed to serialize repair request: %w", err)
	}

	// Repair submission is a one-shot handoff for this build.
	return publish.Message(ctx, registry, mq.TopicKeyFix, publish.MessageParams{
		Tenant:       queueName,
		ID:           publish.IntentID(buildID),
		Payload:      payload,
		PartitionKey: buildID,
	})
}
