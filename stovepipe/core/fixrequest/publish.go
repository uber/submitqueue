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

// Package fixrequest publishes persisted issue openings for repair selection.
package fixrequest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/publish"
	mq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// Publish requires a durable observation and open issue. Replaying a duplicate
// observation must call Publish again, including after a prior publication error.
func Publish(ctx context.Context, registry consumer.TopicRegistry, queue, buildID string, issue entity.OpenIssue) error {
	if queue == "" || buildID == "" || issue.TaskID == "" || issue.FirstSeenBuildID == "" {
		return fmt.Errorf("repair request requires queue, build, task, and opening build")
	}
	payload, err := mq.Marshal(&mq.FixRequest{QueueName: queue, BuildId: buildID, TaskId: issue.TaskID, UnitId: issue.UnitID, FirstSeenBuildId: issue.FirstSeenBuildID})
	if err != nil {
		return err
	}
	// JSON preserves opaque identifiers without delimiter collisions.
	identity, err := json.Marshal([4]string{buildID, issue.TaskID, issue.UnitID, issue.FirstSeenBuildID})
	if err != nil {
		return err
	}
	return publish.Message(ctx, registry, mq.TopicKeyFix, publish.MessageParams{Tenant: queue, ID: publish.IntentID(fmt.Sprintf("%x", sha256.Sum256(identity))), Payload: payload, PartitionKey: buildID})
}
