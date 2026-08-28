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

package hook

import (
	"context"
	"fmt"

	basehook "github.com/uber/submitqueue/api/base/hook"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/publish"
)

// Publish sends one hook event to the domain's hook topic, partitioned by
// partitionKey. The topic key is not a parameter: a domain runs a single hook
// topic, and the caller's registry is what binds that key to a wire topic.
//
// The event id is the message id, so a redelivery republishing the same event
// dedups into the original message instead of enqueuing a second one. Callers
// pass the partition key their own topic partitions on, carrying that ordering
// across the seam.
//
// Errors are returned unclassified, leaving retryability to the caller's
// classifier: a malformed event is the caller's bug, not a transient fault.
func Publish(
	ctx context.Context,
	registry consumer.TopicRegistry,
	event *basehook.HookEvent,
	partitionKey string,
) error {
	if err := basehook.Validate(event); err != nil {
		return fmt.Errorf("refusing to publish a malformed hook event: %w", err)
	}

	body, err := basehook.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize hook event %s: %w", event.GetId(), err)
	}

	if err := publish.Message(
		ctx, registry, basehook.TopicKeyHook, publish.IntentID(event.GetId()), body, partitionKey,
	); err != nil {
		return fmt.Errorf("failed to publish hook event %s: %w", event.GetId(), err)
	}
	return nil
}
