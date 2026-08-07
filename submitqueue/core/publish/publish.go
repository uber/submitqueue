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

// Package publish sends a message to the queue behind a topic key. It owns the
// lookup-and-send plumbing every orchestrator stage otherwise repeats — resolve
// the key to a queue and a topic name, wrap the payload in a message, publish —
// and the message-ID convention that controls deduplication (see UniqueID).
package publish

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
)

// Message publishes payload to the topic registered for key.
//
// msgID selects the dedup behavior, so the caller must choose it deliberately.
// The queue deduplicates on (topic, partition key, message ID) against every
// row it has not garbage-collected yet, consumed ones included:
//
//   - A stable msgID (an entity's own ID) makes a repeat publish a silent
//     no-op. Right for a hand-off that must happen at most once per entity.
//   - UniqueID(id) makes every publish distinct. Right for signals that are
//     re-sent by design — wake-ups, polls, re-dispatches — where a swallowed
//     repeat would stall the pipeline.
func Message(ctx context.Context, registry consumer.TopicRegistry, key consumer.TopicKey, msgID string, payload []byte, partitionKey string) error {
	q, ok := registry.Queue(key)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", key)
	}
	topicName, ok := registry.TopicName(key)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", key)
	}

	msg := entityqueue.NewMessage(msgID, payload, partitionKey, nil)
	return q.Publisher().Publish(ctx, topicName, msg)
}

// sequence breaks ties between UniqueID calls that land on the same clock
// tick: some platforms quantize time.Now coarsely enough for consecutive calls
// to read the same nanosecond.
var sequence atomic.Uint64

// UniqueID returns a message ID no earlier publish for the same entity has
// used, so the queue's (topic, partition key, message ID) dedup never swallows
// the repeat. Use it for every publish that is re-sent by design; reusing the
// bare entity ID instead would make the second publish a silent no-op.
func UniqueID(id string) string {
	return fmt.Sprintf("%s@%d-%d", id, time.Now().UnixNano(), sequence.Add(1))
}
