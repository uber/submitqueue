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
// lookup-and-send plumbing every pipeline stage otherwise repeats — resolve the
// key to a queue and a topic name, wrap the payload in a message, publish — and
// the message-ID convention that controls deduplication (see IntentID).
//
// Every producer publishes through this package. Building a message anywhere
// else would put the ID choice back at each call site, which is the mistake the
// convention exists to prevent, so a linter restricts message construction to
// here and to the queue backends.
package publish

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
)

// Message publishes payload to the topic registered for key.
//
// msgID selects the dedup behavior, so the caller must choose it deliberately.
// The queue deduplicates on (topic, partition key, message ID) against every
// row it has not garbage-collected yet, consumed ones included — a window with
// no upper bound on a busy partition. A publish that collides is reported as a
// success and writes nothing, and nothing retries it.
//
// Build msgID with IntentID: name the entity the message is about and the cause
// this particular message exists for. A retry of the same cause then dedups,
// which is what makes redelivery safe, while a new cause about the same entity
// can never be swallowed by an older row.
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

// IntentID names the occasion to publish rather than the entity published
// about: entityID says what the message concerns, and cause says why this
// particular message exists.
//
// Passing no cause asks for at-most-once delivery per entity — every later
// publish about that entity is dropped while an earlier row survives. That is
// right only for a hand-off that happens once in an entity's life, such as
// announcing that it was created. Anything re-sent by design — a wake-up, a
// poll, a re-dispatch, a dead-letter reconciliation — must name its cause, or
// it collides with that one-shot publish and is lost.
//
// Each cause segment must be stable across redeliveries of one occurrence and
// different between occurrences, so derive it from whatever provoked the
// publish: the dependency that reached a terminal state, the build and the
// status observed, the dead letter being reconciled. A wall-clock reading or a
// random value satisfies "different" while destroying "stable", leaving every
// redelivery to publish again. An empty segment carries no information and
// makes two different occasions share an ID, so callers pass none.
func IntentID(entityID string, cause ...string) string {
	if len(cause) == 0 {
		return entityID
	}
	return entityID + "/" + strings.Join(cause, "/")
}

// sequence breaks ties between UniqueID calls that land on the same clock
// tick: some platforms quantize time.Now coarsely enough for consecutive calls
// to read the same nanosecond.
var sequence atomic.Uint64

// UniqueID returns a message ID no earlier publish has used, so the publish
// cannot be deduplicated away.
//
// This is the fallback for a cause with nothing stable to name it by, and it
// costs the idempotency IntentID preserves: a redelivery mints a fresh ID and
// publishes a second time, so the consumer has to absorb the duplicate. Prefer
// IntentID wherever the cause can be identified.
func UniqueID(id string) string {
	return fmt.Sprintf("%s@%d-%d", id, time.Now().UnixNano(), sequence.Add(1))
}
