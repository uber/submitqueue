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

package request

import (
	"context"
	"fmt"

	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// PublishLog publishes a single request log entry to the log topic for async persistence.
// The partitionKey ensures ordering of log entries for the same request; typically set to the request ID.
//
// The message ID is scoped to (requestID, status, occurrence) so that the queue's
// (topic, partition_key, id) unique index dedupes retries of the same logical
// log event (same delivery re-processed) without rejecting distinct statuses
// for the same request (e.g. "started" emitted by the start controller and
// "cancelled" emitted later by the cancel controller).
//
// occurrence is a stable discriminator naming this occurrence of the status.
// Empty means the status happens at most once per request, and the id collapses
// to (requestID, status) — every repeat is then dropped at publish time, which
// is what a terminal status wants. A status that legitimately recurs (a request
// builds again each time speculation re-plans) must pass something that is
// stable across redeliveries of one occurrence but differs between occurrences:
// the build ID, or the batch and path the entry is about. Deriving it from the
// wall clock or a random value would defeat the dedupe entirely.
func PublishLog(ctx context.Context, registry consumer.TopicRegistry, logEntry entity.RequestLog, partitionKey string, occurrence string) error {
	payload, err := logEntry.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request log: %w", err)
	}

	msgID := fmt.Sprintf("%s/%s", logEntry.RequestID, logEntry.Value())
	if occurrence != "" {
		msgID = fmt.Sprintf("%s/%s", msgID, occurrence)
	}
	msg := entityqueue.NewMessage(msgID, payload, partitionKey, nil)

	q, ok := registry.Queue(topickey.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", topickey.TopicKeyLog)
	}

	topicName, ok := registry.TopicName(topickey.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", topickey.TopicKeyLog)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// PublishBatchLogs publishes a status log entry for each request ID in the batch to the log topic.
// Each entry uses the request ID as the partition key to ensure per-request ordering.
// queue scopes every entry: a request ID is only unique within its own queue.
// occurrence names this occurrence of the status and is shared by every entry in
// the fan-out, so the whole batch dedupes together on a redelivery; see PublishLog.
//
// Entries carry no request version: a status published for a whole batch reports
// something that happened to the batch, not a transition of any one request's
// state machine.
func PublishBatchLogs(ctx context.Context, registry consumer.TopicRegistry, queue string, requestIDs []string, status entity.RequestStatus, occurrence string, metadata map[string]string) error {
	for _, requestID := range requestIDs {
		logEntry := entity.NewRequestStatusLog(queue, requestID, status, 0, "", metadata)
		if err := PublishLog(ctx, registry, logEntry, requestID, occurrence); err != nil {
			return fmt.Errorf("failed to publish request log for request %s: %w", requestID, err)
		}
	}
	return nil
}

// PublishBatchEvents publishes an event log entry for each request ID in the batch
// to the log topic. It is the event counterpart of PublishBatchLogs and shares its
// partitioning and occurrence semantics.
//
// Separate rather than a flag on PublishBatchLogs because the two carry different
// vocabularies: a caller reporting build progress cannot reach for a status, and a
// caller reporting a status cannot accidentally publish something the summary will
// refuse to project.
func PublishBatchEvents(ctx context.Context, registry consumer.TopicRegistry, queue string, requestIDs []string, event entity.RequestEvent, occurrence string, metadata map[string]string) error {
	for _, requestID := range requestIDs {
		logEntry := entity.NewRequestEventLog(queue, requestID, event, metadata)
		if err := PublishLog(ctx, registry, logEntry, requestID, occurrence); err != nil {
			return fmt.Errorf("failed to publish request event for request %s: %w", requestID, err)
		}
	}
	return nil
}
