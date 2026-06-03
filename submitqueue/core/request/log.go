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

	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// PublishLog publishes a single request log entry to the log topic for async persistence.
// The partitionKey ensures ordering of log entries for the same request; typically set to the request ID.
//
// The message ID is scoped to (requestID, status) so that the queue's
// (topic, partition_key, id) unique index dedupes retries of the same logical
// log event (same delivery re-processed) without rejecting distinct statuses
// for the same request (e.g. "started" emitted by the start controller and
// "cancelled" emitted later by the cancel controller).
func PublishLog(ctx context.Context, registry consumer.TopicRegistry, logEntry entity.RequestLog, partitionKey string) error {
	payload, err := logEntry.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize request log: %w", err)
	}

	msgID := fmt.Sprintf("%s/%s", logEntry.RequestID, logEntry.Status)
	msg := entityqueue.NewMessage(msgID, payload, partitionKey, nil)

	q, ok := registry.Queue(consumer.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no queue registered for topic key %s", consumer.TopicKeyLog)
	}

	topicName, ok := registry.TopicName(consumer.TopicKeyLog)
	if !ok {
		return fmt.Errorf("no topic name registered for topic key %s", consumer.TopicKeyLog)
	}

	if err := q.Publisher().Publish(ctx, topicName, msg); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// PublishBatchLogs publishes a request log entry for each request ID in the batch to the log topic.
// Each entry uses the request ID as the partition key to ensure per-request ordering.
func PublishBatchLogs(ctx context.Context, registry consumer.TopicRegistry, requestIDs []string, status entity.RequestStatus, metadata map[string]string) error {
	for _, requestID := range requestIDs {
		logEntry := entity.NewRequestLog(requestID, status, 0, "", metadata)
		if err := PublishLog(ctx, registry, logEntry, requestID); err != nil {
			return fmt.Errorf("failed to publish request log for request %s: %w", requestID, err)
		}
	}
	return nil
}
