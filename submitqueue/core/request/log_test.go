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
	"testing"

	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/mock/gomock"
)

func newTestRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error) consumer.TopicRegistry {
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ}},
	)
	require.NoError(t, err)
	return registry
}

func TestPublishLog_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, nil)

	logEntry := entity.NewRequestLog("req/1", entity.RequestStatusStarted, 1, "", nil)
	err := PublishLog(context.Background(), registry, logEntry, "req/1")
	require.NoError(t, err)
}

func TestPublishLog_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, fmt.Errorf("connection refused"))

	logEntry := entity.NewRequestLog("req/1", entity.RequestStatusStarted, 1, "", nil)
	err := PublishLog(context.Background(), registry, logEntry, "req/1")
	require.Error(t, err)
}

func TestPublishBatchLogs_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, nil)

	err := PublishBatchLogs(context.Background(), registry,
		[]string{"req/1", "req/2", "req/3"},
		entity.RequestStatusScored,
		map[string]string{"batch_id": "b/1"},
	)
	require.NoError(t, err)
}

func TestPublishBatchLogs_PartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	callCount := 0
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			callCount++
			if callCount == 2 {
				return fmt.Errorf("publish failed")
			}
			return nil
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ}},
	)
	require.NoError(t, err)

	err = PublishBatchLogs(context.Background(), registry,
		[]string{"req/1", "req/2", "req/3"},
		entity.RequestStatusScored,
		map[string]string{"batch_id": "b/1"},
	)
	require.Error(t, err)
}

func TestPublishBatchLogs_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, nil)

	err := PublishBatchLogs(context.Background(), registry, nil, entity.RequestStatusScored, nil)
	require.NoError(t, err)
}

// TestPublishLog_MessageIDScopedByStatus locks in the queue-id scheme:
// distinct statuses for the same request must produce distinct message IDs so
// the queue's (topic, partition_key, id) uniqueness check does not reject the
// second publish, while the same status emitted twice (retry of the same
// delivery) must produce the same message ID so the queue dedupes it.
//
// Regression test for the duplicate-key crash where the orchestrator cancel
// controller could not emit a `cancelled` log entry because the start
// controller had already published `started` for the same request.
func TestPublishLog_MessageIDScopedByStatus(t *testing.T) {
	ctrl := gomock.NewController(t)

	var ids []string
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			ids = append(ids, msg.ID)
			return nil
		},
	).AnyTimes()
	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyLog, Name: "log", Queue: mockQ}},
	)
	require.NoError(t, err)

	// Three distinct statuses for the same request.
	for _, st := range []entity.RequestStatus{
		entity.RequestStatusStarted,
		entity.RequestStatusCancelling,
		entity.RequestStatusCancelled,
	} {
		require.NoError(t, PublishLog(context.Background(), registry,
			entity.NewRequestLog("req/1", st, 0, "", nil), "req/1"))
	}
	// Re-emit "started" to simulate a retry of the same delivery — must reuse the same ID.
	require.NoError(t, PublishLog(context.Background(), registry,
		entity.NewRequestLog("req/1", entity.RequestStatusStarted, 0, "", nil), "req/1"))

	require.Equal(t, []string{
		"req/1/started",
		"req/1/cancelling",
		"req/1/cancelled",
		"req/1/started",
	}, ids)
}
