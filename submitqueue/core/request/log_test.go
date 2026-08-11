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

	logEntry := entity.NewRequestStatusLog("req", "req/1", entity.RequestStatusStarted, 1, "", nil)
	err := PublishLog(context.Background(), registry, logEntry, "req/1", "")
	require.NoError(t, err)
}

func TestPublishLog_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, fmt.Errorf("connection refused"))

	logEntry := entity.NewRequestStatusLog("req", "req/1", entity.RequestStatusStarted, 1, "", nil)
	err := PublishLog(context.Background(), registry, logEntry, "req/1", "")
	require.Error(t, err)
}

func TestPublishBatchLogs_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, nil)

	err := PublishBatchLogs(context.Background(), registry, "req", []string{"req/1", "req/2", "req/3"},
		entity.RequestStatusBatched,
		"",
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

	err = PublishBatchLogs(context.Background(), registry, "req", []string{"req/1", "req/2", "req/3"},
		entity.RequestStatusBatched,
		"",
		map[string]string{"batch_id": "b/1"},
	)
	require.Error(t, err)
}

func TestPublishBatchLogs_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newTestRegistry(t, ctrl, nil)

	err := PublishBatchLogs(context.Background(), registry, "req", nil, entity.RequestStatusBatched, "", nil)
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
			entity.NewRequestStatusLog("req", "req/1", st, 0, "", nil), "req/1", ""))
	}
	// Re-emit "started" to simulate a retry of the same delivery — must reuse the same ID.
	require.NoError(t, PublishLog(context.Background(), registry,
		entity.NewRequestStatusLog("req", "req/1", entity.RequestStatusStarted, 0, "", nil), "req/1", ""))

	require.Equal(t, []string{
		"req/1/started",
		"req/1/cancelling",
		"req/1/cancelled",
		"req/1/started",
	}, ids)
}

// TestPublishLog_MessageIDScopedByOccurrence locks in the recurring half of the
// id scheme. An entry that is recorded more than once per request (a request
// builds again every time speculation re-plans) is told apart by its occurrence,
// so each occurrence reaches the log while a redelivery of any one of them still
// dedupes. Without this, the second occurrence is silently swallowed by the
// queue's (topic, partition_key, id) index and a rebuilding request reads
// identically to one that built cleanly first try.
//
// The id is built from what the entry recorded, whichever kind that is, so the
// table carries whole entries rather than statuses.
func TestPublishLog_MessageIDScopedByOccurrence(t *testing.T) {
	tests := []struct {
		name       string
		entry      entity.RequestLog
		occurrence string
		wantID     string
	}{
		{
			name:       "empty occurrence collapses to request and status",
			entry:      entity.NewRequestStatusLog("req", "req/1", entity.RequestStatusStarted, 0, "", nil),
			occurrence: "",
			wantID:     "req/1/started",
		},
		{
			name:       "build id separates one build from the next",
			entry:      entity.NewRequestEventLog("req", "req/1", entity.RequestEventBuilding, nil),
			occurrence: "build/7",
			wantID:     "req/1/building/build/7",
		},
		{
			name:       "batch and path separate one passed path from the next",
			entry:      entity.NewRequestStatusLog("req", "req/1", entity.RequestStatusSpeculated, 0, "", nil),
			occurrence: "batch/2/path/a",
			wantID:     "req/1/speculated/batch/2/path/a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			logEntry := tt.entry
			// Twice: a redelivery of one occurrence must reuse its ID so the
			// queue dedupes it.
			require.NoError(t, PublishLog(context.Background(), registry, logEntry, "req/1", tt.occurrence))
			require.NoError(t, PublishLog(context.Background(), registry, logEntry, "req/1", tt.occurrence))

			require.Equal(t, []string{tt.wantID, tt.wantID}, ids)
		})
	}
}

// TestPublishBatchLogs_SharesOccurrenceAcrossFanout checks that every entry of
// one fan-out carries the same occurrence, so the whole batch dedupes together
// when the publishing delivery is retried, while the per-request partition keys
// keep the entries distinct.
func TestPublishBatchLogs_SharesOccurrenceAcrossFanout(t *testing.T) {
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

	require.NoError(t, PublishBatchLogs(context.Background(), registry, "req",
		[]string{"req/1", "req/2"}, entity.RequestStatusSpeculating, "batch/9", nil))

	require.Equal(t, []string{
		"req/1/speculating/batch/9",
		"req/2/speculating/batch/9",
	}, ids)
}
