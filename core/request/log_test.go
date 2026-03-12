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
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"go.uber.org/mock/gomock"
)

func newTestRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error) consumer.TopicRegistry {
	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ}},
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
		func(ctx context.Context, topic string, msg queue.Message) error {
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
		[]consumer.TopicConfig{{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ}},
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
