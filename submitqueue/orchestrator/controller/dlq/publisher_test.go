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

package dlq

import (
	"context"

	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"go.uber.org/mock/gomock"
)

func newTestLogRegistry(
	t require.TestingT,
	ctrl *gomock.Controller,
	publishCount int,
	publishFn func(entity.RequestLog) error,
) consumer.TopicRegistry {
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, message entityqueue.Message) error {
			logEntry, err := entity.RequestLogFromBytes(message.Payload)
			require.NoError(t, err)
			return publishFn(logEntry)
		},
	).Times(publishCount)

	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).Times(publishCount)

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{{
		Key:   topickey.TopicKeyLog,
		Name:  "log",
		Queue: queue,
	}})
	require.NoError(t, err)
	return registry
}
