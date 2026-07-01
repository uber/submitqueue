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

package merge

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/runway/extension/merger"
	mergermock "github.com/uber/submitqueue/runway/extension/merger/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testID           = "test-queue/1"
	testQueue        = "test-queue"
	testPartitionKey = "test-queue"
)

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, *mergermock.MockMerger, *queuemock.MockQueue, *queuemock.MockPublisher) {
	t.Helper()
	q := queuemock.NewMockQueue(ctrl)
	publisher := queuemock.NewMockPublisher(ctrl)
	mockMerger := mergermock.NewMockMerger(ctrl)
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyMergeSignal, Name: "merge-signal", Queue: q},
	})
	require.NoError(t, err)

	return NewController(Params{
		Logger:         zaptest.NewLogger(t).Sugar(),
		Scope:          tally.NoopScope,
		TopicKey:       runwaymq.TopicKeyMerge,
		SignalTopicKey: runwaymq.TopicKeyMergeSignal,
		ConsumerGroup:  "runway-merge",
		Merger:         mockMerger,
		Registry:       registry,
	}), mockMerger, q, publisher
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *queuemock.MockDelivery {
	t.Helper()
	msg := entityqueue.NewMessage(testID, payload, testPartitionKey, nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func requestPayload(t *testing.T, req runwaymq.MergeRequest) []byte {
	t.Helper()
	payload, err := runwaymq.Marshal(&req)
	require.NoError(t, err)
	return payload
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, _, _, _ := newController(t, ctrl)
	require.NotNil(t, controller)
	assert.Equal(t, runwaymq.TopicKeyMerge, controller.TopicKey())
	assert.Equal(t, "runway-merge", controller.ConsumerGroup())
	assert.Equal(t, "merge", controller.Name())
}

func TestProcess_PublishesMergeSignal(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, mockMerger, q, publisher := newController(t, ctrl)

	req := runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	mockMerger.EXPECT().
		Merge(gomock.Any(), gomock.Any()).
		Return(&runwaymq.MergeResult{
			Id:      testID,
			Outcome: runwaypb.Outcome_SUCCEEDED,
			Steps: []*runwaymq.StepResult{{
				StepId:  "step-1",
				Outputs: []*runwaymq.StepOutput{{Id: "fake-output"}},
			}},
		}, nil)
	expectPublish(t, q, publisher, "merge-signal", func(result *runwaymq.MergeResult) {
		assert.Equal(t, testID, result.Id)
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.Outcome)
		require.Len(t, result.Steps, 1)
		assert.Equal(t, "step-1", result.Steps[0].StepId)
		require.Len(t, result.Steps[0].Outputs, 1)
		assert.Equal(t, "fake-output", result.Steps[0].Outputs[0].Id)
	}, nil)

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestProcess_PublishesFailedSignalOnConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, mockMerger, q, publisher := newController(t, ctrl)

	req := runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	mockMerger.EXPECT().Merge(gomock.Any(), gomock.Any()).Return(nil, merger.ErrConflict)
	expectPublish(t, q, publisher, "merge-signal", func(result *runwaymq.MergeResult) {
		assert.Equal(t, testID, result.Id)
		assert.Equal(t, runwaypb.Outcome_FAILED, result.Outcome)
		assert.Equal(t, merger.ErrConflict.Error(), result.Reason)
	}, nil)

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestProcess_ReturnsErrorOnMergeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, mockMerger, _, _ := newController(t, ctrl)

	req := runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	mockMerger.EXPECT().Merge(gomock.Any(), gomock.Any()).Return(nil, errors.New("merge backend down"))

	require.Error(t, controller.Process(context.Background(), delivery))
}

func TestProcess_ReturnsErrorOnPublishError(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, mockMerger, q, publisher := newController(t, ctrl)

	req := runwaymq.MergeRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.MergeStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	mockMerger.EXPECT().
		Merge(gomock.Any(), gomock.Any()).
		Return(&runwaymq.MergeResult{Id: testID, Outcome: runwaypb.Outcome_SUCCEEDED}, nil)
	expectPublish(t, q, publisher, "merge-signal", func(result *runwaymq.MergeResult) {
		assert.Equal(t, testID, result.Id)
		assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.Outcome)
	}, errors.New("queue publish failed"))

	require.Error(t, controller.Process(context.Background(), delivery))
}

func TestProcess_DeserializeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller, _, _, _ := newController(t, ctrl)

	delivery := newDelivery(t, ctrl, []byte(`{"id": not json}`))

	require.Error(t, controller.Process(context.Background(), delivery))
}

func expectPublish(t *testing.T, q *queuemock.MockQueue, publisher *queuemock.MockPublisher, topic string, assertResult func(*runwaymq.MergeResult), publishErr error) {
	t.Helper()

	q.EXPECT().Publisher().Return(publisher)
	publisher.EXPECT().
		Publish(gomock.Any(), topic, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			assert.Equal(t, testID, msg.ID)
			assert.Equal(t, testPartitionKey, msg.PartitionKey)

			result := &runwaymq.MergeResult{}
			require.NoError(t, runwaymq.Unmarshal(msg.Payload, result))
			assertResult(result)
			return publishErr
		})
}
