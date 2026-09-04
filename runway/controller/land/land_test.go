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

package land

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/runway/extension/lander"
	landermock "github.com/uber/submitqueue/runway/extension/lander/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testID           = "test-queue/1"
	testQueue        = "test-queue"
	testPartitionKey = "test-queue"
)

func newDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	msg := entityqueue.NewMessage(testID, payload, testPartitionKey, nil)
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func requestPayload(t *testing.T, req *runwaymq.LandRequest) []byte {
	t.Helper()
	payload, err := runwaymq.Marshal(req)
	require.NoError(t, err)
	return payload
}

func newRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ entityqueue.Message) error {
			return publishErr
		},
	).AnyTimes()

	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLandSignal, Name: "land-signal", Queue: q},
	})
	require.NoError(t, err)
	return registry, pub
}

func newController(t *testing.T, factory lander.Factory, registry consumer.TopicRegistry) *Controller {
	t.Helper()
	return NewController(Params{
		Logger:        zaptest.NewLogger(t).Sugar(),
		Scope:         tally.NoopScope,
		LanderFactory: factory,
		Registry:      registry,
		TopicKey:      runwaymq.TopicKeyLand,
		ConsumerGroup: "runway-land",
	})
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory := landermock.NewMockFactory(ctrl)
	registry, _ := newRegistry(t, ctrl, nil)

	controller := newController(t, factory, registry)
	require.NotNil(t, controller)
	assert.Equal(t, runwaymq.TopicKeyLand, controller.TopicKey())
	assert.Equal(t, "runway-land", controller.ConsumerGroup())
	assert.Equal(t, "land", controller.Name())
}

func TestProcess_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	expectedResult := &runwaymq.LandResult{
		Id:      testID,
		Outcome: runwaypb.Outcome_SUCCEEDED,
		Steps: []*runwaymq.StepResult{
			{StepId: "step-1", Outputs: []*runwaymq.StepOutput{{Id: "abc123"}}},
		},
	}

	m := landermock.NewMockLander(ctrl)
	m.EXPECT().Land(gomock.Any(), gomock.Any()).Return(expectedResult, nil)

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(m, nil)

	var gotTopic string
	var gotPayload []byte
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLandSignal, Name: "land-signal", Queue: q},
	})
	require.NoError(t, err)

	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	require.NoError(t, controller.Process(context.Background(), delivery))

	assert.Equal(t, "land-signal", gotTopic)
	result := &runwaymq.LandResult{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, result))
	assert.Equal(t, testID, result.Id)
	assert.Equal(t, runwaypb.Outcome_SUCCEEDED, result.Outcome)
	require.Len(t, result.Steps, 1)
	require.Len(t, result.Steps[0].Outputs, 1)
	assert.Equal(t, "abc123", result.Steps[0].Outputs[0].Id)
}

func TestProcess_LandConflict(t *testing.T) {
	ctrl := gomock.NewController(t)

	m := landermock.NewMockLander(ctrl)
	m.EXPECT().Land(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("conflict in foo.go: %w", lander.ErrConflict))

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(m, nil)

	var gotTopic string
	var gotPayload []byte
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLandSignal, Name: "land-signal", Queue: q},
	})
	require.NoError(t, err)

	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	require.NoError(t, controller.Process(context.Background(), delivery))

	assert.Equal(t, "land-signal", gotTopic)
	result := &runwaymq.LandResult{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, result))
	assert.Equal(t, testID, result.Id)
	assert.Equal(t, runwaypb.Outcome_FAILED, result.Outcome)
	assert.NotEmpty(t, result.Reason)
}

func TestProcess_InvalidRequest(t *testing.T) {
	ctrl := gomock.NewController(t)

	m := landermock.NewMockLander(ctrl)
	m.EXPECT().Land(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("bad strategy: %w", lander.ErrInvalidRequest))

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(m, nil)

	var gotTopic string
	var gotPayload []byte
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: runwaymq.TopicKeyLandSignal, Name: "land-signal", Queue: q},
	})
	require.NoError(t, err)

	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	require.NoError(t, controller.Process(context.Background(), delivery))

	assert.Equal(t, "land-signal", gotTopic)
	result := &runwaymq.LandResult{}
	require.NoError(t, runwaymq.Unmarshal(gotPayload, result))
	assert.Equal(t, testID, result.Id)
	assert.Equal(t, runwaypb.Outcome_FAILED, result.Outcome)
	assert.NotEmpty(t, result.Reason)
}

func TestProcess_LanderInfraError(t *testing.T) {
	ctrl := gomock.NewController(t)

	m := landermock.NewMockLander(ctrl)
	m.EXPECT().Land(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("git timeout"))

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(m, nil)

	registry, _ := newRegistry(t, ctrl, nil)
	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestProcess_FactoryError(t *testing.T) {
	ctrl := gomock.NewController(t)

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(nil, fmt.Errorf("unknown queue"))

	registry, _ := newRegistry(t, ctrl, nil)
	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestProcess_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)

	expectedResult := &runwaymq.LandResult{
		Id:      testID,
		Outcome: runwaypb.Outcome_SUCCEEDED,
	}

	m := landermock.NewMockLander(ctrl)
	m.EXPECT().Land(gomock.Any(), gomock.Any()).Return(expectedResult, nil)

	factory := landermock.NewMockFactory(ctrl)
	factory.EXPECT().For(lander.Config{QueueName: testQueue}).Return(m, nil)

	registry, _ := newRegistry(t, ctrl, fmt.Errorf("publish failed"))
	controller := newController(t, factory, registry)

	req := &runwaymq.LandRequest{
		Id:        testID,
		QueueName: testQueue,
		Steps:     []*runwaymq.LandStep{{StepId: "step-1"}},
	}
	delivery := newDelivery(t, ctrl, requestPayload(t, req))

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestProcess_DeserializeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	factory := landermock.NewMockFactory(ctrl)
	registry, _ := newRegistry(t, ctrl, nil)
	controller := newController(t, factory, registry)

	delivery := newDelivery(t, ctrl, []byte(`{"id": not json}`))

	require.Error(t, controller.Process(context.Background(), delivery))
}
