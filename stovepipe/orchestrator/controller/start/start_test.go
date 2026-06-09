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

package start

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/consumer"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/stovepipe/core/topickey"
	entity "github.com/uber/submitqueue/stovepipe/entity"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const testURI = "git://uber/monorepo/main/abcdef0123456789abcdef0123456789abcdef01"

// captureRegistry builds a topic registry whose validate publisher records the
// last payload it received, so tests can assert on what start forwards.
func captureRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error, captured *[]byte) consumer.TopicRegistry {
	t.Helper()

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			if captured != nil {
				*captured = msg.Payload
			}
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyValidate, Name: "validate", Queue: mockQ},
	})
	require.NoError(t, err)
	return registry
}

func newTestController(t *testing.T, ctrl *gomock.Controller, publishErr error, captured *[]byte) *Controller {
	t.Helper()

	return NewController(
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
		captureRegistry(t, ctrl, publishErr, captured),
		topickey.TopicKeyStart,
		"orchestrator-start",
	)
}

func makeDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *queuemock.MockDelivery {
	t.Helper()

	msg := entityqueue.NewMessage(testURI, payload, "uber/monorepo", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

func eventPayload(t *testing.T, uri string) []byte {
	t.Helper()
	payload, err := entity.ChangeEvent{URI: uri}.ToBytes()
	require.NoError(t, err)
	return payload
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyStart, controller.TopicKey())
	assert.Equal(t, "orchestrator-start", controller.ConsumerGroup())
	assert.Equal(t, "start", controller.Name())
}

func TestController_Process_PublishesChangeURI(t *testing.T) {
	ctrl := gomock.NewController(t)

	var validatePayload []byte
	controller := newTestController(t, ctrl, nil, &validatePayload)
	delivery := makeDelivery(t, ctrl, eventPayload(t, testURI))

	require.NoError(t, controller.Process(context.Background(), delivery))

	var ref entity.ChangeURI
	require.NoError(t, json.Unmarshal(validatePayload, &ref))
	assert.Equal(t, testURI, ref.URI)
}

func TestController_Process_Errors(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "invalid json", payload: []byte(`{"invalid": json"}`)},
		{name: "non-git uri", payload: eventPayloadStr(`github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01`)},
		{name: "empty uri", payload: []byte(`{"uri":""}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller := newTestController(t, ctrl, nil, nil)
			delivery := makeDelivery(t, ctrl, tc.payload)

			require.Error(t, controller.Process(context.Background(), delivery))
		})
	}
}

func TestController_Process_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, assert.AnError, nil)
	delivery := makeDelivery(t, ctrl, eventPayload(t, testURI))

	require.Error(t, controller.Process(context.Background(), delivery))
}

// eventPayloadStr is a test-table helper that marshals a raw URI without a *testing.T.
func eventPayloadStr(uri string) []byte {
	payload, _ := entity.ChangeEvent{URI: uri}.ToBytes()
	return payload
}
