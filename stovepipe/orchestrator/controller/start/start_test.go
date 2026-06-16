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

const (
	testURI          = "git://git.example.com/uber/monorepo/refs%2Fheads%2Fmain/abcdef0123456789abcdef0123456789abcdef01"
	testPartitionKey = "git.example.com/uber/monorepo"
)

// captureRegistry builds a topic registry whose validate publisher records the
// last message it received into captured (when non-nil) and returns publishErr.
func captureRegistry(t *testing.T, ctrl *gomock.Controller, publishErr error, captured *entityqueue.Message) consumer.TopicRegistry {
	t.Helper()

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			if captured != nil {
				*captured = msg
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

func newTestController(t *testing.T, ctrl *gomock.Controller, publishErr error, captured *entityqueue.Message) *Controller {
	t.Helper()

	return NewController(Params{
		Logger:        zaptest.NewLogger(t).Sugar(),
		Scope:         tally.NoopScope,
		Registry:      captureRegistry(t, ctrl, publishErr, captured),
		TopicKey:      topickey.TopicKeyStart,
		ConsumerGroup: "orchestrator-start",
	})
}

// makeDelivery builds a delivery whose envelope carries partitionKey, the
// ordering key the producer stamps at ingestion.
func makeDelivery(t *testing.T, ctrl *gomock.Controller, payload []byte, partitionKey string) *queuemock.MockDelivery {
	t.Helper()

	msg := entityqueue.NewMessage(testURI, payload, partitionKey, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

// validPayload is a change event as the gateway produces it: a commit URI. The
// ordering key rides on the message envelope, not the payload.
func validPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := entity.ChangeEvent{URI: testURI}.ToBytes()
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

func TestController_Process_PublishesToValidate(t *testing.T) {
	ctrl := gomock.NewController(t)

	var captured entityqueue.Message
	controller := newTestController(t, ctrl, nil, &captured)
	delivery := makeDelivery(t, ctrl, validPayload(t), testPartitionKey)

	require.NoError(t, controller.Process(context.Background(), delivery))

	// start forwards the commit to validate, keyed by URI for idempotency and
	// propagating the envelope partition key verbatim to the next hop.
	assert.Equal(t, testURI, captured.ID)
	assert.Equal(t, testPartitionKey, captured.PartitionKey)

	var forwarded entity.ChangeEvent
	require.NoError(t, json.Unmarshal(captured.Payload, &forwarded))
	assert.Equal(t, testURI, forwarded.URI)
}

func TestController_Process_Errors(t *testing.T) {
	tests := []struct {
		name         string
		payload      []byte
		partitionKey string
	}{
		{name: "invalid json", payload: []byte(`{"invalid": json"}`), partitionKey: testPartitionKey},
		{name: "missing uri", payload: []byte(`{}`), partitionKey: testPartitionKey},
		{name: "empty uri", payload: []byte(`{"uri":""}`), partitionKey: testPartitionKey},
		// Valid commit, but the producer failed to stamp an envelope partition key.
		{name: "missing partition key", payload: validPayload(t), partitionKey: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller := newTestController(t, ctrl, nil, nil)
			delivery := makeDelivery(t, ctrl, tt.payload, tt.partitionKey)

			require.Error(t, controller.Process(context.Background(), delivery))
		})
	}
}

func TestController_Process_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, assert.AnError, nil)
	delivery := makeDelivery(t, ctrl, validPayload(t), testPartitionKey)

	require.Error(t, controller.Process(context.Background(), delivery))
}
