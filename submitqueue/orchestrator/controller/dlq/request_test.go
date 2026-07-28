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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	queue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQRequestController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, DecodeRequestID, TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	assert.Equal(t, "validate_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("validate_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-validate-dlq", c.ConsumerGroup())
}

func TestDLQRequestController_Process_LandRequestPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 1, State: entity.RequestStateStarted,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(1), int32(2), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, registry, DecodeLandRequestID, TopicKey(topickey.TopicKeyStart), "orchestrator-start-dlq")

	payload, err := entity.LandRequest{ID: "q/1", Queue: "q"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQRequestController_Process_CancelRequestPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/7").Return(entity.Request{
		ID: "q/7", Version: 2, State: entity.RequestStateBatched,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/7", int32(2), int32(3), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, registry, DecodeCancelRequestID, TopicKey(topickey.TopicKeyCancel), "orchestrator-cancel-dlq")

	payload, err := entity.CancelRequest{ID: "q/7", Reason: "user"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQRequestController_Process_RequestIDPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/3").Return(entity.Request{
		ID: "q/3", Version: 1, State: entity.RequestStateValidated,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/3", int32(1), int32(2), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(log entity.RequestLog) error {
		assert.Equal(t, "boom", log.LastError)
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, registry, DecodeRequestID, TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

	payload, err := entity.RequestID{ID: "q/3"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQRequestController_Process_DifferentTerminalOutcomeSkips(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 5, State: entity.RequestStateLanded,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, DecodeRequestID, TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	payload, err := entity.RequestID{ID: "q/1"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQRequestController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	// no store calls expected

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, DecodeRequestID, TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	delivery := newMockDelivery(ctrl, []byte("not json"))
	err := c.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestDLQRequestController_Process_EmptyIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	// no store calls expected

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, DecodeRequestID, TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	payload, err := entity.RequestID{ID: ""}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	err = c.Process(context.Background(), delivery)
	require.Error(t, err)
}

// newMockDelivery returns a MockDelivery wired up enough to be passed through
// the DLQ controller Process flow.
func newMockDelivery(ctrl *gomock.Controller, payload []byte) *queuemock.MockDelivery {
	msg := queue.NewMessage("dlq-msg-1", payload, "", nil)
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	d.EXPECT().Metadata().Return(map[string]string{
		"dlq.original_topic": "validate",
		"dlq.failure_count":  "3",
		"dlq.last_error":     "boom",
	}).AnyTimes()
	return d
}
