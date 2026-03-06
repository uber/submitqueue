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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(t *testing.T, ctrl *gomock.Controller, store *storagemock.MockStorage, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyValidate, Name: "validate", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, registry, consumer.TopicKeyRequest, "orchestrator-request")
}

// newMockStorage creates a MockStorage with a MockRequestStore that succeeds on Create.
func newMockStorage(ctrl *gomock.Controller) *storagemock.MockStorage {
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	return store
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyRequest, controller.TopicKey())
	assert.Equal(t, "orchestrator-request", controller.ConsumerGroup())
	assert.Equal(t, "request", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage("test-queue/123", payload, "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	// Process the delivery
	err := controller.Process(context.Background(), delivery)

	// Should return NonRetryableError for malformed messages
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_AllRequestStates(t *testing.T) {
	tests := []struct {
		name     string
		state    entity.RequestState
		strategy entity.RequestLandStrategy
	}{
		{"new request", entity.RequestStateNew, entity.RequestLandStrategyRebase},
		{"processing request", entity.RequestStateProcessing, entity.RequestLandStrategySquashRebase},
		{"landed request", entity.RequestStateLanded, entity.RequestLandStrategyMerge},
		{"error request", entity.RequestStateError, entity.RequestLandStrategyRebase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

			request := entity.Request{
				ID:           fmt.Sprintf("queue/%s", tt.state),
				Queue:        "test-queue",
				Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/aaa111bbb"}},
				LandStrategy: tt.strategy,
				State:        tt.state,
				Version:      1,
			}

			payload, err := request.ToBytes()
			require.NoError(t, err)

			msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err = controller.Process(context.Background(), delivery)
			require.NoError(t, err)
		})
	}
}

func TestController_Process_MultipleChanges(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

	request := entity.Request{
		ID:    "queue/999",
		Queue: "test-queue",
		Change: entity.Change{
			URIs: []string{
				"github://uber/monorepo/pull/1/aaa111",
				"github://uber/monorepo/pull/2/bbb222",
				"github://uber/monorepo/pull/3/ccc333",
			},
		},
		LandStrategy: entity.RequestLandStrategySquashRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newMockStorage(ctrl), fmt.Errorf("publish failed"))

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	assert.Error(t, err)
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("database connection failed"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_AlreadyExistsSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("duplicate: %w", storage.ErrAlreadyExists))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateNew,
		Version:      1,
	}

	payload, err := request.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	// Should succeed even though Create returns ErrAlreadyExists (idempotent)
	err = controller.Process(context.Background(), delivery)
	require.NoError(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), nil)

	var _ consumer.Controller = controller
}
