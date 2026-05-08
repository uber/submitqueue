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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	changemock "github.com/uber/submitqueue/extension/changestore/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(
	t *testing.T,
	ctrl *gomock.Controller,
	store *storagemock.MockStorage,
	cs *changemock.MockChangeStore,
	publishErr error,
) *Controller {
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
		[]consumer.TopicConfig{
			{Key: consumer.TopicKeyValidate, Name: "validate", Queue: mockQ},
			{Key: consumer.TopicKeyLog, Name: "log", Queue: mockQ},
		},
	)
	require.NoError(t, err)

	return NewController(logger, scope, store, cs, registry, consumer.TopicKeyStart, "orchestrator-start")
}

// newMockStorage creates a MockStorage with a MockRequestStore that succeeds on Create.
func newMockStorage(ctrl *gomock.Controller) *storagemock.MockStorage {
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	return store
}

// newMockChangeStore creates a MockChangeStore that accepts any Create call.
func newMockChangeStore(ctrl *gomock.Controller) *changemock.MockChangeStore {
	cs := changemock.NewMockChangeStore(ctrl)
	cs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return cs
}

// makeDelivery builds a MockDelivery wrapping a serialized LandRequest.
func makeDelivery(t *testing.T, ctrl *gomock.Controller, lr entity.LandRequest) *queuemock.MockDelivery {
	payload, err := lr.ToBytes()
	require.NoError(t, err)

	msg := queue.NewMessage(lr.ID, payload, lr.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyStart, controller.TopicKey())
	assert.Equal(t, "orchestrator-start", controller.ConsumerGroup())
	assert.Equal(t, "start", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), nil)

	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
	})

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), nil)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_ConstructsRequestWithStateAndVersion(t *testing.T) {
	ctrl := gomock.NewController(t)

	var capturedRequest entity.Request
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req entity.Request) error {
			capturedRequest = req
			return nil
		},
	)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), nil)

	landRequest := entity.LandRequest{
		ID:           "test-queue/42",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/abc123def"}},
		LandStrategy: entity.RequestLandStrategySquashRebase,
	}
	delivery := makeDelivery(t, ctrl, landRequest)
	require.NoError(t, controller.Process(context.Background(), delivery))

	assert.Equal(t, landRequest.ID, capturedRequest.ID)
	assert.Equal(t, landRequest.Queue, capturedRequest.Queue)
	assert.Equal(t, landRequest.Change.URIs, capturedRequest.Change.URIs)
	assert.Equal(t, landRequest.LandStrategy, capturedRequest.LandStrategy)
	assert.Equal(t, entity.RequestStateStarted, capturedRequest.State)
	assert.Equal(t, int32(1), capturedRequest.Version)
}

func TestController_Process_AllStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy entity.RequestLandStrategy
	}{
		{"rebase", entity.RequestLandStrategyRebase},
		{"squash rebase", entity.RequestLandStrategySquashRebase},
		{"merge", entity.RequestLandStrategyMerge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), nil)

			delivery := makeDelivery(t, ctrl, entity.LandRequest{
				ID:           fmt.Sprintf("queue/%s", tt.strategy),
				Queue:        "test-queue",
				Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/aaa111bbb"}},
				LandStrategy: tt.strategy,
			})

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}

func TestController_Process_MultipleChanges(t *testing.T) {
	ctrl := gomock.NewController(t)

	cs := changemock.NewMockChangeStore(ctrl)
	var captured []entity.ChangeRecord
	cs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, record entity.ChangeRecord) error {
			captured = append(captured, record)
			return nil
		},
	).Times(3)

	controller := newTestController(t, ctrl, newMockStorage(ctrl), cs, nil)

	uris := []string{
		"github://uber/monorepo/pull/1/aaa111",
		"github://uber/monorepo/pull/2/bbb222",
		"github://uber/monorepo/pull/3/ccc333",
	}
	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "queue/999",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: uris},
		LandStrategy: entity.RequestLandStrategySquashRebase,
	})

	require.NoError(t, controller.Process(context.Background(), delivery))

	require.Len(t, captured, len(uris))
	for i, r := range captured {
		assert.Equal(t, uris[i], r.URI)
		assert.Equal(t, "queue/999", r.RequestID)
		assert.Equal(t, "test-queue", r.Queue)
		assert.Positive(t, r.CreatedAt)
		assert.Equal(t, r.CreatedAt, r.UpdatedAt)
	}
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), fmt.Errorf("publish failed"))

	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
	})

	assert.Error(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("database connection failed"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), nil)

	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
	})

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_AlreadyExistsSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("duplicate: %w", storage.ErrAlreadyExists))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), nil)

	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
	})

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_ChangeStoreFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cs := changemock.NewMockChangeStore(ctrl)
	cs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("change store down"))

	controller := newTestController(t, ctrl, newMockStorage(ctrl), cs, nil)

	delivery := makeDelivery(t, ctrl, entity.LandRequest{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
	})

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newMockStorage(ctrl), newMockChangeStore(ctrl), nil)

	var _ consumer.Controller = controller
}
