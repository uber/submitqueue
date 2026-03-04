package conclude

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
func newTestController(t *testing.T, ctrl *gomock.Controller, mockStorage *storagemock.MockStorage) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockRequestStore := storagemock.NewMockRequestStore(ctrl)
		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
	}

	registry, err := consumer.NewTopicRegistry(nil)
	require.NoError(t, err)

	return NewController(logger, scope, mockStorage, registry, consumer.TopicKeyConclude, "orchestrator-conclude")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyConclude, controller.TopicKey())
	assert.Equal(t, "orchestrator-conclude", controller.ConsumerGroup())
	assert.Equal(t, "conclude", controller.Name())
}

func TestController_Process(t *testing.T) {
	tests := []struct {
		name       string
		batch      entity.Batch
		setupStore func(*gomock.Controller) *storagemock.MockStorage
		wantErr    bool
		retryable  bool
	}{
		{
			name: "succeeded batch lands requests",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1", "test-queue/2"},
				State:    entity.BatchStateSucceeded,
				Version:  3,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID: "test-queue/1", Version: 2, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/1", int32(2), entity.RequestStateLanded).Return(nil)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/2").Return(entity.Request{
					ID: "test-queue/2", Version: 3, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/2", int32(3), entity.RequestStateLanded).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
		},
		{
			name: "failed batch errors requests",
			batch: entity.Batch{
				ID:       "test-queue/batch/2",
				Queue:    "test-queue",
				Contains: []string{"test-queue/5"},
				State:    entity.BatchStateFailed,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/5").Return(entity.Request{
					ID: "test-queue/5", Version: 1, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/5", int32(1), entity.RequestStateError).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
		},
		{
			name: "cancelled batch returns error",
			batch: entity.Batch{
				ID:       "test-queue/batch/3",
				Queue:    "test-queue",
				Contains: []string{"test-queue/10"},
				State:    entity.BatchStateCancelled,
				Version:  2,
			},
			wantErr:   true,
			retryable: false,
		},
		{
			name: "non-terminal batch state returns error",
			batch: entity.Batch{
				ID:       "test-queue/batch/4",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			wantErr:   true,
			retryable: false,
		},
		{
			name: "request store get failure is retryable",
			batch: entity.Batch{
				ID:       "test-queue/batch/5",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{}, fmt.Errorf("db connection lost"))

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			wantErr:   true,
			retryable: true,
		},
		{
			name: "request store update failure is retryable",
			batch: entity.Batch{
				ID:       "test-queue/batch/6",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateSucceeded,
				Version:  2,
			},
			setupStore: func(ctrl *gomock.Controller) *storagemock.MockStorage {
				mockRequestStore := storagemock.NewMockRequestStore(ctrl)
				mockRequestStore.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID: "test-queue/1", Version: 2, State: entity.RequestStateProcessing,
				}, nil)
				mockRequestStore.EXPECT().UpdateState(gomock.Any(), "test-queue/1", int32(2), entity.RequestStateLanded).Return(storage.ErrVersionMismatch)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockRequestStore).AnyTimes()
				return mockStorage
			},
			wantErr:   true,
			retryable: true,
		},
		{
			name: "empty contains list succeeds",
			batch: entity.Batch{
				ID:      "test-queue/batch/7",
				Queue:   "test-queue",
				State:   entity.BatchStateSucceeded,
				Version: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			var mockStorage *storagemock.MockStorage
			if tt.setupStore != nil {
				mockStorage = tt.setupStore(ctrl)
			}

			controller := newTestController(t, ctrl, mockStorage)

			payload, err := tt.batch.ToBytes()
			require.NoError(t, err)

			msg := queue.NewMessage(tt.batch.ID, payload, tt.batch.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err = controller.Process(context.Background(), delivery)

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.retryable, errs.IsRetryable(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, nil)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, nil)

	var _ consumer.Controller = controller
}
