package batch

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	countermock "github.com/uber/submitqueue/extension/counter/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newSequentialCounter returns a mock counter that returns incrementing values starting at 1.
func newSequentialCounter(ctrl *gomock.Controller) *countermock.MockCounter {
	var seq int64
	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, domain string) (int64, error) {
			return atomic.AddInt64(&seq, 1), nil
		},
	).AnyTimes()
	return cnt
}

// newTestController creates a controller with test dependencies.
// If mockStorage is nil, a default MockStorage with an empty batch store is created.
func newTestController(t *testing.T, ctrl *gomock.Controller, cnt *countermock.MockCounter, mockStorage *storagemock.MockStorage, publishErr error) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	if mockStorage == nil {
		mockBatchStore := storagemock.NewMockBatchStore(ctrl)
		mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockBatchStore.EXPECT().UpdateState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockStorage = storagemock.NewMockStorage(ctrl)
		mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	}

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyScore, Name: "score", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, registry, cnt, mockStorage, consumer.TopicKeyBatch, "orchestrator-batch")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyBatch, controller.TopicKey())
	assert.Equal(t, "orchestrator-batch", controller.ConsumerGroup())
	assert.Equal(t, "batch", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

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

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	invalidPayload := []byte(`{"invalid": json"}`)
	msg := queue.NewMessage("invalid-msg", invalidPayload, "partition1", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)

	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, fmt.Errorf("publish failed"))

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

func TestController_Process_CounterFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	cnt := countermock.NewMockCounter(ctrl)
	cnt.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), fmt.Errorf("counter unavailable"))
	controller := newTestController(t, ctrl, cnt, nil, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	assert.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_WithDependencies(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Set up storage with active batches to become dependencies.
	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateCreated, Version: 1},
		{ID: "test-queue/batch/2", Queue: "test-queue", State: entity.BatchStateSpeculating, Version: 2},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().UpdateState(gomock.Any(), gomock.Any(), int32(1), entity.BatchStateReady).Return(nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	// batch/1 has no existing dependents — will be created.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{}, storage.ErrNotFound)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	// batch/2 already has an existing dependent — will be updated.
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/2").Return(entity.BatchDependent{
		BatchID:    "test-queue/batch/2",
		Dependents: []string{"test-queue/batch/99"},
		Version:    1,
	}, nil)
	mockBatchDependentStore.EXPECT().UpdateDependents(gomock.Any(), "test-queue/batch/2", int32(1), gomock.Any()).Return(nil)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

	request := entity.Request{
		ID:           "test-queue/456",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/789/abc123def"}},
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
	require.NoError(t, err)
}

func TestController_Process_BatchCreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("db connection lost"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_BatchCreateAlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_GetActiveBatchesFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(nil, fmt.Errorf("db timeout"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_GetBatchDependentFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateReady, Version: 1},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{}, fmt.Errorf("db error"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_UpdateDependentsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateReady, Version: 1},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{
		BatchID:    "test-queue/batch/1",
		Dependents: []string{"test-queue/batch/99"},
		Version:    1,
	}, nil)
	mockBatchDependentStore.EXPECT().UpdateDependents(gomock.Any(), "test-queue/batch/1", int32(1), gomock.Any()).Return(fmt.Errorf("version conflict"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_CreateBatchDependentFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	activeBatches := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: "test-queue", State: entity.BatchStateReady, Version: 1},
	}

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(activeBatches, nil)

	mockBatchDependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	mockBatchDependentStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.BatchDependent{}, storage.ErrNotFound)
	mockBatchDependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(fmt.Errorf("db error"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()
	mockStorage.EXPECT().GetBatchDependentStore().Return(mockBatchDependentStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_Process_UpdateStateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockBatchStore := storagemock.NewMockBatchStore(ctrl)
	mockBatchStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	mockBatchStore.EXPECT().GetByQueueAndStates(gomock.Any(), "test-queue", gomock.Any()).Return(nil, nil)
	mockBatchStore.EXPECT().UpdateState(gomock.Any(), gomock.Any(), int32(1), entity.BatchStateReady).Return(fmt.Errorf("version conflict"))

	mockStorage := storagemock.NewMockStorage(ctrl)
	mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), mockStorage, nil)

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

	msg := queue.NewMessage(request.ID, payload, request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err = controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsRetryable(err))
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := newTestController(t, ctrl, newSequentialCounter(ctrl), nil, nil)

	var _ consumer.Controller = controller
}
