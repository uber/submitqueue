package merge

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	"github.com/uber/submitqueue/extension/landprovider"
	landprovidermock "github.com/uber/submitqueue/extension/landprovider/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	lp := landprovidermock.NewMockLandProvider(ctrl)
	mockStorage := storagemock.NewMockStorage(ctrl)

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyMergeSignal, Name: "merge-signal", Queue: queuemock.NewMockQueue(ctrl)}},
	)
	require.NoError(t, err)

	controller := NewController(logger, scope, registry, lp, mockStorage, consumer.TopicKeyToMerge, "orchestrator-merge")

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyToMerge, controller.TopicKey())
	assert.Equal(t, "orchestrator-merge", controller.ConsumerGroup())
	assert.Equal(t, "merge", controller.Name())
}

func TestController_Process(t *testing.T) {
	testRequest := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateProcessing,
		Version:      1,
	}

	testBatch := entity.Batch{
		ID:       "test-queue/batch/1",
		Queue:    "test-queue",
		Contains: []string{"test-queue/123"},
		State:    entity.BatchStateLanding,
		Version:  1,
	}

	tests := []struct {
		name       string
		setupMocks func(*gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error)
		payload    string
		wantErr    bool
	}{
		{
			name: "success",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)
				mockBatchStore.EXPECT().UpdateState(gomock.Any(), "test-queue/batch/1", int32(1), entity.BatchStateLandSucceeded).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: false,
		},
		{
			name: "empty batch ID",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				mockStorage := storagemock.NewMockStorage(ctrl)
				return lp, mockStorage, nil
			},
			payload: "",
			wantErr: true,
		},
		{
			name: "batch not found",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(entity.Batch{}, fmt.Errorf("not found"))

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
		{
			name: "idempotent - already land_succeeded",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)

				succeededBatch := testBatch
				succeededBatch.State = entity.BatchStateLandSucceeded
				succeededBatch.Version = 2

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(succeededBatch, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: false,
		},
		{
			name: "idempotent - already land_failed",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)

				failedBatch := testBatch
				failedBatch.State = entity.BatchStateLandFailed
				failedBatch.Version = 2

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(failedBatch, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: false,
		},
		{
			name: "request not found",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(entity.Request{}, fmt.Errorf("not found"))

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
		{
			name: "land rejected - batch marked as failed and published",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(landprovider.WrapLandRejected(fmt.Errorf("merge conflict")))

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)
				mockBatchStore.EXPECT().UpdateState(gomock.Any(), "test-queue/batch/1", int32(1), entity.BatchStateLandFailed).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: false,
		},
		{
			name: "land error - not rejected",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("generic error"))

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
		{
			name: "batch update error after successful land",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)
				mockBatchStore.EXPECT().UpdateState(gomock.Any(), "test-queue/batch/1", int32(1), entity.BatchStateLandSucceeded).Return(fmt.Errorf("version mismatch"))

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
		{
			name: "batch update error after land rejection",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(landprovider.WrapLandRejected(fmt.Errorf("merge conflict")))

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)
				mockBatchStore.EXPECT().UpdateState(gomock.Any(), "test-queue/batch/1", int32(1), entity.BatchStateLandFailed).Return(fmt.Errorf("version mismatch"))

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, nil
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
		{
			name: "publish failure",
			setupMocks: func(ctrl *gomock.Controller) (landprovider.LandProvider, *storagemock.MockStorage, error) {
				lp := landprovidermock.NewMockLandProvider(ctrl)
				lp.EXPECT().Land(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

				mockReqStore := storagemock.NewMockRequestStore(ctrl)
				mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(testRequest, nil)

				mockBatchStore := storagemock.NewMockBatchStore(ctrl)
				mockBatchStore.EXPECT().Get(gomock.Any(), "test-queue/batch/1").Return(testBatch, nil)
				mockBatchStore.EXPECT().UpdateState(gomock.Any(), "test-queue/batch/1", int32(1), entity.BatchStateLandSucceeded).Return(nil)

				mockStorage := storagemock.NewMockStorage(ctrl)
				mockStorage.EXPECT().GetRequestStore().Return(mockReqStore)
				mockStorage.EXPECT().GetBatchStore().Return(mockBatchStore).AnyTimes()

				return lp, mockStorage, fmt.Errorf("publish failed")
			},
			payload: "test-queue/batch/1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			lp, mockStorage, publishErr := tt.setupMocks(ctrl)

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
				[]consumer.TopicConfig{{Key: consumer.TopicKeyMergeSignal, Name: "merge-signal", Queue: mockQ}},
			)
			require.NoError(t, err)

			controller := NewController(logger, scope, registry, lp, mockStorage, consumer.TopicKeyToMerge, "orchestrator-merge")

			msg := queue.NewMessage("test-msg-id", []byte(tt.payload), "test-queue", nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err = controller.Process(context.Background(), delivery)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	lp := landprovidermock.NewMockLandProvider(ctrl)
	mockStorage := storagemock.NewMockStorage(ctrl)

	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: consumer.TopicKeyMergeSignal, Name: "merge-signal", Queue: queuemock.NewMockQueue(ctrl)}},
	)
	require.NoError(t, err)

	controller := NewController(logger, scope, registry, lp, mockStorage, consumer.TopicKeyToMerge, "orchestrator-merge")

	var _ consumer.Controller = controller
}
