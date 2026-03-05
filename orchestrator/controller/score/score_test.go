package score

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
	scorermock "github.com/uber/submitqueue/extension/scorer/mock"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// newTestController creates a controller with test dependencies.
func newTestController(
	t *testing.T,
	ctrl *gomock.Controller,
	publishErr error,
	mockScorer *scorermock.MockScorer,
	mockStorage *storagemock.MockStorage,
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
		[]consumer.TopicConfig{{Key: consumer.TopicKeySpeculate, Name: "speculate", Queue: mockQ}},
	)
	require.NoError(t, err)

	return NewController(logger, scope, registry, consumer.TopicKeyScore, "orchestrator-score", mockScorer, mockStorage)
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockScorer := scorermock.NewMockScorer(ctrl)
	mockStorage := storagemock.NewMockStorage(ctrl)
	controller := newTestController(t, ctrl, nil, mockScorer, mockStorage)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyScore, controller.TopicKey())
	assert.Equal(t, "orchestrator-score", controller.ConsumerGroup())
	assert.Equal(t, "score", controller.Name())
}

func TestController_Process(t *testing.T) {
	tests := []struct {
		name       string
		batch      entity.Batch
		setupMocks func(*scorermock.MockScorer, *storagemock.MockStorage, *storagemock.MockRequestStore, *storagemock.MockBatchStore)
		publishErr error
		wantErr    bool
		wantRetry  bool
	}{
		{
			name: "Success",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID:    "test-queue/1",
					Queue: "test-queue",
					Change: entity.Change{
						URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
					},
				}, nil)
				s.EXPECT().Score(gomock.Any(), entity.Change{
					URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
				}).Return(0.85, nil)
				st.EXPECT().GetBatchStore().Return(bs)
				bs.EXPECT().UpdateScoreAndState(gomock.Any(), "test-queue/batch/1", int32(1), float32(0.85), entity.BatchStateSpeculating).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "EmptyBatch",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name: "MultiRequestBatch",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1", "test-queue/2"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name: "RequestNotFound",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{}, storage.ErrNotFound)
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name: "StorageFailure",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{}, fmt.Errorf("connection refused"))
			},
			wantErr:   true,
			wantRetry: true,
		},
		{
			name: "ScorerFailure",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID:    "test-queue/1",
					Queue: "test-queue",
					Change: entity.Change{
						URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
					},
				}, nil)
				s.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.0, fmt.Errorf("scorer unavailable"))
			},
			wantErr:   true,
			wantRetry: true,
		},
		{
			name: "BatchStoreVersionMismatch",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID:    "test-queue/1",
					Queue: "test-queue",
					Change: entity.Change{
						URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
					},
				}, nil)
				s.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.9, nil)
				st.EXPECT().GetBatchStore().Return(bs)
				bs.EXPECT().UpdateScoreAndState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
					fmt.Errorf("version mismatch: %w", storage.ErrVersionMismatch),
				)
			},
			wantErr:   true,
			wantRetry: false,
		},
		{
			name: "BatchStoreFailure",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID:    "test-queue/1",
					Queue: "test-queue",
					Change: entity.Change{
						URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
					},
				}, nil)
				s.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.9, nil)
				st.EXPECT().GetBatchStore().Return(bs)
				bs.EXPECT().UpdateScoreAndState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
					fmt.Errorf("database unavailable"),
				)
			},
			wantErr:   true,
			wantRetry: true,
		},
		{
			name: "PublishFailure",
			batch: entity.Batch{
				ID:       "test-queue/batch/1",
				Queue:    "test-queue",
				Contains: []string{"test-queue/1"},
				State:    entity.BatchStateCreated,
				Version:  1,
			},
			setupMocks: func(s *scorermock.MockScorer, st *storagemock.MockStorage, rs *storagemock.MockRequestStore, bs *storagemock.MockBatchStore) {
				st.EXPECT().GetRequestStore().Return(rs)
				rs.EXPECT().Get(gomock.Any(), "test-queue/1").Return(entity.Request{
					ID:    "test-queue/1",
					Queue: "test-queue",
					Change: entity.Change{
						URIs: []string{"github://uber/submitqueue/pull/1/abc123"},
					},
				}, nil)
				s.EXPECT().Score(gomock.Any(), gomock.Any()).Return(0.9, nil)
				st.EXPECT().GetBatchStore().Return(bs)
				bs.EXPECT().UpdateScoreAndState(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			},
			publishErr: fmt.Errorf("publish failed"),
			wantErr:    true,
			wantRetry:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockScorer := scorermock.NewMockScorer(ctrl)
			mockStorageFactory := storagemock.NewMockStorage(ctrl)
			mockRequestStore := storagemock.NewMockRequestStore(ctrl)
			mockBatchStore := storagemock.NewMockBatchStore(ctrl)

			tt.setupMocks(mockScorer, mockStorageFactory, mockRequestStore, mockBatchStore)

			controller := newTestController(t, ctrl, tt.publishErr, mockScorer, mockStorageFactory)

			payload, err := tt.batch.ToBytes()
			require.NoError(t, err)

			msg := queue.NewMessage(tt.batch.ID, payload, tt.batch.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err = controller.Process(context.Background(), delivery)

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestController_Process_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockScorer := scorermock.NewMockScorer(ctrl)
	mockStorage := storagemock.NewMockStorage(ctrl)

	controller := newTestController(t, ctrl, nil, mockScorer, mockStorage)

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
	mockScorer := scorermock.NewMockScorer(ctrl)
	mockStorage := storagemock.NewMockStorage(ctrl)
	controller := newTestController(t, ctrl, nil, mockScorer, mockStorage)

	var _ consumer.Controller = controller
}
