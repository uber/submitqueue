// Copyright (c) 2026 Uber Technologies, Inc.
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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/consumer"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstoragemock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	dependencyAnalysisDLQGroup = "orchestrator-dependency-analysis-dlq"
	deadLetteredBatchID        = "8"
	carriedRequestID           = "1"
)

func newDependencyAnalysisController(t *testing.T, store *orchstoragemock.MockStorage, registry consumer.TopicRegistry) consumer.Controller {
	return NewDLQDependencyAnalysisController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, registry,
		TopicKey(topickey.TopicKeyDependencyAnalysis), dependencyAnalysisDLQGroup)
}

func dependencyAnalysisPayload(t *testing.T, batchID string) []byte {
	payload, err := sqmq.MarshalID(sqmq.TopicKeyDependencyAnalysis, batchID, "q")
	require.NoError(t, err)
	return payload
}

func TestDLQDependencyAnalysisController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newDependencyAnalysisController(t, orchstoragemock.NewMockStorage(ctrl), consumer.TopicRegistry{})

	assert.Equal(t, "dependency-analysis_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("dependency-analysis_dlq"), c.TopicKey())
	assert.Equal(t, dependencyAnalysisDLQGroup, c.ConsumerGroup())
}

func TestDLQDependencyAnalysisController_Process_ReconcilesByBatchState(t *testing.T) {
	tests := []struct {
		name  string
		state entity.BatchState
		// otherBatches are further batches associated with the same request.
		otherBatches     []entity.Batch
		wantBatchFailed  bool
		wantRequestError bool
	}{
		{
			name:             "creating batch that owns its request is failed",
			state:            entity.BatchStateCreating,
			wantBatchFailed:  true,
			wantRequestError: true,
		},
		{
			// A sibling still in Creating has not been chosen either, so it
			// does not make this one a duplicate.
			name:             "creating batch beside another creating batch is failed",
			state:            entity.BatchStateCreating,
			otherBatches:     []entity.Batch{{ID: "7", Queue: "q", State: entity.BatchStateCreating}},
			wantBatchFailed:  true,
			wantRequestError: true,
		},
		{
			name:         "creating duplicate of a live batch is left alone",
			state:        entity.BatchStateCreating,
			otherBatches: []entity.Batch{{ID: "7", Queue: "q", State: entity.BatchStateSpeculating}},
		},
		{
			name:         "creating duplicate of a landed batch is left alone",
			state:        entity.BatchStateCreating,
			otherBatches: []entity.Batch{{ID: "7", Queue: "q", State: entity.BatchStateSucceeded}},
		},
		{
			name:             "created batch whose announcement was lost is failed",
			state:            entity.BatchStateCreated,
			wantBatchFailed:  true,
			wantRequestError: true,
		},
		{
			// A previous attempt failed the batch and crashed before the fan-out.
			name:             "failed batch repeats the fan-out",
			state:            entity.BatchStateFailed,
			wantRequestError: true,
		},
		{name: "speculating batch is left to speculate", state: entity.BatchStateSpeculating},
		{name: "landing batch is left to land", state: entity.BatchStateLanding},
		{name: "cancelling batch is left to cancel", state: entity.BatchStateCancelling},
		{name: "succeeded batch is left alone", state: entity.BatchStateSucceeded},
		{name: "cancelled batch is left alone", state: entity.BatchStateCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := entity.Batch{
				ID: deadLetteredBatchID, Queue: "q", Contains: []string{carriedRequestID},
				State: tt.state, Version: 2,
			}
			batches := map[string]entity.Batch{batch.ID: batch}
			associations := []entity.RequestBatch{{RequestID: carriedRequestID, BatchID: batch.ID, Version: 1}}
			for _, other := range tt.otherBatches {
				batches[other.ID] = other
				associations = append(associations, entity.RequestBatch{RequestID: carriedRequestID, BatchID: other.ID, Version: 1})
			}

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, id string) (entity.Batch, error) {
					b, ok := batches[id]
					if !ok {
						return entity.Batch{}, storage.ErrNotFound
					}
					return b, nil
				},
			).AnyTimes()
			if tt.wantBatchFailed {
				batchStore.EXPECT().Update(gomock.Any(), batchWithState(batch, entity.BatchStateFailed), int32(2), int32(3)).Return(nil)
			}

			requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
			requestBatchStore.EXPECT().GetByRequestID(gomock.Any(), carriedRequestID).Return(associations, nil).AnyTimes()

			request := entity.Request{ID: carriedRequestID, Queue: "q", Version: 3, State: entity.RequestStateBatched}
			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), carriedRequestID).Return(request, nil).AnyTimes()
			logPublishes := 0
			if tt.wantRequestError {
				requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(3), int32(4)).Return(nil)
				logPublishes = 1
			}

			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()

			registry := newTestLogRegistry(t, ctrl, logPublishes, func(entity.RequestLog) error { return nil })
			c := newDependencyAnalysisController(t, store, registry)

			require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, dependencyAnalysisPayload(t, batch.ID))))
		})
	}
}

func TestDLQDependencyAnalysisController_Process_BatchNotFoundAcks(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), deadLetteredBatchID).Return(entity.Batch{}, storage.ErrNotFound)

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	c := newDependencyAnalysisController(t, store, consumer.TopicRegistry{})

	require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, dependencyAnalysisPayload(t, deadLetteredBatchID))))
}

// Without the association lookup the controller cannot tell a duplicate from
// the owner, so it must retry rather than guess.
func TestDLQDependencyAnalysisController_Process_AssociationLookupErrorRetries(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), deadLetteredBatchID).Return(entity.Batch{
		ID: deadLetteredBatchID, Queue: "q", Contains: []string{carriedRequestID},
		State: entity.BatchStateCreating, Version: 1,
	}, nil)

	requestBatchStore := storagemock.NewMockRequestBatchStore(ctrl)
	requestBatchStore.EXPECT().GetByRequestID(gomock.Any(), carriedRequestID).Return(nil, errors.New("store unavailable"))

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(requestBatchStore).AnyTimes()
	c := newDependencyAnalysisController(t, store, consumer.TopicRegistry{})

	require.Error(t, c.Process(context.Background(), newMockDelivery(ctrl, dependencyAnalysisPayload(t, deadLetteredBatchID))))
}

func TestDLQDependencyAnalysisController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newDependencyAnalysisController(t, orchstoragemock.NewMockStorage(ctrl), consumer.TopicRegistry{})

	require.Error(t, c.Process(context.Background(), newMockDelivery(ctrl, []byte("garbage"))))
}

func TestDLQDependencyAnalysisController_Process_EmptyIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newDependencyAnalysisController(t, orchstoragemock.NewMockStorage(ctrl), consumer.TopicRegistry{})

	require.Error(t, c.Process(context.Background(), newMockDelivery(ctrl, dependencyAnalysisPayload(t, ""))))
}

func TestDLQDependencyAnalysisController_Process_TenantPayloadQueueMismatchAcks(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newDependencyAnalysisController(t, orchstoragemock.NewMockStorage(ctrl), consumer.TopicRegistry{})

	require.NoError(t, c.Process(context.Background(), newMockDeliveryWithTenant(ctrl, dependencyAnalysisPayload(t, deadLetteredBatchID), "other-queue")))
}
