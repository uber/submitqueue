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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/base/failure"
	queue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstoragemock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQRequestController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyValidate), TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	assert.Equal(t, "validate_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("validate_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-validate-dlq", c.ConsumerGroup())
}

func TestDLQRequestController_Process_LandRequestPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "1", Queue: "q", Version: 1, State: entity.RequestStateStarted,
	}
	requestStore.EXPECT().Get(gomock.Any(), "1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, registry, DecodeLandRequestID, TopicKey(topickey.TopicKeyStart), "orchestrator-start-dlq")

	payload, err := sqmq.Marshal(sqmq.StartFromLandRequest(entity.LandRequest{ID: "1", Queue: "q"}))
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQRequestController_Process_TenantPayloadQueueMismatchAcks(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := orchstoragemock.NewMockStorage(ctrl)
	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyValidate), TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	payload, err := sqmq.MarshalID(sqmq.TopicKeyValidate, "1", "q")
	require.NoError(t, err)

	require.NoError(t, c.Process(context.Background(), newMockDeliveryWithTenant(ctrl, payload, "other-queue")))
}

func TestDLQRequestController_Process_RequestIDPayload(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "3", Queue: "q", Version: 1, State: entity.RequestStateValidated,
	}
	requestStore.EXPECT().Get(gomock.Any(), "3").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(log entity.RequestLog) error {
		assert.Equal(t, "boom", log.LastError)
		return nil
	})

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, registry, DecodeRequestID(topickey.TopicKeyBatch), TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

	payload, err := sqmq.MarshalID(sqmq.TopicKeyBatch, "3", "q")
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQCancelController_WaitsThenReconcilesError(t *testing.T) {
	for _, claim := range []bool{false, true} {
		name := "unowned request fails"
		if claim {
			name = "batch claim after ownership lookup"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			var persisted entity.Request
			requests := storagemock.NewMockRequestStore(ctrl)
			requests.EXPECT().Get(gomock.Any(), "1").DoAndReturn(func(context.Context, string) (entity.Request, error) {
				if persisted.ID == "" {
					return entity.Request{}, storage.ErrNotFound
				}
				return persisted, nil
			}).AnyTimes()
			requests.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request, oldVersion, newVersion int32) error {
				if oldVersion != persisted.Version {
					return storage.ErrVersionMismatch
				}
				assert.Equal(t, oldVersion, request.Version)
				assert.Equal(t, oldVersion+1, newVersion)
				persisted = request
				persisted.Version = newVersion
				return nil
			}).Times(1)
			associations := storagemock.NewMockRequestBatchStore(ctrl)
			associations.EXPECT().GetByRequestID(gomock.Any(), "1").DoAndReturn(func(context.Context, string) ([]entity.RequestBatch, error) {
				if claim && persisted.State == entity.RequestStateValidated {
					persisted.State = entity.RequestStateBatched
					persisted.Version++
					return nil, nil
				}
				if claim && persisted.State == entity.RequestStateBatched {
					return []entity.RequestBatch{{RequestID: "1", BatchID: "batch"}}, nil
				}
				return nil, nil
			}).AnyTimes()
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
			logCount := 1
			if claim {
				logCount = 0
				batches := storagemock.NewMockBatchStore(ctrl)
				batches.EXPECT().Get(gomock.Any(), "batch").Return(entity.Batch{ID: "batch", State: entity.BatchStateCreated}, nil)
				store.EXPECT().GetBatchStore().Return(batches)
			}
			registry := newTestLogRegistry(t, ctrl, logCount, func(log entity.RequestLog) error {
				assert.Equal(t, entity.RequestStatusError, log.Status)
				assert.Equal(t, "1", log.RequestID)
				assert.Equal(t, int32(8), log.RequestVersion)
				assert.Equal(t, "boom", log.LastError)
				return nil
			})
			controller := NewDLQCancelController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store}, registry, TopicKey(topickey.TopicKeyCancel), "orchestrator-dlq")
			payload, err := sqmq.Marshal(sqmq.CancelFromEntity(entity.CancelRequest{ID: "1", Queue: "q", Reason: "user changed mind"}))
			require.NoError(t, err)
			delivery := newMockDelivery(ctrl, payload)
			delivery.EXPECT().Hold(waitForRequestDelayMs).Times(2)
			for range 2 {
				require.NoError(t, controller.Process(context.Background(), delivery))
			}
			persisted = entity.Request{ID: "1", Queue: "q", State: entity.RequestStateValidated, Version: 7}
			err = controller.Process(context.Background(), delivery)
			if claim {
				require.ErrorIs(t, err, storage.ErrVersionMismatch)
				require.NoError(t, controller.Process(context.Background(), delivery))
				assert.Equal(t, entity.RequestStateBatched, persisted.State)
			} else {
				require.NoError(t, err)
				assert.Equal(t, entity.RequestStateError, persisted.State)
			}
			assert.Equal(t, int32(8), persisted.Version)
		})
	}
}

func TestDLQCancelController_Process_TerminalOutcomes(t *testing.T) {
	for _, state := range []entity.RequestState{entity.RequestStateLanded, entity.RequestStateCancelled, entity.RequestStateError} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			request := entity.Request{ID: "1", Queue: "q", Version: 5, State: state}
			calls := 1
			logCount := 0
			if state == entity.RequestStateError {
				calls, logCount = 2, 2
			}
			requests := storagemock.NewMockRequestStore(ctrl)
			requests.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).Times(calls)
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()
			publishErr := errors.New("publisher unavailable")
			published := 0
			registry := newTestLogRegistry(t, ctrl, logCount, func(log entity.RequestLog) error {
				assert.Equal(t, entity.RequestStatusError, log.Status)
				assert.Equal(t, request.Version, log.RequestVersion)
				published++
				if published == 1 {
					return publishErr
				}
				return nil
			})
			controller := NewDLQCancelController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store}, registry, TopicKey(topickey.TopicKeyCancel), "orchestrator-cancel-dlq")
			payload, err := sqmq.Marshal(sqmq.CancelFromEntity(entity.CancelRequest{ID: request.ID, Queue: request.Queue}))
			require.NoError(t, err)
			delivery := newMockDelivery(ctrl, payload)
			if state == entity.RequestStateError {
				require.ErrorIs(t, controller.Process(context.Background(), delivery), publishErr)
			}
			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}

func TestDLQRequestController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()
	// no store calls expected

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyValidate), TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	delivery := newMockDelivery(ctrl, []byte("not json"))
	err := c.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestDLQRequestController_Process_EmptyIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noBatchAssociations(ctrl)).AnyTimes()
	// no store calls expected

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyValidate), TopicKey(topickey.TopicKeyValidate), "orchestrator-validate-dlq")

	payload, err := sqmq.MarshalID(sqmq.TopicKeyValidate, "", "q")
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	err = c.Process(context.Background(), delivery)
	require.Error(t, err)
}

// newMockDelivery returns a MockDelivery wired up enough to be passed through
// the DLQ controller Process flow.
func newMockDelivery(ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	return newMockDeliveryWithFailure(ctrl, payload, failure.Failure{}, false)
}

func newMockDeliveryWithTenant(ctrl *gomock.Controller, payload []byte, tenant string) *consumermock.MockDelivery {
	return newMockDeliveryWithTenantAndFailure(ctrl, payload, tenant, failure.Failure{}, false)
}

func newMockDeliveryWithFailure(ctrl *gomock.Controller, payload []byte, f failure.Failure, failed bool) *consumermock.MockDelivery {
	return newMockDeliveryWithTenantAndFailure(ctrl, payload, "q", f, failed)
}

func newMockDeliveryWithTenantAndFailure(ctrl *gomock.Controller, payload []byte, tenant string, f failure.Failure, failed bool) *consumermock.MockDelivery {
	msg := queue.NewMessage("dlq-msg-1", payload, "", nil)
	msg.Tenant = tenant
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	d.EXPECT().Metadata().Return(map[string]string{
		"dlq.original_topic": "validate",
		"dlq.failure_count":  "3",
		"dlq.last_error":     "boom",
	}).AnyTimes()
	d.EXPECT().Failure().Return(f, failed).AnyTimes()
	return d
}

// The dead letter can be a redundant batch attempt whose predecessor already
// enrolled the request. Failing it here would overwrite a live batch's claim
// and leave that batch building for a request reported as failed.
func TestDLQRequestController_Process_SkipsRequestOwnedByLiveBatch(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCreated,
		entity.BatchStateSpeculating,
		entity.BatchStateLanding,
		entity.BatchStateCancelling,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			// Request store with no EXPECTs — the request must not be touched.
			associations := storagemock.NewMockRequestBatchStore(ctrl)
			associations.EXPECT().GetByRequestID(gomock.Any(), "1").Return([]entity.RequestBatch{
				{RequestID: "1", BatchID: "1", Version: 1},
			}, nil)

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), "1").Return(entity.Batch{
				ID: "1", Queue: "q", Contains: []string{"1"}, State: state, Version: 1,
			}, nil)

			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()

			c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store},
				consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyBatch), TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

			payload, err := sqmq.MarshalID(sqmq.TopicKeyBatch, "1", "q")
			require.NoError(t, err)

			require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, payload)))
		})
	}
}

// A failed batch permits the same Error outcome to be reconciled.
func TestDLQRequestController_Process_FailsWhenAssociatedBatchFailed(t *testing.T) {
	ctrl := gomock.NewController(t)

	request := entity.Request{ID: "1", Queue: "q", Version: 1, State: entity.RequestStateBatched}
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), "1").Return([]entity.RequestBatch{
		{RequestID: "1", BatchID: "1", Version: 1},
	}, nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "1").Return(entity.Batch{
		ID: "1", Queue: "q", Contains: []string{"1"}, State: entity.BatchStateFailed, Version: 1,
	}, nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error { return nil })

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store},
		registry, DecodeRequestID(topickey.TopicKeyBatch), TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

	payload, err := sqmq.MarshalID(sqmq.TopicKeyBatch, "1", "q")
	require.NoError(t, err)

	require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, payload)))
}

// The association is written before the claim, so it outlives an attempt whose
// claim was abandoned. That batch stays in Creating forever with nothing to
// promote it, so it must not answer "owned" — doing so would suppress the
// reconcile and strand the request short of a terminal state.
func TestDLQRequestController_Process_FailsWhenCreatingBatchNeverClaimed(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateCancelling,
		entity.RequestStateValidated,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			request := entity.Request{ID: "1", Queue: "q", Version: 1, State: state}
			requestStore := storagemock.NewMockRequestStore(ctrl)
			requestStore.EXPECT().Get(gomock.Any(), "1").Return(request, nil).Times(2)
			requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

			associations := storagemock.NewMockRequestBatchStore(ctrl)
			associations.EXPECT().GetByRequestID(gomock.Any(), "1").Return([]entity.RequestBatch{
				{RequestID: "1", BatchID: "1", Version: 1},
			}, nil)

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), "1").Return(entity.Batch{
				ID: "1", Queue: "q", Contains: []string{"1"}, State: entity.BatchStateCreating, Version: 1,
			}, nil)

			registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error { return nil })

			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

			c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store},
				registry, DecodeRequestID(topickey.TopicKeyBatch), TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

			payload, err := sqmq.MarshalID(sqmq.TopicKeyBatch, "1", "q")
			require.NoError(t, err)

			require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, payload)))
		})
	}
}

// The same Creating batch mid-promotion: the claim landed, so the batch is on
// its way to Created and owns the request. Batched is written only by that
// claim, which is what separates this from the abandoned case above.
func TestDLQRequestController_Process_SkipsWhenCreatingBatchAlreadyClaimed(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "1").
		Return(entity.Request{ID: "1", Queue: "q", Version: 2, State: entity.RequestStateBatched}, nil)
	// Update must NOT be called — the batch owns the outcome.

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), "1").Return([]entity.RequestBatch{
		{RequestID: "1", BatchID: "1", Version: 1},
	}, nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "1").Return(entity.Batch{
		ID: "1", Queue: "q", Contains: []string{"1"}, State: entity.BatchStateCreating, Version: 1,
	}, nil)

	store := orchstoragemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQRequestController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store},
		consumer.TopicRegistry{}, DecodeRequestID(topickey.TopicKeyBatch), TopicKey(topickey.TopicKeyBatch), "orchestrator-batch-dlq")

	payload, err := sqmq.MarshalID(sqmq.TopicKeyBatch, "1", "q")
	require.NoError(t, err)

	require.NoError(t, c.Process(context.Background(), newMockDelivery(ctrl, payload)))
}
