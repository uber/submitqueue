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

package dependencyanalysis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/conflict"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/all"
	conflictmock "github.com/uber/submitqueue/submitqueue/extension/conflict/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

const (
	testQueue     = "test-queue"
	testRequestID = "test-queue/123"
	testBatchID   = "test-queue/batch/3"
)

// analyzerCfg is the per-queue identity handed to the conflict analyzer in
// cases that do not exercise per-queue routing.
var analyzerCfg = conflict.Config{QueueName: testQueue}

// testBatch returns the batch under analysis, as the batch stage leaves it.
func testBatch() entity.Batch {
	return entity.Batch{
		ID:           testBatchID,
		Queue:        testQueue,
		Contains:     []string{testRequestID},
		Dependencies: []string{},
		State:        entity.BatchStateCreating,
		Version:      1,
	}
}

// liveRequest returns the batch's member request in a state that does not halt
// promotion.
func liveRequest() entity.Request {
	return entity.Request{
		ID:      testRequestID,
		Queue:   testQueue,
		State:   entity.RequestStateBatched,
		Version: 2,
	}
}

func batchIDPayload(t *testing.T, id, queue string) []byte {
	t.Helper()
	payload, err := entity.BatchID{ID: id, Queue: queue}.ToBytes()
	require.NoError(t, err)
	return payload
}

// newQueueBatchStateStore accepts any record write and lists the given batches
// as membership records under their current state. Callers hydrating candidates
// must set up the corresponding BatchStore.Get expectations themselves.
func newQueueBatchStateStore(ctrl *gomock.Controller, active ...entity.Batch) *storagemock.MockQueueBatchStateStore {
	s := storagemock.NewMockQueueBatchStateStore(ctrl)
	s.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.EXPECT().List(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, state entity.BatchState) ([]entity.QueueBatchState, error) {
			var records []entity.QueueBatchState
			for _, b := range active {
				if b.State == state {
					records = append(records, entity.QueueBatchState{Queue: b.Queue, State: state, BatchID: b.ID})
				}
			}
			return records, nil
		},
	).AnyTimes()
	return s
}

func storageFactoryFor(ctrl *gomock.Controller, store storage.Storage) *storagemock.MockFactory {
	f := storagemock.NewMockFactory(ctrl)
	f.EXPECT().For(gomock.Any()).Return(store, nil).AnyTimes()
	return f
}

// newTestController builds a controller over the given store. A nil analyzer
// resolves to the "all" analyzer, under which every dependency-eligible batch
// conflicts.
func newTestController(t *testing.T, ctrl *gomock.Controller, store storage.Storage, analyzer conflict.Analyzer, publisher *queuemock.MockPublisher) *Controller {
	t.Helper()

	if analyzer == nil {
		analyzer = all.New(analyzerCfg)
	}
	analyzerFactory := conflictmock.NewMockFactory(ctrl)
	analyzerFactory.EXPECT().For(gomock.Any()).Return(analyzer, nil).AnyTimes()

	if publisher == nil {
		publisher = queuemock.NewMockPublisher(ctrl)
		publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	}
	queue := queuemock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(publisher).AnyTimes()
	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeySpeculate, Name: "speculate", Queue: queue},
		{Key: topickey.TopicKeyLog, Name: "log", Queue: queue},
	})
	require.NoError(t, err)

	return NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, storageFactoryFor(ctrl, store),
		analyzerFactory, registry, topickey.TopicKeyDependencyAnalysis, "orchestrator-dependency-analysis")
}

func newDelivery(t *testing.T, ctrl *gomock.Controller, id, queue string) *consumermock.MockDelivery {
	t.Helper()

	msg := entityqueue.NewMessage(id, batchIDPayload(t, id, queue), queue, nil)
	delivery := consumermock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()
	return delivery
}

// requestStoreFor returns a request store that answers with the given requests,
// keyed by ID.
func requestStoreFor(ctrl *gomock.Controller, requests ...entity.Request) *storagemock.MockRequestStore {
	s := storagemock.NewMockRequestStore(ctrl)
	for _, request := range requests {
		s.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()
	}
	s.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s
}

// noPriorEnrollment answers the dedup lookup with nothing and accepts the
// association write, i.e. this batch is the first one through for its request.
func noPriorEnrollment(ctrl *gomock.Controller) *storagemock.MockRequestBatchStore {
	s := storagemock.NewMockRequestBatchStore(ctrl)
	s.EXPECT().GetByRequestID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return s
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	controller := newTestController(t, ctrl, store, nil, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyDependencyAnalysis, controller.TopicKey())
	assert.Equal(t, "orchestrator-dependency-analysis", controller.ConsumerGroup())
	assert.Equal(t, "dependency-analysis", controller.Name())

	var _ consumer.Controller = controller
}

// The whole point of the stage: resolve what the batch must serialize behind,
// record it on both sides of the graph, and promote it to Created.
func TestController_Process_AnalyzesAndTransitionsToCreated(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	inFlight := []entity.Batch{
		{ID: "test-queue/batch/1", Queue: testQueue, State: entity.BatchStateCreated, Version: 1},
		{ID: "test-queue/batch/2", Queue: testQueue, State: entity.BatchStateSpeculating, Version: 2},
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), inFlight[0].ID).Return(inFlight[0], nil)
	batchStore.EXPECT().Get(gomock.Any(), inFlight[1].ID).Return(inFlight[1], nil)
	batchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           batch.ID,
		Queue:        testQueue,
		Contains:     []string{testRequestID},
		Dependencies: []string{inFlight[0].ID, inFlight[1].ID},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), entity.BatchDependent{
		BatchID:    batch.ID,
		Dependents: []string{},
		Version:    1,
	}).Return(nil)
	dependentStore.EXPECT().Get(gomock.Any(), inFlight[0].ID).Return(entity.BatchDependent{
		BatchID: inFlight[0].ID,
		Version: 1,
	}, nil)
	dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    inFlight[0].ID,
		Dependents: []string{batch.ID},
		Version:    1,
	}, int32(1), int32(2)).Return(nil)
	dependentStore.EXPECT().Get(gomock.Any(), inFlight[1].ID).Return(entity.BatchDependent{
		BatchID:    inFlight[1].ID,
		Dependents: []string{"test-queue/batch/99"},
		Version:    2,
	}, nil)
	dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    inFlight[1].ID,
		Dependents: []string{"test-queue/batch/99", batch.ID},
		Version:    2,
	}, int32(2), int32(3)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, inFlight...)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// The analyzer may report one in-flight batch several times when more than one
// conflict type applies; the dependency graph only tracks the relation.
func TestController_Process_DedupesAnalyzerConflicts(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	inFlight := entity.Batch{ID: "test-queue/batch/2", Queue: testQueue, State: entity.BatchStateSpeculating, Version: 2}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(inFlight, nil)
	batchStore.EXPECT().Update(gomock.Any(), entity.Batch{
		ID:           batch.ID,
		Queue:        testQueue,
		Contains:     []string{testRequestID},
		Dependencies: []string{inFlight.ID},
		State:        entity.BatchStateCreated,
		Version:      1,
	}, int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	dependentStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(entity.BatchDependent{
		BatchID: inFlight.ID,
		Version: 5,
	}, nil)
	dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    inFlight.ID,
		Dependents: []string{batch.ID},
		Version:    5,
	}, int32(5), int32(6)).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, inFlight)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	analyzer := conflictmock.NewMockAnalyzer(ctrl)
	analyzer.EXPECT().Analyze(gomock.Any(), gomock.Any(), gomock.Any()).Return([]entity.Conflict{
		{BatchID: inFlight.ID, Type: entity.ConflictTypeConservative},
		{BatchID: inFlight.ID, Type: entity.ConflictTypeTargetOverlap},
	}, nil)

	controller := newTestController(t, ctrl, store, analyzer, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// A redelivery whose previous attempt transitioned but lost its announcement:
// re-announcing is the whole job, and re-analyzing would corrupt the graph.
func TestController_Process_RedeliveryAfterTransitionOnlyRepublishes(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	batch.State = entity.BatchStateCreated
	batch.Dependencies = []string{"test-queue/batch/1"}
	batch.Version = 2

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	// Dependent store and request store with no EXPECTs — must not be touched.
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(storagemock.NewMockBatchDependentStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil).AnyTimes()
	publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).Return(nil)

	controller := newTestController(t, ctrl, store, nil, publisher)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// A failure part-way through the index loop leaves the batch in Creating, so
// the retry re-enters the loop over dependencies it may already have written.
func TestController_Process_RedeliveryMidIndexDoesNotDoubleAppend(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	inFlight := entity.Batch{ID: "test-queue/batch/1", Queue: testQueue, State: entity.BatchStateCreated, Version: 1}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(inFlight, nil)
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	// The first pass already listed this batch; Update must not be called again.
	dependentStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(entity.BatchDependent{
		BatchID:    inFlight.ID,
		Dependents: []string{batch.ID},
		Version:    2,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, inFlight)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

func TestController_Process_HaltedRequestIsNotPromoted(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateCancelling,
		entity.RequestStateCancelled,
		entity.RequestStateLanded,
		entity.RequestStateError,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			request := liveRequest()
			request.State = state

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

			// Dependent store with no EXPECTs — must not be touched.
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetBatchDependentStore().Return(storagemock.NewMockBatchDependentStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, request)).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

			// Publisher with no EXPECTs — must not be called.
			controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
			require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
		})
	}
}

func TestController_Process_HaltedBatchAcksWithoutPublishing(t *testing.T) {
	for _, state := range []entity.BatchState{
		entity.BatchStateCancelling,
		entity.BatchStateCancelled,
		entity.BatchStateFailed,
		entity.BatchStateSucceeded,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			batch.State = state

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

			// Publisher with no EXPECTs — must not be called.
			controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
			require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
		})
	}
}

// A batch already admitted got its announcement; re-sending one would only buy
// a redundant re-plan.
func TestController_Process_AlreadyAdmittedAcksWithoutPublishing(t *testing.T) {
	for _, state := range []entity.BatchState{entity.BatchStateSpeculating, entity.BatchStateLanding} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batch := testBatch()
			batch.State = state

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

			controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
			require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
		})
	}
}

func TestController_Process_QueueMismatchRejected(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
	require.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, "some-other-queue")))
}

// The announced batch ID carries its queue, and the message is partitioned by
// queue so speculate stays serial per queue.
func TestController_Process_StampsQueueOnAnnouncement(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	var announced []entityqueue.Message
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), "log", gomock.Any()).Return(nil).AnyTimes()
	publisher.EXPECT().Publish(gomock.Any(), "speculate", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, msg entityqueue.Message) error {
			announced = append(announced, msg)
			return nil
		},
	)

	controller := newTestController(t, ctrl, store, nil, publisher)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))

	require.Len(t, announced, 1)
	bid, err := entity.BatchIDFromBytes(announced[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, batch.ID, bid.ID)
	assert.Equal(t, testQueue, bid.Queue)
	assert.Equal(t, testQueue, announced[0].PartitionKey)
}

func TestController_Process_AnalyzerFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	analyzer := conflictmock.NewMockAnalyzer(ctrl)
	analyzer.EXPECT().Analyze(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("analyzer down"))

	controller := newTestController(t, ctrl, store, analyzer, nil)
	require.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// A lost Update must leave the caller's copy of Dependents untouched, or the
// next attempt would append onto a slice that already grew.
func TestController_Process_IndexUpdateFailureDoesNotMutateFetchedDependents(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	inFlight := entity.Batch{ID: "test-queue/batch/1", Queue: testQueue, State: entity.BatchStateCreated, Version: 1}

	dependents := make([]string, 1, 2)
	dependents[0] = "test-queue/batch/98"
	existing := entity.BatchDependent{BatchID: inFlight.ID, Dependents: dependents, Version: 4}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(inFlight, nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	dependentStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(existing, nil)
	dependentStore.EXPECT().Update(gomock.Any(), entity.BatchDependent{
		BatchID:    inFlight.ID,
		Dependents: []string{"test-queue/batch/98", batch.ID},
		Version:    existing.Version,
	}, existing.Version, existing.Version+1).Return(errors.New("update failed"))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, inFlight)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, nil)
	require.Error(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
	assert.Equal(t, "", dependents[:cap(dependents)][1])
	assert.Equal(t, int32(4), existing.Version)
}

func TestController_Process_PromotionErrors(t *testing.T) {
	batch := testBatch()
	dependencyID := "test-queue/batch/1"
	inFlight := entity.Batch{ID: dependencyID, Queue: testQueue, State: entity.BatchStateCreated, Version: 1}
	storeErr := errors.New("storage failed")

	tests := map[string]struct {
		setup  func(*storagemock.MockBatchStore, *storagemock.MockBatchDependentStore)
		errMsg string
	}{
		"own reverse index create fails": {
			setup: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storeErr)
			},
			errMsg: "failed to create batch dependent index",
		},
		"dependency get fails": {
			setup: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), dependencyID).Return(entity.BatchDependent{}, storeErr)
			},
			errMsg: "failed to get batch dependent",
		},
		"dependency update fails": {
			setup: func(_ *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), dependencyID).Return(entity.BatchDependent{
					BatchID: dependencyID,
					Version: 2,
				}, nil)
				dependentStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(storeErr)
			},
			errMsg: "failed to update batch dependent index",
		},
		"created transition fails": {
			setup: func(batchStore *storagemock.MockBatchStore, dependentStore *storagemock.MockBatchDependentStore) {
				dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				dependentStore.EXPECT().Get(gomock.Any(), dependencyID).Return(entity.BatchDependent{
					BatchID: dependencyID,
					Version: 2,
				}, nil)
				dependentStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(nil)
				batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storeErr)
			},
			errMsg: "failed to mark batch",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			batchStore := storagemock.NewMockBatchStore(ctrl)
			batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
			batchStore.EXPECT().Get(gomock.Any(), inFlight.ID).Return(inFlight, nil)
			dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
			tt.setup(batchStore, dependentStore)

			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl, inFlight)).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
			store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
			store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

			controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
			err := controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue))
			assert.ErrorContains(t, err, tt.errMsg)
		})
	}
}

// "batched" is reported from here, not from batch creation: until the batch is
// Created it has no dependency set and nothing in the queue can see it.
func TestController_Process_PublishesBatchedLogOnPromotion(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()

	var logs []entity.RequestLog
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			if topic == "log" {
				entry, err := entity.RequestLogFromBytes(msg.Payload)
				require.NoError(t, err)
				logs = append(logs, entry)
			}
			return nil
		},
	).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, publisher)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))

	require.Len(t, logs, 1)
	assert.Equal(t, testRequestID, logs[0].RequestID)
	assert.Equal(t, entity.RequestStatusBatched, logs[0].Status)
	assert.Equal(t, batch.ID, logs[0].Metadata["batch_id"])
}

// The batch stage mints one batch per delivery, so a lost ack leaves two. The
// first through here enrols the request; the second must stop, or the same
// change ends up in two live batches.
func TestController_Process_AbandonsBatchWhoseRequestIsAlreadyEnrolled(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	winner := entity.Batch{
		ID: "test-queue/batch/2", Queue: testQueue, Contains: []string{testRequestID},
		State: entity.BatchStateSpeculating, Version: 3,
	}

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Get(gomock.Any(), winner.ID).Return(winner, nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return([]entity.RequestBatch{
		{RequestID: testRequestID, BatchID: winner.ID, Version: 1},
	}, nil)

	// Dependent store and request store with no EXPECTs — neither may be touched.
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(storagemock.NewMockBatchDependentStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(storagemock.NewMockRequestStore(ctrl)).AnyTimes()

	// Publisher with no EXPECTs — an abandoned batch announces nothing.
	controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// The batch's own association must not read as someone else's enrolment.
func TestController_Process_OwnAssociationDoesNotBlockPromotion(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil).AnyTimes()
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return([]entity.RequestBatch{
		{RequestID: testRequestID, BatchID: batch.ID, Version: 1},
	}, nil)
	associations.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStoreFor(ctrl, liveRequest())).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// Promotion writes the association and claims the request.
func TestController_Process_EnrolsTheRequest(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	request := liveRequest()
	request.State = entity.RequestStateValidated

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	batchStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return(nil, nil)
	associations.EXPECT().Create(gomock.Any(), entity.RequestBatch{
		RequestID: testRequestID, BatchID: batch.ID, Version: 1,
	}).Return(nil)

	claimed := request
	claimed.State = entity.RequestStateBatched
	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), testRequestID).Return(request, nil).AnyTimes()
	requestStore.EXPECT().Update(gomock.Any(), claimed, request.Version, request.Version+1).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// Cancel reached the request first. The batch is left in Creating and the
// delivery is acked: retrying would not change the answer.
func TestController_Process_ClaimLostToCancelAbandonsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// Update must NOT be called — the batch stays in Creating.

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return(nil, nil)
	associations.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), testRequestID).Return(liveRequest(), nil).AnyTimes()
	requestStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("cas: %w", storage.ErrVersionMismatch))

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	// Publisher with no EXPECTs — an abandoned batch announces nothing.
	controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// Cancel ran to completion while analysis was resolving dependencies, so the
// claim's re-read finds the request halted at a version cancel itself wrote.
// Comparing versions alone would compare equal and write Batched straight over
// Cancelled, reviving a request the user gave up on.
func TestController_Process_RequestCancelledDuringAnalysisAbandonsBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()

	live := liveRequest()
	cancelled := liveRequest()
	cancelled.State = entity.RequestStateCancelled
	// Two CASes: cancel records intent, then writes the terminal state.
	cancelled.Version = live.Version + 2

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)
	// Update must NOT be called — the batch stays in Creating.

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	gomock.InOrder(
		requestStore.EXPECT().Get(gomock.Any(), testRequestID).Return(live, nil),
		requestStore.EXPECT().Get(gomock.Any(), testRequestID).Return(cancelled, nil),
	)
	// Update must NOT be called — claiming would overwrite the cancellation.

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(noPriorEnrollment(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	// Publisher with no EXPECTs — an abandoned batch announces nothing.
	controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
	require.NoError(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)))
}

// Any claim failure other than a lost race is retryable and must nack.
func TestController_Process_ClaimStorageErrorPropagates(t *testing.T) {
	ctrl := gomock.NewController(t)

	batch := testBatch()
	claimErr := errors.New("db connection lost")

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), batch.ID).Return(batch, nil)

	dependentStore := storagemock.NewMockBatchDependentStore(ctrl)
	dependentStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	associations := storagemock.NewMockRequestBatchStore(ctrl)
	associations.EXPECT().GetByRequestID(gomock.Any(), testRequestID).Return(nil, nil)
	associations.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), testRequestID).Return(liveRequest(), nil).AnyTimes()
	requestStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(claimErr)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetBatchDependentStore().Return(dependentStore).AnyTimes()
	store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	controller := newTestController(t, ctrl, store, nil, queuemock.NewMockPublisher(ctrl))
	assert.ErrorIs(t, controller.Process(context.Background(), newDelivery(t, ctrl, batch.ID, testQueue)), claimErr)
}
