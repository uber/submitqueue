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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/change"
	"github.com/uber/submitqueue/platform/base/mergestrategy"
	"github.com/uber/submitqueue/platform/consumer"
	sqmq "github.com/uber/submitqueue/submitqueue/core/messagequeue"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	orchstorage "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage"
	orchstoragemock "github.com/uber/submitqueue/submitqueue/orchestrator/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQStartController_ReconcilesOnlyError(t *testing.T) {
	tests := []struct {
		name          string
		state         entity.RequestState
		batchState    entity.BatchState
		queueMismatch bool
		tenant        string
		wantError     bool
		wantFailed    bool
	}{
		{name: "missing", wantFailed: true},
		{name: "started", state: entity.RequestStateStarted, wantFailed: true},
		{name: "validated", state: entity.RequestStateValidated, wantFailed: true},
		{name: "cancelling", state: entity.RequestStateCancelling, wantFailed: true},
		{name: "unowned batched", state: entity.RequestStateBatched, wantFailed: true},
		{name: "unowned processing", state: entity.RequestStateProcessing, wantFailed: true},
		{name: "batch owns outcome", state: entity.RequestStateBatched, batchState: entity.BatchStateSpeculating},
		{name: "succeeded batch awaits conclude", state: entity.RequestStateBatched, batchState: entity.BatchStateSucceeded},
		{name: "cancelled batch awaits conclude", state: entity.RequestStateBatched, batchState: entity.BatchStateCancelled},
		{name: "failed batch permits error", state: entity.RequestStateBatched, batchState: entity.BatchStateFailed, wantFailed: true},
		{name: "request error already won", state: entity.RequestStateError, batchState: entity.BatchStateSucceeded, wantFailed: true},
		{name: "already error", state: entity.RequestStateError, wantFailed: true},
		{name: "landed winner", state: entity.RequestStateLanded},
		{name: "cancelled winner", state: entity.RequestStateCancelled},
		{name: "payload tenant mismatch", state: entity.RequestStateStarted, tenant: "other-queue"},
		{name: "different authoritative queue", state: entity.RequestStateStarted, queueMismatch: true, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			landRequest := entity.LandRequest{ID: "1", Queue: "q", Change: change.Change{URIs: []string{"uri/one", "uri/two"}}, LandStrategy: mergestrategy.MergeStrategyRebase}
			var persisted entity.Request
			if tt.state != entity.RequestStateUnknown {
				persisted = entity.Request{ID: landRequest.ID, Queue: landRequest.Queue, Change: landRequest.Change, LandStrategy: landRequest.LandStrategy, State: tt.state, Version: 7}
			}
			if tt.queueMismatch {
				persisted.Queue = "other-queue"
			}
			before := persisted
			writes := 0
			requests := storagemock.NewMockRequestStore(ctrl)
			requests.EXPECT().Get(gomock.Any(), landRequest.ID).DoAndReturn(func(context.Context, string) (entity.Request, error) {
				if persisted.ID == "" {
					return entity.Request{}, storage.ErrNotFound
				}
				return persisted, nil
			}).AnyTimes()
			requests.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request, oldVersion, newVersion int32) error {
				writes++
				assert.Equal(t, persisted.Version, oldVersion)
				assert.Equal(t, oldVersion+1, newVersion)
				assert.Equal(t, oldVersion, request.Version)
				persisted = request
				persisted.Version = newVersion
				return nil
			}).AnyTimes()
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			associations := storagemock.NewMockRequestBatchStore(ctrl)
			var memberships []entity.RequestBatch
			if tt.batchState != entity.BatchStateUnknown {
				memberships = []entity.RequestBatch{{RequestID: landRequest.ID, BatchID: "batch"}}
				batches := storagemock.NewMockBatchStore(ctrl)
				batches.EXPECT().Get(gomock.Any(), "batch").Return(entity.Batch{ID: "batch", State: tt.batchState}, nil).AnyTimes()
				store.EXPECT().GetBatchStore().Return(batches).AnyTimes()
			}
			associations.EXPECT().GetByRequestID(gomock.Any(), landRequest.ID).Return(memberships, nil).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
			publishCount := 0
			if tt.wantFailed {
				publishCount = 2
			}
			var logs []entity.RequestLog
			registry := newTestLogRegistry(t, ctrl, publishCount, func(log entity.RequestLog) error {
				if tt.state == entity.RequestStateUnknown {
					assert.Equal(t, entity.Request{}, persisted)
				} else {
					assert.Equal(t, entity.RequestStateError, persisted.State)
				}
				assert.Equal(t, landRequest.Queue, log.Queue)
				assert.Equal(t, landRequest.ID, log.RequestID)
				assert.Equal(t, persisted.Version, log.RequestVersion)
				assert.Equal(t, entity.RequestStatusError, log.Status)
				logs = append(logs, log)
				return nil
			})
			scope := tally.NewTestScope("", nil)
			controller := NewDLQStartController(zaptest.NewLogger(t).Sugar(), scope, staticStorageFactory{store}, registry, TopicKey(topickey.TopicKeyStart), "orchestrator-dlq")
			payload, err := sqmq.Marshal(sqmq.StartFromLandRequest(landRequest))
			require.NoError(t, err)
			tenant := landRequest.Queue
			if tt.tenant != "" {
				tenant = tt.tenant
			}
			for range 2 {
				err := controller.Process(context.Background(), newMockDeliveryWithTenant(ctrl, payload, tenant))
				if tt.wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			counters := scope.Snapshot().Counters()
			if tt.wantError {
				require.Contains(t, counters, "start_dlq.process.reconcile_errors+")
				assert.EqualValues(t, 2, counters["start_dlq.process.reconcile_errors+"].Value())
			} else {
				assert.NotContains(t, counters, "start_dlq.process.reconcile_errors+")
			}
			if !tt.wantFailed {
				assert.Equal(t, before, persisted)
				assert.Zero(t, writes)
				return
			}
			if tt.state == entity.RequestStateUnknown {
				assert.Equal(t, entity.Request{}, persisted)
				assert.Zero(t, writes)
				assert.Zero(t, logs[0].RequestVersion)
				assert.Equal(t, "boom", logs[0].LastError)
				return
			}
			assert.Equal(t, landRequest.Change, persisted.Change)
			assert.Equal(t, landRequest.LandStrategy, persisted.LandStrategy)
			if tt.state == entity.RequestStateError {
				assert.Zero(t, writes)
			} else {
				assert.Equal(t, 1, writes)
				assert.Equal(t, "boom", logs[0].LastError)
			}
		})
	}
}

func TestDLQStartController_RetryRepairsFailedWritesAndPublication(t *testing.T) {
	for _, fault := range []string{"resolve", "read", "update", "publish", "concurrent batch claim", "concurrent terminal winner"} {
		t.Run(fault, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			landRequest := entity.LandRequest{ID: "1", Queue: "q", Change: change.Change{URIs: []string{"uri/one"}}}
			initial := entity.Request{ID: "1", Queue: "q", Change: landRequest.Change, State: entity.RequestStateStarted, Version: 1}
			var persisted entity.Request
			if fault == "update" || fault == "concurrent batch claim" || fault == "concurrent terminal winner" {
				persisted = initial
			}
			dependencyError := errors.New("dependency failed")
			firstAttempt := true
			requests := storagemock.NewMockRequestStore(ctrl)
			requests.EXPECT().Get(gomock.Any(), "1").DoAndReturn(func(context.Context, string) (entity.Request, error) {
				if firstAttempt && fault == "read" {
					return entity.Request{}, dependencyError
				}
				if persisted.ID == "" {
					return entity.Request{}, storage.ErrNotFound
				}
				return persisted, nil
			}).AnyTimes()
			requests.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request entity.Request, oldVersion, newVersion int32) error {
				if firstAttempt && fault == "update" {
					return dependencyError
				}
				if firstAttempt && (fault == "concurrent batch claim" || fault == "concurrent terminal winner") {
					persisted.State = entity.RequestStateBatched
					if fault == "concurrent terminal winner" {
						persisted.State = entity.RequestStateCancelled
					}
					persisted.Version++
					return storage.ErrVersionMismatch
				}
				require.Equal(t, persisted.Version, oldVersion)
				persisted = request
				persisted.Version = newVersion
				return nil
			}).AnyTimes()
			store := orchstoragemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(requests).AnyTimes()
			associations := storagemock.NewMockRequestBatchStore(ctrl)
			associations.EXPECT().GetByRequestID(gomock.Any(), "1").DoAndReturn(func(context.Context, string) ([]entity.RequestBatch, error) {
				if persisted.State == entity.RequestStateBatched {
					return []entity.RequestBatch{{RequestID: "1", BatchID: "batch"}}, nil
				}
				return nil, nil
			}).AnyTimes()
			store.EXPECT().GetRequestBatchStore().Return(associations).AnyTimes()
			batches := storagemock.NewMockBatchStore(ctrl)
			batches.EXPECT().Get(gomock.Any(), "batch").Return(entity.Batch{ID: "batch", State: entity.BatchStateSpeculating}, nil).AnyTimes()
			store.EXPECT().GetBatchStore().Return(batches).AnyTimes()
			count := 1
			if fault == "publish" {
				count = 2
			}
			if fault == "concurrent batch claim" || fault == "concurrent terminal winner" {
				count = 0
			}
			registry := newTestLogRegistry(t, ctrl, count, func(log entity.RequestLog) error {
				if persisted.ID != "" {
					assert.Equal(t, entity.RequestStateError, persisted.State)
				}
				assert.Equal(t, entity.RequestStatusError, log.Status)
				assert.Equal(t, persisted.Version, log.RequestVersion)
				if firstAttempt && fault == "publish" {
					return dependencyError
				}
				return nil
			})
			stores := orchstoragemock.NewMockFactory(ctrl)
			stores.EXPECT().For(orchstorage.Config{QueueName: "q"}).DoAndReturn(func(orchstorage.Config) (orchstorage.Storage, error) {
				if firstAttempt && fault == "resolve" {
					return nil, dependencyError
				}
				return store, nil
			}).AnyTimes()
			scope := tally.NewTestScope("", nil)
			controller := NewDLQStartController(zaptest.NewLogger(t).Sugar(), scope, stores, registry, TopicKey(topickey.TopicKeyStart), "orchestrator-dlq")
			payload, err := sqmq.Marshal(sqmq.StartFromLandRequest(landRequest))
			require.NoError(t, err)
			err = controller.Process(context.Background(), newMockDelivery(ctrl, payload))
			require.Error(t, err)
			require.Contains(t, scope.Snapshot().Counters(), "start_dlq.process.reconcile_errors+")
			assert.EqualValues(t, 1, scope.Snapshot().Counters()["start_dlq.process.reconcile_errors+"].Value())
			firstAttempt = false
			require.NoError(t, controller.Process(context.Background(), newMockDelivery(ctrl, payload)))
			assert.EqualValues(t, 1, scope.Snapshot().Counters()["start_dlq.process.reconcile_errors+"].Value())
			wantState := entity.RequestStateError
			if fault == "resolve" || fault == "read" || fault == "publish" {
				wantState = entity.RequestStateUnknown
			}
			if fault == "concurrent batch claim" {
				wantState = entity.RequestStateBatched
			}
			if fault == "concurrent terminal winner" {
				wantState = entity.RequestStateCancelled
			}
			assert.Equal(t, wantState, persisted.State)
			if wantState == entity.RequestStateUnknown {
				assert.Equal(t, entity.Request{}, persisted)
			} else {
				assert.Equal(t, landRequest.Change, persisted.Change)
			}
		})
	}
}

func TestDLQStartController_RejectsMalformedPayload(t *testing.T) {
	for _, payload := range []string{"not json", `{"queue":"q","change":{"uris":["uri/one"]}}`, `{"id":1,"queue":"q"}`, `{"id":"1","id":"2","queue":"q"}`, `{"id":"1","queue":"q","change":false}`} {
		t.Run(payload, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			scope := tally.NewTestScope("", nil)
			controller := NewDLQStartController(zaptest.NewLogger(t).Sugar(), scope, staticStorageFactory{orchstoragemock.NewMockStorage(ctrl)}, consumer.TopicRegistry{}, TopicKey(topickey.TopicKeyStart), "orchestrator-dlq")
			require.Error(t, controller.Process(context.Background(), newMockDelivery(ctrl, []byte(payload))))
			require.Contains(t, scope.Snapshot().Counters(), "start_dlq.process.reconcile_errors+")
			assert.EqualValues(t, 1, scope.Snapshot().Counters()["start_dlq.process.reconcile_errors+"].Value())
		})
	}
}
