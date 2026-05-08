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

package validate

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
	"github.com/uber/submitqueue/extension/changeprovider"
	changemock "github.com/uber/submitqueue/extension/changestore/mock"
	"github.com/uber/submitqueue/extension/mergechecker"
	mergecheckermock "github.com/uber/submitqueue/extension/mergechecker/mock"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	"github.com/uber/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

// requestIDPayload serializes a RequestID to JSON bytes for test message payloads.
func requestIDPayload(t *testing.T, id string) []byte {
	payload, err := entity.RequestID{ID: id}.ToBytes()
	require.NoError(t, err)
	return payload
}

// mockChangeProvider is a simple mock that returns test data.
type mockChangeProvider struct{}

func (m *mockChangeProvider) Get(ctx context.Context, change entity.Change) ([]changeprovider.ChangeInfo, error) {
	return []changeprovider.ChangeInfo{
		{
			URI: "github://org/repo/123/abc123",
			User: changeprovider.User{
				Name:  "Test User",
				Email: "test@example.com",
			},
			ChangedFiles: []changeprovider.ChangedFile{
				{Path: "main.go"},
			},
		},
	}, nil
}

// newMergeableMock returns a mock MergeChecker that always returns mergeable.
func newMergeableMock(ctrl *gomock.Controller) *mergecheckermock.MockMergeChecker {
	mc := mergecheckermock.NewMockMergeChecker(ctrl)
	mc.EXPECT().Check(gomock.Any(), gomock.Any(), gomock.Any()).Return(mergechecker.Result{Mergeable: true}, nil).AnyTimes()
	return mc
}

// newMockStorage creates a MockStorage with a MockRequestStore that returns the given request on Get.
// The returned MockRequestStore is exposed so individual tests can layer additional Get expectations.
func newMockStorage(ctrl *gomock.Controller, request entity.Request) (*storagemock.MockStorage, *storagemock.MockRequestStore) {
	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil).AnyTimes()

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()
	return store, mockReqStore
}

// newMockChangeStore creates a MockChangeStore with default no-overlap behavior.
// Tests that need to simulate overlap can override FindOverlapping with their own EXPECT.
// Validate is read-only against the change store — it never calls Create.
func newMockChangeStore(ctrl *gomock.Controller) *changemock.MockChangeStore {
	cs := changemock.NewMockChangeStore(ctrl)
	cs.EXPECT().FindOverlapping(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	return cs
}

// newTestController creates a controller with test dependencies.
func newTestController(
	t *testing.T,
	ctrl *gomock.Controller,
	store *storagemock.MockStorage,
	cs *changemock.MockChangeStore,
	mc mergechecker.MergeChecker,
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
		[]consumer.TopicConfig{{Key: consumer.TopicKeyBatch, Name: "batch", Queue: mockQ}},
	)
	require.NoError(t, err)

	cp := &mockChangeProvider{}

	return NewController(logger, scope, store, cs, registry, mc, cp, consumer.TopicKeyValidate, "orchestrator-validate")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)
	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	require.NotNil(t, controller)
	assert.Equal(t, consumer.TopicKeyValidate, controller.TopicKey())
	assert.Equal(t, "orchestrator-validate", controller.ConsumerGroup())
	assert.Equal(t, "validate", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abc123def"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := queue.NewMessage("test-queue/123", requestIDPayload(t, request.ID), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}

func TestController_Process_StorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	mockReqStore := storagemock.NewMockRequestStore(ctrl)
	mockReqStore.EXPECT().Get(gomock.Any(), "test-queue/123").Return(entity.Request{}, fmt.Errorf("db connection lost"))
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := queue.NewMessage("test-queue/123", requestIDPayload(t, "test-queue/123"), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_PublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/xyz789abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, fmt.Errorf("publish failed"))

	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	assert.Error(t, controller.Process(context.Background(), delivery))
}

func TestController_InterfaceImplementation(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)
	request := entity.Request{ID: "test-queue/123", Queue: "test-queue"}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	var _ consumer.Controller = controller
}

func TestController_Process_NotMergeable(t *testing.T) {
	ctrl := gomock.NewController(t)

	mc := mergecheckermock.NewMockMergeChecker(ctrl)
	mc.EXPECT().Check(gomock.Any(), gomock.Any(), gomock.Any()).Return(mergechecker.Result{Mergeable: false}, nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.True(t, errs.IsUserError(err))
}

func TestController_Process_MergeCheckError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mc := mergecheckermock.NewMockMergeChecker(ctrl)
	mc.EXPECT().Check(gomock.Any(), gomock.Any(), gomock.Any()).Return(mergechecker.Result{}, fmt.Errorf("merge check failed"))

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/repo/1/abc123"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestController_Process_DuplicateDetection(t *testing.T) {
	const (
		queueName     = "test-queue"
		newRequestID  = queueName + "/123"
		dupRequestID  = queueName + "/100"
		uri           = "github://uber/service/pull/1/abc"
		anotherReqID  = queueName + "/050"
		orphanReqID   = queueName + "/999"
		terminalReqID = queueName + "/200"
	)

	tests := []struct {
		name           string
		overlap        []entity.ChangeRecord
		ownerLookup    map[string]entity.Request
		ownerNotFound  map[string]bool
		ownerErr       map[string]error
		wantUserErr    bool
		wantUnexpected bool
	}{
		{
			name:    "no overlap proceeds to merge check",
			overlap: nil,
		},
		{
			name:    "overlap with live in-flight request returns user error",
			overlap: []entity.ChangeRecord{{URI: uri, RequestID: dupRequestID, Queue: queueName}},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateStarted, Version: 1},
			},
			wantUserErr: true,
		},
		{
			name:    "overlap with terminal owner is skipped",
			overlap: []entity.ChangeRecord{{URI: uri, RequestID: terminalReqID, Queue: queueName}},
			ownerLookup: map[string]entity.Request{
				terminalReqID: {ID: terminalReqID, Queue: queueName, State: entity.RequestStateLanded, Version: 5},
			},
		},
		{
			name:          "overlap with orphan owner (ErrNotFound) is skipped",
			overlap:       []entity.ChangeRecord{{URI: uri, RequestID: orphanReqID, Queue: queueName}},
			ownerNotFound: map[string]bool{orphanReqID: true},
		},
		{
			name: "multiple URIs same owner deduped to single Get call",
			overlap: []entity.ChangeRecord{
				{URI: uri, RequestID: dupRequestID, Queue: queueName},
				{URI: "github://uber/service/pull/2/def", RequestID: dupRequestID, Queue: queueName},
			},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateValidated, Version: 2},
			},
			wantUserErr: true,
		},
		{
			name: "first owner terminal then second live picks the live one",
			overlap: []entity.ChangeRecord{
				{URI: uri, RequestID: terminalReqID, Queue: queueName},
				{URI: "github://uber/service/pull/2/def", RequestID: anotherReqID, Queue: queueName},
			},
			ownerLookup: map[string]entity.Request{
				terminalReqID: {ID: terminalReqID, State: entity.RequestStateError, Version: 3},
				anotherReqID:  {ID: anotherReqID, State: entity.RequestStateProcessing, Version: 4},
			},
			wantUserErr: true,
		},
		{
			// Store doesn't exclude self; controller filters by RequestID and must not look up its own row.
			name: "self row in overlap is filtered (no Get call)",
			overlap: []entity.ChangeRecord{
				{URI: uri, RequestID: newRequestID, Queue: queueName},
			},
		},
		{
			name: "self row mixed with live other returns the other",
			overlap: []entity.ChangeRecord{
				{URI: uri, RequestID: newRequestID, Queue: queueName},
				{URI: uri, RequestID: dupRequestID, Queue: queueName},
			},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateStarted, Version: 1},
			},
			wantUserErr: true,
		},
		{
			name:    "owner lookup unexpected error propagates",
			overlap: []entity.ChangeRecord{{URI: uri, RequestID: dupRequestID, Queue: queueName}},
			ownerErr: map[string]error{
				dupRequestID: fmt.Errorf("db down"),
			},
			wantUnexpected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mc := newMergeableMock(ctrl)

			request := entity.Request{
				ID:           newRequestID,
				Queue:        queueName,
				Change:       entity.Change{URIs: []string{uri}},
				LandStrategy: entity.RequestLandStrategyRebase,
				State:        entity.RequestStateStarted,
				Version:      1,
			}

			mockReqStore := storagemock.NewMockRequestStore(ctrl)
			mockReqStore.EXPECT().Get(gomock.Any(), request.ID).Return(request, nil)
			for id, req := range tt.ownerLookup {
				mockReqStore.EXPECT().Get(gomock.Any(), id).Return(req, nil)
			}
			for id := range tt.ownerNotFound {
				mockReqStore.EXPECT().Get(gomock.Any(), id).Return(entity.Request{}, storage.WrapNotFound(fmt.Errorf("missing")))
			}
			for id, e := range tt.ownerErr {
				mockReqStore.EXPECT().Get(gomock.Any(), id).Return(entity.Request{}, e)
			}
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(mockReqStore).AnyTimes()

			cs := changemock.NewMockChangeStore(ctrl)
			cs.EXPECT().FindOverlapping(gomock.Any(), queueName, []string{uri}).Return(tt.overlap, nil)

			controller := newTestController(t, ctrl, store, cs, mc, nil)

			msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			err := controller.Process(context.Background(), delivery)

			switch {
			case tt.wantUnexpected:
				require.Error(t, err)
				assert.False(t, errs.IsUserError(err), "owner lookup failure should not be a user error")
			case tt.wantUserErr:
				require.Error(t, err)
				assert.True(t, errs.IsUserError(err), "duplicate detection should be a user error")
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestController_Process_ChangeStoreQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/abc"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)

	cs := changemock.NewMockChangeStore(ctrl)
	cs.EXPECT().FindOverlapping(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("change store down"))

	controller := newTestController(t, ctrl, store, cs, mc, nil)

	msg := queue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsUserError(err), "infra error should not be classified as user error")
}
