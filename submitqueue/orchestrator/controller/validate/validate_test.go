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
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/core/consumer"
	"github.com/uber/submitqueue/core/errs"
	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
	queuemock "github.com/uber/submitqueue/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	changeprovidermock "github.com/uber/submitqueue/submitqueue/extension/changeprovider/mock"
	"github.com/uber/submitqueue/submitqueue/extension/mergechecker"
	mergecheckermock "github.com/uber/submitqueue/submitqueue/extension/mergechecker/mock"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
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

func (m *mockChangeProvider) Get(ctx context.Context, request entity.Request) ([]entity.ChangeInfo, error) {
	return []entity.ChangeInfo{
		{
			URI: "github://org/repo/pull/123/abcdef0123456789abcdef0123456789abcdef01",
			Details: entity.ChangeDetails{
				Author: entity.Author{
					Name:  "Test User",
					Email: "test@example.com",
				},
				ChangedFiles: []entity.ChangedFile{
					{Path: "main.go"},
				},
			},
		},
	}, nil
}

// newMergeableMock returns a mock MergeChecker that always returns mergeable.
func newMergeableMock(ctrl *gomock.Controller) *mergecheckermock.MockMergeChecker {
	mc := mergecheckermock.NewMockMergeChecker(ctrl)
	mc.EXPECT().Check(gomock.Any(), gomock.Any()).Return(entity.MergeResult{Mergeable: true}, nil).AnyTimes()
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

// newMockChangeStore creates a MockChangeStore with default no-overlap behavior
// (GetByURI returns nothing) and accepts the claim Create. Tests that need to
// simulate overlap or assert the claim override these with their own EXPECTs.
func newMockChangeStore(ctrl *gomock.Controller) *storagemock.MockChangeStore {
	cs := storagemock.NewMockChangeStore(ctrl)
	cs.EXPECT().GetByURI(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	cs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return cs
}

// newTestController creates a controller with test dependencies.
func newTestController(
	t *testing.T,
	ctrl *gomock.Controller,
	store *storagemock.MockStorage,
	cs *storagemock.MockChangeStore,
	mc mergechecker.MergeChecker,
	publishErr error,
) *Controller {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	store.EXPECT().GetChangeStore().Return(cs).AnyTimes()

	mockPub := queuemock.NewMockPublisher(ctrl)
	mockPub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			return publishErr
		},
	).AnyTimes()

	mockQ := queuemock.NewMockQueue(ctrl)
	mockQ.EXPECT().Publisher().Return(mockPub).AnyTimes()

	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBatch, Name: "batch", Queue: mockQ}},
	)
	require.NoError(t, err)

	cp := &mockChangeProvider{}

	mcFactory := mergecheckermock.NewMockFactory(ctrl)
	mcFactory.EXPECT().For(gomock.Any()).Return(mc, nil).AnyTimes()
	cpFactory := changeprovidermock.NewMockFactory(ctrl)
	cpFactory.EXPECT().For(gomock.Any()).Return(cp, nil).AnyTimes()

	return NewController(logger, scope, store, registry, mcFactory, cpFactory, topickey.TopicKeyValidate, "orchestrator-validate")
}

func TestNewController(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)
	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	require.NotNil(t, controller)
	assert.Equal(t, topickey.TopicKeyValidate, controller.TopicKey())
	assert.Equal(t, "orchestrator-validate", controller.ConsumerGroup())
	assert.Equal(t, "validate", controller.Name())
}

func TestController_Process_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/456/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := entityqueue.NewMessage("test-queue/123", requestIDPayload(t, request.ID), "test-queue", nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	require.NoError(t, controller.Process(context.Background(), delivery))
}

// TestController_Process_ClaimsChangeRecordsWithDetails verifies that, on the happy
// path, validate creates a change record per fetched change, capturing the provider
// details in a single immutable Create.
func TestController_Process_ClaimsChangeRecordsWithDetails(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := newMergeableMock(ctrl)

	// The request's URI matches the URI the mock change provider returns, so the
	// claim carries that change's details.
	const uri = "github://org/repo/pull/123/abcdef0123456789abcdef0123456789abcdef01"
	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{uri}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)

	wantDetails := entity.ChangeDetails{
		Author:       entity.Author{Name: "Test User", Email: "test@example.com"},
		ChangedFiles: []entity.ChangedFile{{Path: "main.go"}},
	}
	cs := storagemock.NewMockChangeStore(ctrl)
	// Duplicate-detection read finds no overlap.
	cs.EXPECT().GetByURI(gomock.Any(), request.Queue, uri).Return(nil, nil).AnyTimes()
	// Capture the record passed to Create; assert identity + details.
	cs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, rec entity.ChangeRecord) error {
			assert.Equal(t, uri, rec.URI)
			assert.Equal(t, request.ID, rec.RequestID)
			assert.Equal(t, request.Queue, rec.Queue)
			assert.Equal(t, wantDetails, rec.Details)
			return nil
		},
	)

	controller := newTestController(t, ctrl, store, cs, mc, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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

	msg := entityqueue.NewMessage("test-queue/123", requestIDPayload(t, "test-queue/123"), "test-queue", nil)
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
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/789abc1234567890abcdef1234567890abcdef12"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)

	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, fmt.Errorf("publish failed"))

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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
	mc.EXPECT().Check(gomock.Any(), gomock.Any()).Return(entity.MergeResult{Mergeable: false}, nil)

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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
	mc.EXPECT().Check(gomock.Any(), gomock.Any()).Return(entity.MergeResult{}, fmt.Errorf("merge check failed"))

	request := entity.Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       entity.Change{URIs: []string{"github://uber/repo/pull/1/abcdef0123456789abcdef0123456789abcdef01"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)
	controller := newTestController(t, ctrl, store, newMockChangeStore(ctrl), mc, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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
		uriA          = "github://uber/service/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		uriB          = "github://uber/service/pull/2/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		anotherReqID  = queueName + "/050"
		orphanReqID   = queueName + "/999"
		terminalReqID = queueName + "/200"
	)

	tests := []struct {
		name           string
		requestURIs    []string                         // URIs on the new request; defaults to [uriA]
		byURI          map[string][]entity.ChangeRecord // GetByURI mock returns
		ownerLookup    map[string]entity.Request
		ownerNotFound  map[string]bool
		ownerErr       map[string]error
		wantUserErr    bool
		wantUnexpected bool
	}{
		{
			name:  "no overlap proceeds to merge check",
			byURI: map[string][]entity.ChangeRecord{uriA: nil},
		},
		{
			name: "overlap with live in-flight request returns user error",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: dupRequestID, Queue: queueName}},
			},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateStarted, Version: 1},
			},
			wantUserErr: true,
		},
		{
			name: "overlap with terminal owner is skipped",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: terminalReqID, Queue: queueName}},
			},
			ownerLookup: map[string]entity.Request{
				terminalReqID: {ID: terminalReqID, Queue: queueName, State: entity.RequestStateLanded, Version: 5},
			},
		},
		{
			name: "overlap with orphan owner (ErrNotFound) is skipped",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: orphanReqID, Queue: queueName}},
			},
			ownerNotFound: map[string]bool{orphanReqID: true},
		},
		{
			name:        "multi-URI same owner deduped to single Get call",
			requestURIs: []string{uriA, uriB},
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: dupRequestID, Queue: queueName}},
				uriB: {{URI: uriB, RequestID: dupRequestID, Queue: queueName}},
			},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateValidated, Version: 2},
			},
			wantUserErr: true,
		},
		{
			name:        "first URI's owner is terminal, second URI's owner is live",
			requestURIs: []string{uriA, uriB},
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: terminalReqID, Queue: queueName}},
				uriB: {{URI: uriB, RequestID: anotherReqID, Queue: queueName}},
			},
			ownerLookup: map[string]entity.Request{
				terminalReqID: {ID: terminalReqID, State: entity.RequestStateError, Version: 3},
				anotherReqID:  {ID: anotherReqID, State: entity.RequestStateProcessing, Version: 4},
			},
			wantUserErr: true,
		},
		{
			// Store doesn't exclude self; controller filters by RequestID and must not look up its own row.
			name: "self row in result is filtered (no Get call)",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: newRequestID, Queue: queueName}},
			},
		},
		{
			name: "self row mixed with live other returns the other",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {
					{URI: uriA, RequestID: newRequestID, Queue: queueName},
					{URI: uriA, RequestID: dupRequestID, Queue: queueName},
				},
			},
			ownerLookup: map[string]entity.Request{
				dupRequestID: {ID: dupRequestID, Queue: queueName, State: entity.RequestStateStarted, Version: 1},
			},
			wantUserErr: true,
		},
		{
			name: "owner lookup unexpected error propagates",
			byURI: map[string][]entity.ChangeRecord{
				uriA: {{URI: uriA, RequestID: dupRequestID, Queue: queueName}},
			},
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

			uris := tt.requestURIs
			if uris == nil {
				uris = []string{uriA}
			}

			request := entity.Request{
				ID:           newRequestID,
				Queue:        queueName,
				Change:       entity.Change{URIs: uris},
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

			cs := storagemock.NewMockChangeStore(ctrl)
			// One GetByURI per URI on the request, in order. Controller short-circuits on first
			// live duplicate, so .AnyTimes() lets unmatched URIs go un-queried.
			for _, u := range uris {
				cs.EXPECT().GetByURI(gomock.Any(), queueName, u).Return(tt.byURI[u], nil).MaxTimes(1)
			}
			// When no duplicate is found, the controller continues to fetch change info
			// and claims each fetched change via Create. Accept any Create.
			cs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			controller := newTestController(t, ctrl, store, cs, mc, nil)

			msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
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
		Change:       entity.Change{URIs: []string{"github://uber/service/pull/1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		LandStrategy: entity.RequestLandStrategyRebase,
		State:        entity.RequestStateStarted,
		Version:      1,
	}
	store, _ := newMockStorage(ctrl, request)

	cs := storagemock.NewMockChangeStore(ctrl)
	cs.EXPECT().GetByURI(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("change store down"))

	controller := newTestController(t, ctrl, store, cs, mc, nil)

	msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
	delivery := queuemock.NewMockDelivery(ctrl)
	delivery.EXPECT().Message().Return(msg).AnyTimes()
	delivery.EXPECT().Attempt().Return(1).AnyTimes()

	err := controller.Process(context.Background(), delivery)
	require.Error(t, err)
	assert.False(t, errs.IsUserError(err), "infra error should not be classified as user error")
}

// A request already in a terminal state (e.g. cancelled while the validate
// message was in flight) must be short-circuited before any extension is
// touched and before any publish happens. We verify this by registering a
// merge checker and change store with NO expectations — gomock fails the test
// if either is called — and a publisher that returns an error if invoked.
func TestController_Process_TerminalShortCircuit(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateCancelled,
		entity.RequestStateLanded,
		entity.RequestStateError,
	} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)

			request := entity.Request{
				ID:      "test-queue/123",
				Queue:   "test-queue",
				State:   state,
				Version: 5,
			}
			store, _ := newMockStorage(ctrl, request)

			// No EXPECTs on merge checker or change store: gomock will fail if either is called.
			mc := mergecheckermock.NewMockMergeChecker(ctrl)
			cs := storagemock.NewMockChangeStore(ctrl)

			// Sentinel publish error: if Process publishes, it returns a non-nil err,
			// which the require.NoError below will catch.
			controller := newTestController(t, ctrl, store, cs, mc, fmt.Errorf("should not publish"))

			msg := entityqueue.NewMessage(request.ID, requestIDPayload(t, request.ID), request.Queue, nil)
			delivery := queuemock.NewMockDelivery(ctrl)
			delivery.EXPECT().Message().Return(msg).AnyTimes()
			delivery.EXPECT().Attempt().Return(1).AnyTimes()

			require.NoError(t, controller.Process(context.Background(), delivery))
		})
	}
}
