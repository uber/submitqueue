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

package record

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testID    = "request/monorepo/main/7"
	testURI   = "git://remote/monorepo/main/head-sha"
)

// recordMocks bundles the mocks a record controller test case wires
// expectations on.
type recordMocks struct {
	reqStore   *storagemock.MockRequestStore
	queueStore *storagemock.MockQueueStore
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, recordMocks) {
	t.Helper()

	m := recordMocks{
		reqStore:   storagemock.NewMockRequestStore(ctrl),
		queueStore: storagemock.NewMockQueueStore(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	c := NewController(
		zap.NewNop().Sugar(),
		tally.NewTestScope("test", nil),
		staticStorageFactory{store: store},
		stovepipemq.TopicKeyRecord,
		"stovepipe-record",
	)
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testID, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func recordPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.Record{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return b
}

// requestWithState returns a terminal-stage Request in the given state.
func requestWithState(state entity.RequestState) entity.Request {
	return entity.Request{
		ID:      testID,
		Queue:   testQueue,
		URI:     testURI,
		State:   state,
		Version: 2,
	}
}

// queueRow returns the testQueue's row holding the given bookmark.
func queueRow(lastGreenURI, lastGreenRequestID string, version int32) entity.Queue {
	return entity.Queue{
		Name:               testQueue,
		LastGreenURI:       lastGreenURI,
		LastGreenRequestID: lastGreenRequestID,
		Version:            version,
	}
}

func TestProcess_AdvancesBookmarkOnSuccess(t *testing.T) {
	tests := []struct {
		name    string
		stored  entity.Queue
		wantURI string
	}{
		{
			name:    "bookmark unset",
			stored:  queueRow("", "", 1),
			wantURI: testURI,
		},
		{
			name:    "stored bookmark is older",
			stored:  queueRow("git://remote/monorepo/main/old", "request/monorepo/main/3", 4),
			wantURI: testURI,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(tt.stored, nil)

			var written entity.Queue
			m.queueStore.EXPECT().
				Update(gomock.Any(), gomock.Any(), tt.stored.Version, tt.stored.Version+1).
				DoAndReturn(func(_ context.Context, q entity.Queue, _, _ int32) error {
					written = q
					return nil
				})

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
			assert.Equal(t, tt.wantURI, written.LastGreenURI)
			assert.Equal(t, testID, written.LastGreenRequestID)
		})
	}
}

func TestProcess_SkipsBookmarkWhenNotNewer(t *testing.T) {
	tests := []struct {
		name   string
		stored entity.Queue
	}{
		{
			name:   "same request redelivered",
			stored: queueRow(testURI, testID, 3),
		},
		{
			name:   "stored bookmark is newer",
			stored: queueRow("git://remote/monorepo/main/newer", "request/monorepo/main/9", 5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).
				Return(requestWithState(entity.RequestStateSucceeded), nil)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(tt.stored, nil)
			// No Update: the bookmark only moves forward.

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_TerminalWithoutGreenDoesNotTouchQueue(t *testing.T) {
	tests := []struct {
		name  string
		state entity.RequestState
	}{
		{name: "failed", state: entity.RequestStateFailed},
		{name: "cancelled", state: entity.RequestStateCancelled},
		{name: "superseded", state: entity.RequestStateSuperseded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(tt.state), nil)
			// No queue reads or writes at all.

			require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_NonTerminalRequestFails(t *testing.T) {
	tests := []struct {
		name  string
		state entity.RequestState
	}{
		{name: "accepted", state: entity.RequestStateAccepted},
		{name: "processing", state: entity.RequestStateProcessing},
		{name: "unknown", state: entity.RequestStateUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(tt.state), nil)

			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_RetriesBookmarkOnVersionMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	stale := queueRow("", "", 1)
	fresh := queueRow("", "", 2)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)

	gomock.InOrder(
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(stale, nil),
		m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), stale.Version, stale.Version+1).
			Return(storage.ErrVersionMismatch),
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(fresh, nil),
		m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), fresh.Version, fresh.Version+1).
			Return(nil),
	)

	require.NoError(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_MalformedRequestIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	request := requestWithState(entity.RequestStateSucceeded)
	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
		Return(queueRow("git://remote/monorepo/main/old", "not-a-request-id", 1), nil)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
}

func TestProcess_StorageErrorsPropagate(t *testing.T) {
	tests := []struct {
		name  string
		setup func(m recordMocks)
	}{
		{
			name: "request load fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(entity.Request{}, errors.New("boom"))
			},
		},
		{
			name: "queue load fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).
					Return(entity.Queue{}, errors.New("boom"))
			},
		},
		{
			name: "queue update fails",
			setup: func(m recordMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).
					Return(requestWithState(entity.RequestStateSucceeded), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow("", "", 1), nil)
				m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).
					Return(errors.New("boom"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			tt.setup(m)

			require.Error(t, c.Process(context.Background(), delivery(t, ctrl, recordPayload(t, testID))))
		})
	}
}

func TestProcess_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, []byte("not-protojson"))))
}

func TestProcess_QueueMismatchFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	// The request's own queue is authoritative; a payload disagreeing with it
	// is malformed, so the bookmark must not be touched.
	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(requestWithState(entity.RequestStateSucceeded), nil)

	payload, err := stovepipemq.Marshal(&stovepipemq.Record{Id: testID, QueueName: "monorepo/other"})
	require.NoError(t, err)

	require.Error(t, c.Process(context.Background(), delivery(t, ctrl, payload)))
}
