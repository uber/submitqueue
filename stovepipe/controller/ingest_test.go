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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	countermock "github.com/uber/submitqueue/platform/extension/counter/mock"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue = "monorepo/main"
	testURI   = "git://repo/monorepo/main/abc123"
)

// ingestMocks bundles the mocks an Ingest test case wires expectations on.
type ingestMocks struct {
	counter   *countermock.MockCounter
	factory   *scmock.MockFactory
	sc        *scmock.MockSourceControl
	reqStore  *storagemock.MockRequestStore
	uriStore  *storagemock.MockRequestURIStore
	publisher *mqmock.MockPublisher
}

func newIngestController(t *testing.T, ctrl *gomock.Controller) (*IngestController, ingestMocks) {
	t.Helper()

	m := ingestMocks{
		counter:   countermock.NewMockCounter(ctrl),
		factory:   scmock.NewMockFactory(ctrl),
		sc:        scmock.NewMockSourceControl(ctrl),
		reqStore:  storagemock.NewMockRequestStore(ctrl),
		uriStore:  storagemock.NewMockRequestURIStore(ctrl),
		publisher: mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetRequestURIStore().Return(m.uriStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyProcess, Name: "process", Queue: queue},
	})
	require.NoError(t, err)

	c := NewIngestController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), m.counter, m.factory, store, registry)
	return c, m
}

// expectResolve wires the SourceControl factory + Latest happy path returning testURI.
func expectResolve(m ingestMocks) {
	m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
	m.sc.EXPECT().Latest(gomock.Any()).Return(testURI, nil)
}

func TestIngestController_Ingest(t *testing.T) {
	tests := []struct {
		name        string
		queue       string
		setup       func(m ingestMocks)
		wantID      string
		wantErr     bool
		wantInvalid bool
	}{
		{
			name:  "happy path mints persists and publishes",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), "request/"+testQueue).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testQueue, testURI, "request/monorepo/main/7").Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "request/monorepo/main/7").Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "request/monorepo/main/7",
		},
		{
			name:  "dedup with existing accepted request republishes without minting",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("request/monorepo/main/3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "request/monorepo/main/3").Return(entity.Request{ID: "request/monorepo/main/3", State: entity.RequestStateAccepted}, nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "request/monorepo/main/3",
		},
		{
			name:  "heals when uri mapped but request missing",
			queue: testQueue,
			setup: func(m ingestMocks) {
				// Prior attempt committed the URI mapping but failed before the request write.
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("request/monorepo/main/3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "request/monorepo/main/3").Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "request/monorepo/main/3",
		},
		{
			name:  "dedup race returns winner id and completes",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), "request/"+testQueue).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testQueue, testURI, "request/monorepo/main/7").Return(storage.ErrAlreadyExists)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("request/monorepo/main/3", nil)
				m.reqStore.EXPECT().Get(gomock.Any(), "request/monorepo/main/3").Return(entity.Request{ID: "request/monorepo/main/3", State: entity.RequestStateAccepted}, nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)
			},
			wantID: "request/monorepo/main/3",
		},
		{
			name:        "empty queue is invalid",
			queue:       "",
			setup:       func(m ingestMocks) {},
			wantErr:     true,
			wantInvalid: true,
		},
		{
			name:  "unknown queue head is invalid",
			queue: testQueue,
			setup: func(m ingestMocks) {
				m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
				m.sc.EXPECT().Latest(gomock.Any()).Return("", sourcecontrol.ErrNotFound)
			},
			wantErr:     true,
			wantInvalid: true,
		},
		{
			name:  "source control infra error is not invalid",
			queue: testQueue,
			setup: func(m ingestMocks) {
				m.factory.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(m.sc, nil)
				m.sc.EXPECT().Latest(gomock.Any()).Return("", errors.New("sc unavailable"))
			},
			wantErr: true,
		},
		{
			name:  "counter error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(0), errors.New("counter unavailable"))
			},
			wantErr: true,
		},
		{
			name:  "request store create error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testQueue, testURI, gomock.Any()).Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("db down"))
			},
			wantErr: true,
		},
		{
			name:  "publish error",
			queue: testQueue,
			setup: func(m ingestMocks) {
				expectResolve(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testQueue, testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), gomock.Any()).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testQueue, testURI, gomock.Any()).Return(nil)
				m.reqStore.EXPECT().Get(gomock.Any(), gomock.Any()).Return(entity.Request{}, storage.ErrNotFound)
				m.reqStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(errors.New("queue down"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newIngestController(t, ctrl)
			tt.setup(m)

			resp, err := c.Ingest(context.Background(), &pb.IngestRequest{Queue: tt.queue})

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantInvalid, IsInvalidRequest(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantID, resp.Id)
		})
	}
}
