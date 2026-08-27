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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const testBuildID = "go-code-on-odin-submitqueue/builds/2867068"

type buildSignalDLQMocks struct {
	reqStore     *storagemock.MockRequestStore
	queueStore   *storagemock.MockQueueStore
	buildStore   *storagemock.MockBuildStore
	metricsScope tally.TestScope
}

func newBuildSignalController(t *testing.T, ctrl *gomock.Controller) (consumer.Controller, buildSignalDLQMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := buildSignalDLQMocks{
		reqStore:     storagemock.NewMockRequestStore(ctrl),
		queueStore:   storagemock.NewMockQueueStore(ctrl),
		buildStore:   storagemock.NewMockBuildStore(ctrl),
		metricsScope: scope,
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()

	c := NewDLQBuildSignalController(
		zap.NewNop().Sugar(),
		scope,
		staticStorageFactory{store: store},
		TopicKey(stovepipemq.TopicKeyBuildSignal),
		"stovepipe-buildsignal-dlq",
	)
	return c, m
}

func buildSignalPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return b
}

func build() entity.Build {
	return entity.Build{
		ID:        testBuildID,
		RequestID: testID,
		Status:    entity.BuildStatusRunning,
		Version:   3,
	}
}

func TestBuildSignalProcess(t *testing.T) {
	tests := []struct {
		name       string
		payload    []byte
		setup      func(m buildSignalDLQMocks)
		wantErr    bool
		wantMetric string
	}{
		{
			// The case this reconciler exists for: a poll message that
			// dead-lettered while its request was holding a build slot.
			name: "processing request releases the queue slot before marking failed",
			setup: func(m buildSignalDLQMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
					Name: testQueue, InFlightCount: 1, Version: 5,
				}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{
					Name: testQueue, InFlightCount: 0, Version: 5,
				}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			},
		},
		{
			name: "already terminal request is a no-op",
			setup: func(m buildSignalDLQMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil)
			},
		},
		{
			name: "build not found is a no-op",
			setup: func(m buildSignalDLQMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, storage.ErrNotFound)
			},
		},
		{
			name: "build store error is returned",
			setup: func(m buildSignalDLQMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, assert.AnError)
			},
			wantErr: true,
		},
		{
			name: "build without a request id is a no-op",
			setup: func(m buildSignalDLQMocks) {
				b := build()
				b.RequestID = ""
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(b, nil)
			},
		},
		{
			name: "slot release failure aborts the terminal write",
			setup: func(m buildSignalDLQMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, assert.AnError)
			},
			wantErr: true,
		},
		{
			name:    "malformed payload is returned as an error",
			payload: []byte("not-a-proto"),
			setup:   func(buildSignalDLQMocks) {},
			wantErr: true,
		},
		{
			name:       "empty build id is returned as an error",
			payload:    buildSignalPayload(t, ""),
			setup:      func(buildSignalDLQMocks) {},
			wantErr:    true,
			wantMetric: "test.buildsignal_dlq_controller.buildsignal_dlq.empty_id_errors+queue=monorepo/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newBuildSignalController(t, ctrl)
			tt.setup(m)

			payload := tt.payload
			if payload == nil {
				payload = buildSignalPayload(t, testBuildID)
			}

			err := c.Process(queueContext(), delivery(t, ctrl, payload))
			if tt.wantMetric != "" {
				counter, ok := m.metricsScope.Snapshot().Counters()[tt.wantMetric]
				require.True(t, ok)
				assert.EqualValues(t, 1, counter.Value())
			}

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBuildSignalTopicKey(t *testing.T) {
	c, _ := newBuildSignalController(t, gomock.NewController(t))

	assert.Equal(t, consumer.TopicKey("buildsignal_dlq"), TopicKey(stovepipemq.TopicKeyBuildSignal))
	assert.Equal(t, consumer.TopicKey("buildsignal_dlq"), c.TopicKey())
	assert.Equal(t, "buildsignal_dlq", c.Name())
	assert.Equal(t, "stovepipe-buildsignal-dlq", c.ConsumerGroup())
}
