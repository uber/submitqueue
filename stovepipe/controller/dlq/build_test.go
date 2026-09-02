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
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newBuildController(t *testing.T, ctrl *gomock.Controller) (consumer.Controller, dlqMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := dlqMocks{
		reqStore:     storagemock.NewMockRequestStore(ctrl),
		queueStore:   storagemock.NewMockQueueStore(ctrl),
		store:        storagemock.NewMockStorage(ctrl),
		materializer: requestlogmock.NewMockMaterializer(ctrl),
		metricsScope: scope,
	}
	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	c := NewDLQBuildController(zap.NewNop().Sugar(), scope, staticStorageFactory{store: m.store}, m.materializer, TopicKey(stovepipemq.TopicKeyBuild), "stovepipe-build-dlq")
	return c, m
}

func buildPayload(t *testing.T, id string) []byte {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return payload
}

func TestBuildProcess(t *testing.T) {
	tests := []struct {
		name       string
		payload    []byte
		setup      func(m dlqMocks)
		wantErr    bool
		wantMetric string
	}{
		{
			name: "processing request is failed",
			setup: func(m dlqMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue, InFlightCount: 1, Version: 5}, nil)
				m.queueStore.EXPECT().Update(gomock.Any(), entity.Queue{Name: testQueue, Version: 5}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				updateCall := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
				expectFailureLog(m, entity.RequestOutcomeReasonProcessingFailed, 3).After(updateCall)
			},
			wantMetric: "test.build_dlq_controller.build_dlq.reconciled+queue=monorepo/main",
		},
		{name: "malformed payload is returned", payload: []byte("not-a-proto"), wantErr: true},
		{
			name:       "empty request id is returned",
			payload:    buildPayload(t, ""),
			wantErr:    true,
			wantMetric: "test.build_dlq_controller.build_dlq.empty_id_errors+queue=monorepo/main",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			controller, mocks := newBuildController(t, ctrl)
			if tt.setup != nil {
				tt.setup(mocks)
			}

			payload := tt.payload
			if payload == nil {
				payload = buildPayload(t, testID)
			}
			err := controller.Process(queueContext(), delivery(t, ctrl, payload))
			if tt.wantMetric != "" {
				counter, ok := mocks.metricsScope.Snapshot().Counters()[tt.wantMetric]
				require.True(t, ok)
				assert.EqualValues(t, 1, counter.Value())
			}
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, consumer.TopicKey("build_dlq"), controller.TopicKey())
			assert.Equal(t, "build_dlq", controller.Name())
			assert.Equal(t, "stovepipe-build-dlq", controller.ConsumerGroup())
		})
	}
}
