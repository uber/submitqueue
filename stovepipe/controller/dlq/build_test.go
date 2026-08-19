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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/consumer"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func buildPayload(t *testing.T, id string) []byte {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return payload
}

func TestBuildProcess(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr bool
	}{
		{name: "processing request is failed"},
		{name: "malformed payload is returned", payload: []byte("not-a-proto"), wantErr: true},
		{name: "empty request id is returned", payload: buildPayload(t, ""), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			reqStore := storagemock.NewMockRequestStore(ctrl)
			queueStore := storagemock.NewMockQueueStore(ctrl)
			store := storagemock.NewMockStorage(ctrl)
			store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()
			store.EXPECT().GetQueueStore().Return(queueStore).AnyTimes()

			controller := NewDLQBuildController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), staticStorageFactory{store: store}, TopicKey(stovepipemq.TopicKeyBuild), "stovepipe-build-dlq")
			if !tt.wantErr {
				reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue, InFlightCount: 1, Version: 5}, nil)
				queueStore.EXPECT().Update(gomock.Any(), entity.Queue{Name: testQueue, Version: 5}, int32(5), int32(6)).Return(nil)
				updated := requestWithState(entity.RequestStateProcessing)
				updated.State = entity.RequestStateFailed
				reqStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
			}

			payload := tt.payload
			if payload == nil {
				payload = buildPayload(t, testID)
			}
			err := controller.Process(context.Background(), delivery(t, ctrl, payload))
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
