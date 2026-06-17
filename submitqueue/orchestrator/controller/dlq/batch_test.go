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
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQBatchController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := NewDLQBatchController(zaptest.NewLogger(t).Sugar(), testScope(), store, TopicKey(topickey.TopicKeyMerge), "orchestrator-merge-dlq")

	assert.Equal(t, "merge_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("merge_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-merge-dlq", c.ConsumerGroup())
}

func TestDLQBatchController_Process_FailsAndFansOut(t *testing.T) {
	ctrl := gomock.NewController(t)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/9").Return(entity.Batch{
		ID: "q/batch/9", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateMerging, Version: 2,
	}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), "q/batch/9", int32(2), int32(3), entity.BatchStateFailed).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 1, State: entity.RequestStateProcessing,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(1), int32(2), entity.RequestStateError).Return(nil)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()

	c := NewDLQBatchController(zaptest.NewLogger(t).Sugar(), testScope(), store, TopicKey(topickey.TopicKeyMerge), "orchestrator-merge-dlq")

	payload, err := entity.BatchID{ID: "q/batch/9"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQBatchController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	c := NewDLQBatchController(zaptest.NewLogger(t).Sugar(), testScope(), store, TopicKey(topickey.TopicKeyMerge), "orchestrator-merge-dlq")

	delivery := newMockDelivery(ctrl, []byte("garbage"))
	err := c.Process(context.Background(), delivery)
	require.Error(t, err)
}

func TestDLQBatchController_Process_EmptyIDFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	c := NewDLQBatchController(zaptest.NewLogger(t).Sugar(), testScope(), store, TopicKey(topickey.TopicKeyMerge), "orchestrator-merge-dlq")

	payload, err := entity.BatchID{ID: ""}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	err = c.Process(context.Background(), delivery)
	require.Error(t, err)
}
