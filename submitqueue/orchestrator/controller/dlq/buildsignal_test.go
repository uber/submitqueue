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
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQBuildSignalController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)

	c := NewDLQBuildSignalController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, TopicKey(topickey.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")

	assert.Equal(t, "buildsignal_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("buildsignal_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-buildsignal-dlq", c.ConsumerGroup())
}

func TestDLQBuildSignalController_Process_FansOutToBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), "build-1").Return(entity.Build{
		ID: "build-1", BatchID: "q/batch/2", Status: entity.BuildStatusRunning,
	}, nil)

	batchStore := storagemock.NewMockBatchStore(ctrl)
	batchStore.EXPECT().Get(gomock.Any(), "q/batch/2").Return(entity.Batch{
		ID: "q/batch/2", Queue: "q", Contains: []string{"q/1"},
		State: entity.BatchStateSpeculating, Version: 3,
	}, nil)
	batchStore.EXPECT().UpdateState(gomock.Any(), "q/batch/2", int32(3), int32(4), entity.BatchStateFailed).Return(nil)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.Request{
		ID: "q/1", Version: 1, State: entity.RequestStateProcessing,
	}, nil)
	requestStore.EXPECT().UpdateState(gomock.Any(), "q/1", int32(1), int32(2), entity.RequestStateError).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()
	store.EXPECT().GetBatchStore().Return(batchStore).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQBuildSignalController(zaptest.NewLogger(t).Sugar(), testScope(), store, registry, TopicKey(topickey.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")

	payload, err := entity.BuildID{ID: "build-1"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQBuildSignalController_Process_BuildNotFoundIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), "build-1").Return(entity.Build{}, storage.ErrNotFound)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()

	c := NewDLQBuildSignalController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, TopicKey(topickey.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")

	payload, err := entity.BuildID{ID: "build-1"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQBuildSignalController_Process_BuildMissingBatchIsNoOp(t *testing.T) {
	ctrl := gomock.NewController(t)

	buildStore := storagemock.NewMockBuildStore(ctrl)
	buildStore.EXPECT().Get(gomock.Any(), "build-1").Return(entity.Build{
		ID: "build-1", BatchID: "", Status: entity.BuildStatusRunning,
	}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetBuildStore().Return(buildStore).AnyTimes()

	c := NewDLQBuildSignalController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, TopicKey(topickey.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")

	payload, err := entity.BuildID{ID: "build-1"}.ToBytes()
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQBuildSignalController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	c := NewDLQBuildSignalController(zaptest.NewLogger(t).Sugar(), testScope(), store, consumer.TopicRegistry{}, TopicKey(topickey.TopicKeyBuildSignal), "orchestrator-buildsignal-dlq")

	delivery := newMockDelivery(ctrl, []byte("garbage"))
	err := c.Process(context.Background(), delivery)
	require.Error(t, err)
}
