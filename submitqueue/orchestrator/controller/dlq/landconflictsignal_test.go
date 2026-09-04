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
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func TestDLQLandConflictSignalController_InterfaceAndAccessors(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()

	c := NewDLQLandConflictSignalController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, TopicKey(runwaymq.TopicKeyLandConflictCheckSignal), "orchestrator-landconflictsignal-dlq")

	assert.Equal(t, "land-conflict-check-signal_dlq", c.Name())
	assert.Equal(t, consumer.TopicKey("land-conflict-check-signal_dlq"), c.TopicKey())
	assert.Equal(t, "orchestrator-landconflictsignal-dlq", c.ConsumerGroup())
}

func TestDLQLandConflictSignalController_Process_ReconcilesRequest(t *testing.T) {
	ctrl := gomock.NewController(t)

	requestStore := storagemock.NewMockRequestStore(ctrl)
	request := entity.Request{
		ID: "q/1", Version: 1, State: entity.RequestStateProcessing,
	}
	requestStore.EXPECT().Get(gomock.Any(), "q/1").Return(request, nil)
	requestStore.EXPECT().Update(gomock.Any(), requestWithState(request, entity.RequestStateError), int32(1), int32(2)).Return(nil)

	registry := newTestLogRegistry(t, ctrl, 1, func(entity.RequestLog) error {
		return nil
	})

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	store.EXPECT().GetRequestStore().Return(requestStore).AnyTimes()

	c := NewDLQLandConflictSignalController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, registry, TopicKey(runwaymq.TopicKeyLandConflictCheckSignal), "orchestrator-landconflictsignal-dlq")

	payload, err := runwaymq.Marshal(&runwaymq.LandResult{Id: "q/1", Outcome: runwaypb.Outcome_FAILED, Reason: "boom"})
	require.NoError(t, err)

	delivery := newMockDelivery(ctrl, payload)
	require.NoError(t, c.Process(context.Background(), delivery))
}

func TestDLQLandConflictSignalController_Process_MalformedPayloadFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetQueueBatchStateStore().Return(newQueueBatchStateStore(ctrl)).AnyTimes()
	c := NewDLQLandConflictSignalController(zaptest.NewLogger(t).Sugar(), testScope(), staticStorageFactory{store: store}, consumer.TopicRegistry{}, TopicKey(runwaymq.TopicKeyLandConflictCheckSignal), "orchestrator-landconflictsignal-dlq")

	delivery := newMockDelivery(ctrl, []byte("garbage"))
	require.Error(t, c.Process(context.Background(), delivery))
}
