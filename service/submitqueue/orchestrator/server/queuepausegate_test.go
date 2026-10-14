// Copyright (c) 2026 Uber Technologies, Inc.
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

package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/extension/consumergate"
	consumergatemock "github.com/uber/submitqueue/platform/extension/consumergate/mock"
	"github.com/uber/submitqueue/submitqueue/entity"
	queuepausemock "github.com/uber/submitqueue/submitqueue/extension/queuepause/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestQueuePauseGate_Enter(t *testing.T) {
	key := consumergate.Key{
		ConsumerGroup: "orchestrator",
		Partition:     entityqueue.PartitionIdentity{Tenant: "my-queue", PartitionKey: "p1"},
	}
	tests := []struct {
		name        string
		state       entity.QueuePause
		storeErr    error
		nextBlocked bool
		wantBlocked bool
		wantNext    bool
	}{
		{name: "freeze closes the gate", state: entity.QueuePause{FreezeProcessing: true}, wantBlocked: true},
		{name: "freeze with reject closes the gate", state: entity.QueuePause{FreezeProcessing: true, RejectLands: true}, wantBlocked: true},
		{name: "failed read closes the gate", storeErr: errors.New("pause backend down"), wantBlocked: true},
		{name: "not paused defers to the wrapped gate", state: entity.QueuePause{}, wantNext: true},
		{name: "reject only does not freeze", state: entity.QueuePause{RejectLands: true}, wantNext: true},
		{name: "wrapped gate can still close it", state: entity.QueuePause{}, nextBlocked: true, wantNext: true, wantBlocked: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			pauses := queuepausemock.NewMockStore(ctrl)
			pauses.EXPECT().Get(gomock.Any(), "my-queue").Return(tt.state, tt.storeErr)
			next := consumergatemock.NewMockGate(ctrl)
			if tt.wantNext {
				nextEntry := consumergatemock.NewMockEntry(ctrl)
				nextEntry.EXPECT().Blocked().Return(tt.nextBlocked).AnyTimes()
				next.EXPECT().Enter(gomock.Any(), key).Return(nextEntry, nil)
			}
			gate := newQueuePauseGate(zap.NewNop().Sugar(), pauses, next)

			entry, err := gate.Enter(context.Background(), key)

			require.NoError(t, err)
			assert.Equal(t, tt.wantBlocked, entry.Blocked())
		})
	}
}

func TestQueuePauseGate_EnterPropagatesWrappedGateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	key := consumergate.Key{Partition: entityqueue.PartitionIdentity{Tenant: "my-queue"}}
	pauses := queuepausemock.NewMockStore(ctrl)
	pauses.EXPECT().Get(gomock.Any(), "my-queue").Return(entity.QueuePause{}, nil)
	next := consumergatemock.NewMockGate(ctrl)
	next.EXPECT().Enter(gomock.Any(), key).Return(nil, errors.New("gate files unreadable"))
	gate := newQueuePauseGate(zap.NewNop().Sugar(), pauses, next)

	_, err := gate.Enter(context.Background(), key)

	require.Error(t, err)
}

func TestQueuePauseGate_EnterReturnsCancelledContextError(t *testing.T) {
	ctrl := gomock.NewController(t)
	key := consumergate.Key{Partition: entityqueue.PartitionIdentity{Tenant: "my-queue"}}
	pauses := queuepausemock.NewMockStore(ctrl)
	pauses.EXPECT().Get(gomock.Any(), "my-queue").Return(entity.QueuePause{}, fmt.Errorf("read pause state: %w", context.Canceled))
	gate := newQueuePauseGate(zap.NewNop().Sugar(), pauses, consumergatemock.NewMockGate(ctrl))

	_, err := gate.Enter(context.Background(), key)

	require.ErrorIs(t, err, context.Canceled)
}

func TestFrozenEntry_ParkAndUnparkKeepNothing(t *testing.T) {
	entry := frozenEntry{}

	require.NoError(t, entry.Park(context.Background(), consumergate.DeliveryDescriptor{}))
	require.NoError(t, entry.Unpark(context.Background(), consumergate.DeliveryDescriptor{}))
}
