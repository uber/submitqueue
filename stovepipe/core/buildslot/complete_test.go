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

package buildslot

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestCompleteReleasesOnlyOwnedCapacityAndAdvancesSuccessfulBaseline(t *testing.T) {
	for _, tt := range []struct {
		name      string
		state     entity.RequestState
		bookmark  string
		wantGreen string
	}{
		{name: "successful build", state: entity.RequestStateSucceeded, bookmark: "3", wantGreen: "7"},
		{name: "newer build already succeeded", state: entity.RequestStateSucceeded, bookmark: "9", wantGreen: "9"},
		{name: "first green build", state: entity.RequestStateSucceeded, wantGreen: "7"},
		{name: "failed build", state: entity.RequestStateFailed, bookmark: "3", wantGreen: "3"},
		{name: "cancelled build", state: entity.RequestStateCancelled, bookmark: "3", wantGreen: "3"},
		{name: "superseded reservation", state: entity.RequestStateSuperseded, bookmark: "3", wantGreen: "3"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockStorage(ctrl)
			queues := storagemock.NewMockQueueStore(ctrl)
			store.EXPECT().GetQueueStore().Return(queues).AnyTimes()
			request := entity.Request{ID: "7", Queue: "repo/main", URI: "commit-7", State: tt.state}
			queue := entity.Queue{Name: request.Queue, Version: 4, InFlightCount: 2,
				InFlightRequestIDs: []string{"7", "8"}, LastGreenRequestID: tt.bookmark, LastGreenURI: "old-commit"}
			queues.EXPECT().Get(gomock.Any(), request.Queue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
				return queue, nil
			}).Times(2)
			queues.EXPECT().Update(gomock.Any(), gomock.Any(), int32(4), int32(5)).DoAndReturn(
				func(_ context.Context, updated entity.Queue, _, version int32) error {
					assert.Equal(t, []string{"7", "8"}, queue.InFlightRequestIDs)
					assert.Equal(t, int32(1), updated.InFlightCount)
					assert.Equal(t, []string{"8"}, updated.InFlightRequestIDs)
					assert.Equal(t, tt.wantGreen, updated.LastGreenRequestID)
					if tt.wantGreen == "7" {
						assert.Equal(t, request.URI, updated.LastGreenURI)
					}
					updated.Version = version
					queue = updated
					return nil
				})
			require.NoError(t, Complete(context.Background(), store, request))
			require.NoError(t, Complete(context.Background(), store, request))
		})
	}
}

func TestCompleteRecoversAfterCommittedWriteReturnsAnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	queues := storagemock.NewMockQueueStore(ctrl)
	store.EXPECT().GetQueueStore().Return(queues).AnyTimes()
	request := entity.Request{ID: "7", Queue: "repo/main", URI: "commit-7", State: entity.RequestStateSucceeded}
	queue := entity.Queue{Name: request.Queue, Version: 1, InFlightCount: 2, InFlightRequestIDs: []string{"7", "8"}}
	queues.EXPECT().Get(gomock.Any(), request.Queue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
		return queue, nil
	}).Times(2)
	queues.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(
		func(_ context.Context, updated entity.Queue, _, version int32) error {
			queue = updated
			queue.Version = version
			return assert.AnError
		})
	require.ErrorIs(t, Complete(context.Background(), store, request), assert.AnError)
	require.NoError(t, Complete(context.Background(), store, request))
	assert.Equal(t, []string{"8"}, queue.InFlightRequestIDs)
	assert.Equal(t, int32(1), queue.InFlightCount)
	assert.Equal(t, request.URI, queue.LastGreenURI)
}

func TestCompleteReloadsConcurrentQueueUpdates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := storagemock.NewMockStorage(ctrl)
	queues := storagemock.NewMockQueueStore(ctrl)
	store.EXPECT().GetQueueStore().Return(queues)
	request := entity.Request{ID: "7", Queue: "repo/main", URI: "commit-7", State: entity.RequestStateSucceeded}
	stale := entity.Queue{Name: request.Queue, Version: 1, InFlightCount: 1, InFlightRequestIDs: []string{"7"}}
	fresh := stale
	fresh.Version = 2
	fresh.InFlightCount = 2
	fresh.InFlightRequestIDs = []string{"7", "8"}
	fresh.LastGreenRequestID = "9"
	fresh.LastGreenURI = "commit-9"
	gomock.InOrder(
		queues.EXPECT().Get(gomock.Any(), request.Queue).Return(stale, nil),
		queues.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storage.ErrVersionMismatch),
		queues.EXPECT().Get(gomock.Any(), request.Queue).Return(fresh, nil),
		queues.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).DoAndReturn(
			func(_ context.Context, updated entity.Queue, _, _ int32) error {
				assert.Equal(t, []string{"8"}, updated.InFlightRequestIDs)
				assert.Equal(t, fresh.LastGreenURI, updated.LastGreenURI)
				return nil
			}),
	)
	require.NoError(t, Complete(context.Background(), store, request))
}

func TestCompleteRejectsInvalidOwnership(t *testing.T) {
	for _, queue := range []entity.Queue{
		{InFlightCount: 1},
		{InFlightCount: 2, InFlightRequestIDs: []string{"7", "7"}},
		{InFlightCount: 1, InFlightRequestIDs: []string{""}},
		{InFlightCount: -1},
	} {
		t.Run(fmt.Sprint(queue), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := storagemock.NewMockStorage(ctrl)
			queues := storagemock.NewMockQueueStore(ctrl)
			store.EXPECT().GetQueueStore().Return(queues)
			queues.EXPECT().Get(gomock.Any(), "repo/main").Return(queue, nil)
			require.Error(t, Complete(context.Background(), store, entity.Request{ID: "7", Queue: "repo/main", State: entity.RequestStateFailed}))
		})
	}
}
