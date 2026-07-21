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

package request

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestMaterializer_PersistLog(t *testing.T) {
	base := testRequestSummary()
	log := entity.RequestLog{RequestID: "q/1", TimestampMs: 20, Status: entity.RequestStatusLanded, RequestVersion: 2, Metadata: map[string]string{}}
	t.Run("winning log updates both projections", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(base, nil)
		summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			assert.Equal(t, entity.RequestStatusLanded, updated.Status)
			assert.Equal(t, int32(2), updated.RequestVersion)
			return nil
		})
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(base), nil)
		queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})

	t.Run("unversioned terminal status does not receive terminal precedence", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		current := base
		current.Status = entity.RequestStatusLanded
		current.RequestVersion = 0
		incoming := entity.RequestLog{RequestID: "q/1", TimestampMs: 20, Status: entity.RequestStatusProcessing, RequestVersion: 0, Metadata: map[string]string{}}
		logStore.EXPECT().Insert(gomock.Any(), incoming).Return(nil)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(current, nil)
		summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			assert.Equal(t, entity.RequestStatusProcessing, updated.Status)
			return nil
		})
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(current), nil)
		queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), incoming))
	})

	t.Run("CAS conflict reloads and repairs winner", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(base, nil)
		summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(errs.ErrVersionMismatch)
		advanced := base
		advanced.Status = entity.RequestStatusLanded
		advanced.RequestVersion = 2
		advanced.StatusTimestampMs = 20
		advanced.Version = 2
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(advanced, nil)
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(base), nil)
		queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})

	t.Run("non-winning redelivery repairs stale queue projection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		advanced := base
		advanced.Status = entity.RequestStatusLanded
		advanced.RequestVersion = 2
		advanced.StatusTimestampMs = 20
		advanced.Version = 2
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(advanced, nil)
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(base), nil)
		queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})

	t.Run("first public event activates URI and queue projections", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, uriStore, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(base, nil)
		summaryStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(nil)
		activated := base
		activated.Status = entity.RequestStatusLanded
		activated.RequestVersion = 2
		activated.StatusTimestampMs = 20
		activated.Version = 2
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(entity.RequestQueueSummary{}, errs.ErrNotFound)
		uriStore.EXPECT().Create(gomock.Any(), entity.RequestURI{ChangeURI: "uri/1", ReceivedAtMs: 10, RequestID: "q/1"}).Return(nil)
		uriStore.EXPECT().Create(gomock.Any(), entity.RequestURI{ChangeURI: "uri/2", ReceivedAtMs: 10, RequestID: "q/1"}).Return(nil)
		queueStore.EXPECT().Create(gomock.Any(), queueSummaryFromSummary(activated)).Return(nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})

	t.Run("retry after projection failure appends another audit row", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		materializer := NewMaterializer(store)
		advanced := base
		advanced.Status = entity.RequestStatusLanded
		advanced.RequestVersion = 2
		advanced.StatusTimestampMs = 20
		advanced.Version = 2
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil).Times(2)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(advanced, nil).Times(2)
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(entity.RequestQueueSummary{}, errors.New("queue store down"))
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueSummaryFromSummary(advanced), nil)

		require.Error(t, materializer.PersistLog(context.Background(), log))
		require.NoError(t, materializer.PersistLog(context.Background(), log))
	})

	t.Run("missing authoritative summary fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, _, _, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.RequestSummary{}, errs.ErrNotFound)
		require.Error(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})

	t.Run("queue projection already ahead succeeds", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store, summaryStore, queueStore, _, logStore := materializerStores(ctrl)
		logStore.EXPECT().Insert(gomock.Any(), log).Return(nil)
		advanced := base
		advanced.Status = entity.RequestStatusLanded
		advanced.RequestVersion = 2
		advanced.StatusTimestampMs = 20
		advanced.Version = 2
		summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(advanced, nil)
		queueAhead := queueSummaryFromSummary(advanced)
		queueAhead.Version = 3
		queueStore.EXPECT().Get(gomock.Any(), "q", int64(10), "q/1").Return(queueAhead, nil)
		require.NoError(t, NewMaterializer(store).PersistLog(context.Background(), log))
	})
}

func TestLogWins(t *testing.T) {
	base := testRequestSummary()
	tests := []struct {
		name     string
		current  entity.RequestSummary
		incoming entity.RequestLog
		want     bool
	}{
		{
			name:    "accepted activates accepting receipt",
			current: entity.RequestSummary{Status: entity.RequestStatusAccepting, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusAccepted, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "started activates accepting receipt despite older timestamp",
			current: entity.RequestSummary{Status: entity.RequestStatusAccepting, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusStarted, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "late accepted does not replace started",
			current: entity.RequestSummary{Status: entity.RequestStatusStarted, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusAccepted, TimestampMs: 200,
			},
		},
		{
			name:    "versioned terminal beats newer unversioned status",
			current: entity.RequestSummary{Status: entity.RequestStatusProcessing, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 1, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "nonterminal cannot replace versioned terminal",
			current: entity.RequestSummary{Status: entity.RequestStatusLanded, RequestVersion: 1, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusProcessing, TimestampMs: 200,
			},
		},
		{
			name:    "higher terminal request version wins",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 1, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 100,
			},
			want: true,
		},
		{
			name:    "lower terminal request version loses",
			current: entity.RequestSummary{Status: entity.RequestStatusLanded, RequestVersion: 2, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusError, RequestVersion: 1, TimestampMs: 200,
			},
		},
		{
			name:    "equal terminal version uses later timestamp",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 2, StatusTimestampMs: 100},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 200,
			},
			want: true,
		},
		{
			name:    "without terminal winner later timestamp wins",
			current: base,
			incoming: entity.RequestLog{
				Status: entity.RequestStatusStarted, TimestampMs: base.StatusTimestampMs + 1,
			},
			want: true,
		},
		{
			name:    "exact version and timestamp tie keeps current winner",
			current: entity.RequestSummary{Status: entity.RequestStatusError, RequestVersion: 2, StatusTimestampMs: 200},
			incoming: entity.RequestLog{
				Status: entity.RequestStatusLanded, RequestVersion: 2, TimestampMs: 200,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, logWins(tt.incoming, tt.current))
		})
	}
}

func materializerStores(ctrl *gomock.Controller) (*storagemock.MockStorage, *storagemock.MockRequestSummaryStore, *storagemock.MockRequestQueueSummaryStore, *storagemock.MockRequestURIStore, *storagemock.MockRequestLogStore) {
	store := storagemock.NewMockStorage(ctrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	queueStore := storagemock.NewMockRequestQueueSummaryStore(ctrl)
	uriStore := storagemock.NewMockRequestURIStore(ctrl)
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore).AnyTimes()
	store.EXPECT().GetRequestQueueSummaryStore().Return(queueStore).AnyTimes()
	store.EXPECT().GetRequestURIStore().Return(uriStore).AnyTimes()
	store.EXPECT().GetRequestLogStore().Return(logStore).AnyTimes()
	return store, summaryStore, queueStore, uriStore, logStore
}

func testRequestSummary() entity.RequestSummary {
	return entity.RequestSummary{
		RequestID: "q/1", Queue: "q", ChangeURIs: []string{"uri/1", "uri/2"}, ReceivedAtMs: 10,
		Status: entity.RequestStatusAccepting, StatusTimestampMs: 10, Version: 1, Metadata: map[string]string{},
	}
}
