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

package requestlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

type acceptanceMaterializerFixture struct {
	materializer *materializer
	stores       *storagemock.MockStorage
	logs         *storagemock.MockRequestLogStore
	summaries    *storagemock.MockRequestSummaryStore
	requests     *storagemock.MockRequestStore
	acceptances  *storagemock.MockRequestAcceptanceStore
}

func newAcceptanceMaterializerFixture(t *testing.T) acceptanceMaterializerFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := acceptanceMaterializerFixture{
		materializer: &materializer{scope: tally.NoopScope, now: func() time.Time { return time.UnixMilli(testNowMs) }},
		stores:       storagemock.NewMockStorage(ctrl),
		logs:         storagemock.NewMockRequestLogStore(ctrl),
		summaries:    storagemock.NewMockRequestSummaryStore(ctrl),
		requests:     storagemock.NewMockRequestStore(ctrl),
		acceptances:  storagemock.NewMockRequestAcceptanceStore(ctrl),
	}
	f.stores.EXPECT().GetRequestLogStore().Return(f.logs).AnyTimes()
	f.stores.EXPECT().GetRequestSummaryStore().Return(f.summaries).AnyTimes()
	f.stores.EXPECT().GetRequestStore().Return(f.requests).AnyTimes()
	f.stores.EXPECT().GetRequestAcceptanceStore().Return(f.acceptances).AnyTimes()
	return f
}

func TestMaterializerEnsuresAcceptanceMapping(t *testing.T) {
	accepted := entity.RequestLog{
		ID: "state/1", Queue: testQueue, RequestID: testRequestID, TimestampMs: testNowMs,
		State: entity.RequestStateAccepted, RequestVersion: 1,
	}
	processing := accepted
	processing.ID = "state/2"
	processing.State = entity.RequestStateProcessing
	processing.RequestVersion = 2
	processing.TimestampMs++
	current := entity.RequestSummary{
		Queue: testQueue, RequestID: testRequestID, URI: testRequestURI, State: entity.RequestStateProcessing,
		RequestVersion: 2, StateTimestampMs: processing.TimestampMs, Version: 2,
	}
	known := current
	known.AcceptedAtMs = accepted.TimestampMs
	for _, tt := range []struct {
		name    string
		current entity.RequestSummary
		log     entity.RequestLog
		updates bool
		maps    bool
	}{
		{"older acceptance fills missing fact", current, accepted, true, true},
		{"older acceptance with known fact", known, accepted, false, true},
		{"current processing with known fact", known, processing, false, true},
		{"current processing with unknown fact", current, processing, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newAcceptanceMaterializerFixture(t)
			calls := []any{
				f.logs.EXPECT().Create(gomock.Any(), tt.log).Return(nil),
				f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(tt.current, nil),
			}
			if tt.updates {
				calls = append(calls, f.summaries.EXPECT().Update(gomock.Any(), known, current.Version, current.Version+1).Return(nil))
			}
			if tt.maps {
				calls = append(calls, f.acceptances.EXPECT().Create(gomock.Any(), entity.RequestAcceptance{
					Queue: testQueue, RequestID: testRequestID, AcceptedAtMs: accepted.TimestampMs,
				}).Return(storage.ErrAlreadyExists))
			}
			gomock.InOrder(calls...)
			require.NoError(t, f.materializer.PersistLog(context.Background(), f.stores, tt.log))
		})
	}
}

func TestMaterializerRetriesAcceptanceMappingAfterPartialWrite(t *testing.T) {
	for _, createSummary := range []bool{true, false} {
		name := "summary update"
		if createSummary {
			name = "summary create"
		}
		t.Run(name, func(t *testing.T) {
			f := newAcceptanceMaterializerFixture(t)
			ctx := context.Background()
			request := entity.Request{ID: testRequestID, Queue: testQueue, URI: testRequestURI, State: entity.RequestStateAccepted, Version: 1}
			log := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
			log.TimestampMs = testNowMs
			persisted := requestSummaryFromRequestAndLog(request, log)
			persisted.Version = 1
			mapping := entity.RequestAcceptance{Queue: testQueue, RequestID: testRequestID, AcceptedAtMs: testNowMs}
			mappingErr := errors.New("mapping unavailable")
			calls := []any{f.logs.EXPECT().Create(gomock.Any(), log).Return(nil)}
			if createSummary {
				calls = append(calls,
					f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound),
					f.requests.EXPECT().Get(gomock.Any(), testRequestID).Return(request, nil),
					f.summaries.EXPECT().Create(gomock.Any(), persisted).Return(nil),
				)
			} else {
				current := persisted
				current.AcceptedAtMs = 0
				calls = append(calls,
					f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
					f.summaries.EXPECT().Update(gomock.Any(), persisted, int32(1), int32(2)).Return(nil),
				)
				persisted.Version = 2
			}
			calls = append(calls, f.acceptances.EXPECT().Create(gomock.Any(), mapping).Return(mappingErr))
			gomock.InOrder(calls...)
			require.ErrorIs(t, f.materializer.PersistLog(ctx, f.stores, log), mappingErr)

			f.materializer.now = func() time.Time { return time.UnixMilli(testNowMs + 1000) }
			retry := log
			retry.TimestampMs = 0
			candidate := log
			candidate.TimestampMs += 1000
			gomock.InOrder(
				f.logs.EXPECT().Create(gomock.Any(), candidate).Return(storage.ErrAlreadyExists),
				f.logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(log, nil),
				f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(persisted, nil),
				f.acceptances.EXPECT().Create(gomock.Any(), mapping).Return(nil),
			)
			require.NoError(t, f.materializer.PersistLog(ctx, f.stores, retry))
		})
	}
}

func TestMaterializerDoesNotMapFailedSummaryWrite(t *testing.T) {
	f := newAcceptanceMaterializerFixture(t)
	log := entity.RequestLog{
		ID: "state/1", Queue: testQueue, RequestID: testRequestID, TimestampMs: testNowMs,
		State: entity.RequestStateAccepted, RequestVersion: 1,
	}
	writeErr := errors.New("summary unavailable")
	gomock.InOrder(
		f.logs.EXPECT().Create(gomock.Any(), log).Return(nil),
		f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(seededRequestSummary(), nil),
		f.summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(writeErr),
	)
	require.ErrorIs(t, f.materializer.PersistLog(context.Background(), f.stores, log), writeErr)
}

func TestMaterializerDoesNotMapConflictingAcceptance(t *testing.T) {
	f := newAcceptanceMaterializerFixture(t)
	log := entity.RequestLog{
		ID: "state/1", Queue: testQueue, RequestID: testRequestID, TimestampMs: testNowMs,
		State: entity.RequestStateAccepted, RequestVersion: 1,
	}
	current := seededRequestSummary()
	current.AcceptedAtMs = testNowMs - 1
	gomock.InOrder(
		f.logs.EXPECT().Create(gomock.Any(), log).Return(nil),
		f.summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
	)
	require.Error(t, f.materializer.PersistLog(context.Background(), f.stores, log))
}
