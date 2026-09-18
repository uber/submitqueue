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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestMaterializerAcceptsStateLogWithoutMetadata(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	log := entity.RequestLog{
		ID: "state/1", Queue: testQueue, RequestID: testRequestID, State: entity.RequestStateAccepted, RequestVersion: 1,
	}
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	expectRequestSummaryUpdate(summaries)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerRepairsSummaryAfterPartialWrite(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	request := entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
		BuildStrategy: entity.BuildStrategyIncrementalSinceGreen,
		State:         entity.RequestStateProcessing,
		Version:       2,
	}
	log := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)

	var retained entity.RequestLog
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, candidate entity.RequestLog) error {
		retained = candidate
		return nil
	})
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, errors.New("summary unavailable"))
	require.Error(t, materializer.PersistLog(context.Background(), stores, log))

	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	logs.EXPECT().Get(gomock.Any(), testRequestID, retained.ID).Return(retained, nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(seededRequestSummary(), nil)
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).DoAndReturn(func(_ context.Context, summary entity.RequestSummary, _, _ int32) error {
		assert.Equal(t, entity.RequestSummary{
			RequestID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
			State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: testNowMs, Version: 1,
		}, summary)
		return nil
	})
	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerCreatesMissingSummaryFromRequest(t *testing.T) {
	materializer, stores, logs, summaries, requests := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI, State: entity.RequestStateAccepted, Version: 1,
	}, entity.RequestOutcomeReasonUnknown)
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound)
	requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		BuildStrategy: entity.BuildStrategyIncrementalSinceGreen, BaseURI: testBaseURI,
		State: entity.RequestStateProcessing, Version: 2,
	}, nil)
	summaries.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, summary entity.RequestSummary) error {
		assert.Equal(t, entity.RequestSummary{
			RequestID: testRequestID, Queue: testQueue, URI: testRequestURI,
			State: entity.RequestStateAccepted, RequestVersion: 1, StateTimestampMs: testNowMs, Version: 1,
		}, summary)
		return nil
	})

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerMissingSummaryCreateRace(t *testing.T) {
	materializer, stores, logs, summaries, requests := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI, State: entity.RequestStateAccepted, Version: 1,
	}, entity.RequestOutcomeReasonUnknown)
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound)
	requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
	}, nil)
	summaries.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	current := seededRequestSummary()
	current.State = entity.RequestStateAccepted
	current.RequestVersion = 1
	current.StateTimestampMs = testNowMs
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerMissingSummaryFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*storagemock.MockRequestStore, *storagemock.MockRequestSummaryStore)
	}{
		{
			name: "request unavailable",
			setup: func(requests *storagemock.MockRequestStore, _ *storagemock.MockRequestSummaryStore) {
				requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{}, errors.New("request unavailable"))
			},
		},
		{
			name: "summary create",
			setup: func(requests *storagemock.MockRequestStore, summaries *storagemock.MockRequestSummaryStore) {
				requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{
					ID: testRequestID, Queue: testQueue, URI: testRequestURI,
				}, nil)
				summaries.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("summary unavailable"))
			},
		},
		{
			name: "request identity conflict",
			setup: func(requests *storagemock.MockRequestStore, _ *storagemock.MockRequestSummaryStore) {
				requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{
					ID: "other-request", Queue: testQueue, URI: testRequestURI,
				}, nil)
			},
		},
		{
			name: "request URI empty",
			setup: func(requests *storagemock.MockRequestStore, _ *storagemock.MockRequestSummaryStore) {
				requests.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.Request{
					ID: testRequestID, Queue: testQueue,
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			materializer, stores, logs, summaries, requests := newTestMaterializer(t)
			log := NewRequestStateLog(entity.Request{
				ID: testRequestID, Queue: testQueue, URI: testRequestURI, State: entity.RequestStateAccepted, Version: 1,
			}, entity.RequestOutcomeReasonUnknown)
			logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound)
			tt.setup(requests, summaries)

			require.Error(t, materializer.PersistLog(context.Background(), stores, log))
		})
	}
}

func TestMaterializerProjectsOnlyNewerRequestState(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	request := entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
		BuildStrategy: entity.BuildStrategyIncrementalSinceGreen, State: entity.RequestStateProcessing, Version: 2,
	}
	log := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateAccepted, RequestVersion: 1, StateTimestampMs: testNowMs - 1, Version: 4,
	}, nil)
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(4), int32(5)).DoAndReturn(
		func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			assert.Equal(t, entity.RequestStateProcessing, updated.State)
			assert.Equal(t, testBaseURI, updated.BaseURI)
			assert.Equal(t, int32(2), updated.RequestVersion)
			assert.Equal(t, int32(4), updated.Version)
			return nil
		},
	)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerRetriesRequestSummaryVersionMismatch(t *testing.T) {
	materializer, stores, requestLogs, summaries, _ := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		BuildStrategy: entity.BuildStrategyFull, State: entity.RequestStateProcessing, Version: 2,
	}, entity.RequestOutcomeReasonUnknown)
	requestLogs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)

	current := seededRequestSummary()
	current.State = entity.RequestStateAccepted
	current.RequestVersion = 1
	current.StateTimestampMs = testNowMs - 1
	current.Version = 4
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil)
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(4), int32(5)).Return(storage.ErrVersionMismatch)

	winner := current
	winner.Version = 5
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(winner, nil)
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(5), int32(6)).DoAndReturn(
		func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			assert.Equal(t, int32(5), updated.Version)
			assert.Equal(t, entity.RequestStateProcessing, updated.State)
			return nil
		},
	)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerRejectsConflictingRequestSummary(t *testing.T) {
	tests := []struct {
		name    string
		current entity.RequestSummary
	}{
		{
			name: "identity",
			current: entity.RequestSummary{
				RequestID: "other-request", Queue: testQueue, URI: testRequestURI, Version: 1,
			},
		},
		{
			name: "same request version with different state",
			current: entity.RequestSummary{
				RequestID: testRequestID, Queue: testQueue, URI: testRequestURI,
				State: entity.RequestStateProcessing, RequestVersion: 1, StateTimestampMs: testNowMs, Version: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			materializer, stores, requestLogs, summaries, _ := newTestMaterializer(t)
			log := NewRequestStateLog(entity.Request{
				ID: testRequestID, Queue: testQueue, URI: testRequestURI,
				State: entity.RequestStateAccepted, Version: 1,
			}, entity.RequestOutcomeReasonUnknown)
			requestLogs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(tt.current, nil)

			require.Error(t, materializer.PersistLog(context.Background(), stores, log))
		})
	}
}

func TestMaterializerIgnoresOlderStateForSummary(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	request := entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateAccepted, Version: 1,
	}
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
		State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: testNowMs + 1, Version: 2,
	}, nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)))
}

func TestMaterializerPreservesBaseURIWhenLogOmitsIt(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	request := entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateFailed, Version: 3,
	}
	log := NewRequestStateLog(request, entity.RequestOutcomeReasonProcessingFailed)
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	current := seededRequestSummary()
	current.BaseURI = testBaseURI
	current.State = entity.RequestStateProcessing
	current.RequestVersion = 2
	current.StateTimestampMs = testNowMs - 1
	current.Version = 4
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil)
	summaries.EXPECT().Update(gomock.Any(), gomock.Any(), int32(4), int32(5)).DoAndReturn(
		func(_ context.Context, updated entity.RequestSummary, _, _ int32) error {
			assert.Equal(t, testBaseURI, updated.BaseURI)
			return nil
		},
	)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}
