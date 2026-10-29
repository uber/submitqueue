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

	retained := log
	retained.TimestampMs = testNowMs
	logs.EXPECT().Create(gomock.Any(), retained).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, errors.New("summary unavailable"))
	require.Error(t, materializer.PersistLog(context.Background(), stores, log))

	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
	logs.EXPECT().Get(gomock.Any(), testRequestID, retained.ID).Return(retained, nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(seededRequestSummary(), nil)
	summaries.EXPECT().Update(gomock.Any(), entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
		State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: testNowMs, Version: 1,
	}, int32(1), int32(2)).Return(nil)
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
	summaries.EXPECT().Create(gomock.Any(), entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateAccepted, RequestVersion: 1, StateTimestampMs: testNowMs, Version: 1,
		AcceptedAtMs: testNowMs,
	}).Return(nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerCreatesMissingSummaryFromTerminalLog(t *testing.T) {
	materializer, stores, logs, summaries, requests := newTestMaterializer(t)
	request := entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateFailed, Version: 3,
	}
	log := NewRequestStateLog(request, entity.RequestOutcomeReasonBuildFailed)
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound)
	requests.EXPECT().Get(gomock.Any(), testRequestID).Return(request, nil)
	summaries.EXPECT().Create(gomock.Any(), entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI,
		State: entity.RequestStateFailed, RequestVersion: 3, StateTimestampMs: testNowMs,
		OutcomeReason: entity.RequestOutcomeReasonBuildFailed, Version: 1,
	}).Return(nil)
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
	current.AcceptedAtMs = testNowMs
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
	summaries.EXPECT().Update(gomock.Any(), entity.RequestSummary{
		RequestID: testRequestID, Queue: testQueue, URI: testRequestURI, BaseURI: testBaseURI,
		State: entity.RequestStateProcessing, RequestVersion: 2, StateTimestampMs: testNowMs, Version: 4,
	}, int32(4), int32(5)).Return(nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerRetriesRequestSummaryVersionMismatch(t *testing.T) {
	materializer, stores, requestLogs, summaries, _ := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, URI: testRequestURI,
		BuildStrategy: entity.BuildStrategyFull, State: entity.RequestStateProcessing, Version: 2,
	}, entity.RequestOutcomeReasonUnknown)
	current := seededRequestSummary()
	current.State = entity.RequestStateAccepted
	current.RequestVersion = 1
	current.StateTimestampMs = testNowMs - 1
	current.Version = 4
	winner := current
	winner.Version = 5
	winner.ProjectResultsCompletedAtMs = testNowMs - 500
	winner.ProjectCount = 3
	winner.BrokenProjectCount = 1
	updated := winner
	updated.State = log.State
	updated.RequestVersion = log.RequestVersion
	updated.StateTimestampMs = testNowMs
	firstAttempt := updated
	firstAttempt.Version = current.Version
	firstAttempt.ProjectResultsCompletedAtMs = 0
	firstAttempt.ProjectCount = 0
	firstAttempt.BrokenProjectCount = 0
	gomock.InOrder(
		requestLogs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
		summaries.EXPECT().Update(gomock.Any(), firstAttempt, int32(4), int32(5)).Return(storage.ErrVersionMismatch),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(winner, nil),
		summaries.EXPECT().Update(gomock.Any(), updated, int32(5), int32(6)).Return(nil),
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
		AcceptedAtMs: testNowMs,
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
	updated := current
	updated.State = log.State
	updated.RequestVersion = log.RequestVersion
	updated.StateTimestampMs = testNowMs
	updated.OutcomeReason = log.OutcomeReason
	summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil)
	summaries.EXPECT().Update(gomock.Any(), updated, int32(4), int32(5)).Return(nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestUpdateRequestSummaryAcceptanceTime(t *testing.T) {
	log := entity.RequestLog{
		RequestID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted,
		RequestVersion: 1, TimestampMs: testNowMs - 1000,
	}
	terminal := seededRequestSummary()
	terminal.BaseURI = testBaseURI
	terminal.State = entity.RequestStateFailed
	terminal.RequestVersion = 3
	terminal.StateTimestampMs = testNowMs
	terminal.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
	terminal.Version = 4
	accepted := seededRequestSummary()
	accepted.State = log.State
	accepted.RequestVersion = log.RequestVersion
	accepted.StateTimestampMs = log.TimestampMs

	tests := []struct {
		name         string
		current      entity.RequestSummary
		acceptedAtMs int64
		wantErr      bool
	}{
		{name: "older acceptance repairs timestamp only", current: terminal},
		{name: "known acceptance is idempotent", current: terminal, acceptedAtMs: log.TimestampMs},
		{name: "same version acceptance repairs timestamp", current: accepted},
		{name: "same version known acceptance is idempotent", current: accepted, acceptedAtMs: log.TimestampMs},
		{name: "conflicting acceptance is rejected", current: terminal, acceptedAtMs: log.TimestampMs - 1, wantErr: true},
		{name: "same version conflicting acceptance is rejected", current: accepted, acceptedAtMs: log.TimestampMs - 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := tt.current
			current.AcceptedAtMs = tt.acceptedAtMs
			want := current
			want.AcceptedAtMs = log.TimestampMs
			summaries := storagemock.NewMockRequestSummaryStore(gomock.NewController(t))
			if !tt.wantErr && want != current {
				summaries.EXPECT().Update(gomock.Any(), want, current.Version, current.Version+1).Return(nil)
			}

			_, err := updateExistingRequestSummary(context.Background(), summaries, current, log)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateRequestSummaryOutcomeReason(t *testing.T) {
	baseline := seededRequestSummary()
	baseline.BaseURI = testBaseURI
	baseline.State = entity.RequestStateFailed
	baseline.RequestVersion = 3
	baseline.StateTimestampMs = testNowMs
	baseline.AcceptedAtMs = testNowMs - 1000
	baseline.Version = 4
	buildFailed := entity.RequestOutcomeReasonBuildFailed
	processingFailed := entity.RequestOutcomeReasonProcessingFailed

	tests := []struct {
		name          string
		currentReason entity.RequestOutcomeReason
		logVersion    int32
		wantReason    entity.RequestOutcomeReason
		wantErr       bool
	}{
		{name: "same version repairs reason", logVersion: 3, wantReason: buildFailed},
		{name: "known reason is idempotent", currentReason: buildFailed, logVersion: 3, wantReason: buildFailed},
		{name: "older reason cannot overwrite winner", currentReason: processingFailed, logVersion: 2, wantReason: processingFailed},
		{name: "conflicting reason is rejected", currentReason: processingFailed, logVersion: 3, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := baseline
			current.OutcomeReason = tt.currentReason
			log := entity.RequestLog{
				RequestID: testRequestID, Queue: testQueue, State: entity.RequestStateFailed,
				RequestVersion: tt.logVersion, TimestampMs: testNowMs, OutcomeReason: buildFailed,
			}
			want := current
			want.OutcomeReason = tt.wantReason
			summaries := storagemock.NewMockRequestSummaryStore(gomock.NewController(t))
			if !tt.wantErr && want != current {
				summaries.EXPECT().Update(gomock.Any(), want, current.Version, current.Version+1).Return(nil)
			}

			_, err := updateExistingRequestSummary(context.Background(), summaries, current, log)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateRequestSummaryNewerState(t *testing.T) {
	current := seededRequestSummary()
	current.BaseURI = testBaseURI
	current.State = entity.RequestStateAccepted
	current.RequestVersion = 1
	current.StateTimestampMs = testNowMs - 1000
	current.AcceptedAtMs = testNowMs - 1000
	current.ProjectResultsCompletedAtMs = testNowMs - 500
	current.Version = 4

	tests := []struct {
		name        string
		metadata    map[string]string
		wantBaseURI string
	}{
		{name: "omitted baseline is preserved", wantBaseURI: testBaseURI},
		{name: "full build clears baseline", metadata: map[string]string{MetadataKeyBaseURI: ""}},
		{name: "new baseline replaces previous", metadata: map[string]string{MetadataKeyBaseURI: "git://repo/new-base"}, wantBaseURI: "git://repo/new-base"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := entity.RequestLog{
				RequestID: testRequestID, Queue: testQueue, State: entity.RequestStateFailed,
				RequestVersion: 3, TimestampMs: testNowMs, OutcomeReason: entity.RequestOutcomeReasonBuildFailed,
				Metadata: tt.metadata,
			}
			want := current
			want.State = entity.RequestStateFailed
			want.RequestVersion = 3
			want.StateTimestampMs = testNowMs
			want.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
			want.BaseURI = tt.wantBaseURI
			summaries := storagemock.NewMockRequestSummaryStore(gomock.NewController(t))
			summaries.EXPECT().Update(gomock.Any(), want, current.Version, current.Version+1).Return(nil)

			_, err := updateExistingRequestSummary(context.Background(), summaries, current, log)
			require.NoError(t, err)
		})
	}
}

func TestMaterializerRetriesAcceptanceRepairAfterNewerStateWins(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1,
	}, entity.RequestOutcomeReasonUnknown)
	log.TimestampMs = testNowMs - 1000
	current := seededRequestSummary()
	current.State = entity.RequestStateProcessing
	current.RequestVersion = 2
	current.StateTimestampMs = testNowMs
	updated := current
	updated.AcceptedAtMs = log.TimestampMs
	winner := current
	winner.State = entity.RequestStateFailed
	winner.RequestVersion = 3
	winner.StateTimestampMs++
	winner.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
	winner.Version = 2
	updatedWinner := winner
	updatedWinner.AcceptedAtMs = log.TimestampMs
	gomock.InOrder(
		logs.EXPECT().Create(gomock.Any(), log).Return(storage.ErrAlreadyExists),
		logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(log, nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
		summaries.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(storage.ErrVersionMismatch),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(winner, nil),
		summaries.EXPECT().Update(gomock.Any(), updatedWinner, int32(2), int32(3)).Return(nil),
	)
	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerRetriesAcceptanceRepairAfterPartialWrite(t *testing.T) {
	materializer, stores, logs, summaries, _ := newTestMaterializer(t)
	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1,
	}, entity.RequestOutcomeReasonUnknown)
	retained := log
	retained.TimestampMs = testNowMs
	gomock.InOrder(
		logs.EXPECT().Create(gomock.Any(), retained).Return(nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, errors.New("unavailable")),
	)
	require.Error(t, materializer.PersistLog(context.Background(), stores, log))

	materializer.now = func() time.Time { return time.UnixMilli(testNowMs + 1000) }
	candidate := log
	candidate.TimestampMs = testNowMs + 1000
	current := seededRequestSummary()
	current.State = entity.RequestStateProcessing
	current.RequestVersion = 2
	current.StateTimestampMs = testNowMs + 500
	updated := current
	updated.AcceptedAtMs = retained.TimestampMs
	gomock.InOrder(
		logs.EXPECT().Create(gomock.Any(), candidate).Return(storage.ErrAlreadyExists),
		logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(retained, nil),
		summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
		summaries.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil),
	)
	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerProjectsProjectCompletion(t *testing.T) {
	for _, tt := range []struct {
		name         string
		event        entity.RequestEvent
		completedAt  int64
		count        string
		projectCount int32
	}{
		{name: "completed projects", event: entity.RequestEventProjectFactsRecorded, count: "3", projectCount: 3},
		{name: "completed empty set", event: entity.RequestEventProjectFactsRecorded, count: "0"},
		{name: "duplicate completion", event: entity.RequestEventProjectFactsRecorded, completedAt: testNowMs, count: "3", projectCount: 3},
		{name: "recording after abandonment", event: entity.RequestEventProjectFactsRecorded, completedAt: testNowMs - 1, count: "3", projectCount: 3},
		{name: "delayed recording after abandonment", event: entity.RequestEventProjectFactsRecorded, completedAt: testNowMs + 1, count: "3", projectCount: 3},
		{name: "abandoned recording", event: entity.RequestEventRecordAbandoned},
		{name: "duplicate abandonment", event: entity.RequestEventRecordAbandoned, completedAt: testNowMs},
		{name: "abandonment after recording", event: entity.RequestEventRecordAbandoned, completedAt: testNowMs - 1},
		{name: "delayed abandonment after recording", event: entity.RequestEventRecordAbandoned, completedAt: testNowMs + 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			materializer, stores, logs, summaries, _ := newTestMaterializer(t)
			var metadata map[string]string
			if tt.event == entity.RequestEventProjectFactsRecorded {
				metadata = map[string]string{MetadataKeyProjectFactCount: tt.count, MetadataKeyBrokenProjectFactCount: "0"}
			}
			log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, tt.event, "test", metadata)
			log.TimestampMs = testNowMs
			current := seededRequestSummary()
			current.State = entity.RequestStateFailed
			current.RequestVersion = 3
			current.StateTimestampMs = testNowMs - 1000
			current.BaseURI = testBaseURI
			current.AcceptedAtMs = testNowMs - 2000
			current.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
			current.ProjectResultsCompletedAtMs = tt.completedAt
			current.ProjectCount = 5
			current.BrokenProjectCount = 2
			want := current
			if want.ProjectResultsCompletedAtMs == 0 {
				want.ProjectResultsCompletedAtMs = log.TimestampMs
			}
			if tt.event == entity.RequestEventProjectFactsRecorded {
				want.ProjectCount = tt.projectCount
				want.BrokenProjectCount = 0
			}
			logs.EXPECT().Create(gomock.Any(), log).Return(storage.ErrAlreadyExists)
			logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(log, nil)
			summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil)
			if want != current {
				summaries.EXPECT().Update(gomock.Any(), want, current.Version, current.Version+1).Return(nil)
			}
			require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
		})
	}
}

func TestMaterializerRepairsCompletionAfterProjectionFailure(t *testing.T) {
	for _, event := range []entity.RequestEvent{entity.RequestEventProjectFactsRecorded, entity.RequestEventRecordAbandoned} {
		t.Run(string(event), func(t *testing.T) {
			materializer, stores, logs, summaries, _ := newTestMaterializer(t)
			log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, event, "test", map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "1"})
			retained := log
			retained.TimestampMs = testNowMs
			current := seededRequestSummary()
			current.State = entity.RequestStateFailed
			current.RequestVersion = 3
			current.StateTimestampMs = testNowMs - 1000
			updated := current
			updated.ProjectResultsCompletedAtMs = retained.TimestampMs
			if event == entity.RequestEventProjectFactsRecorded {
				updated.ProjectCount = 3
				updated.BrokenProjectCount = 1
			}
			writeErr := errors.New("summary unavailable")
			gomock.InOrder(
				logs.EXPECT().Create(gomock.Any(), retained).Return(nil),
				summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
				summaries.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(writeErr),
			)
			require.ErrorIs(t, materializer.PersistLog(context.Background(), stores, log), writeErr)

			materializer.now = func() time.Time { return time.UnixMilli(testNowMs + 1000) }
			candidate := log
			candidate.TimestampMs = testNowMs + 1000
			gomock.InOrder(
				logs.EXPECT().Create(gomock.Any(), candidate).Return(storage.ErrAlreadyExists),
				logs.EXPECT().Get(gomock.Any(), testRequestID, log.ID).Return(retained, nil),
				summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
				summaries.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil),
			)
			require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
		})
	}
}

func TestMaterializerRetriesCompletionAfterStateProjectionWins(t *testing.T) {
	for _, event := range []entity.RequestEvent{entity.RequestEventProjectFactsRecorded, entity.RequestEventRecordAbandoned} {
		t.Run(string(event), func(t *testing.T) {
			materializer, stores, logs, summaries, _ := newTestMaterializer(t)
			log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, event, "test", map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "1"})
			log.TimestampMs = testNowMs
			current := seededRequestSummary()
			current.State = entity.RequestStateProcessing
			current.RequestVersion = 2
			updated := current
			updated.ProjectResultsCompletedAtMs = log.TimestampMs
			if event == entity.RequestEventProjectFactsRecorded {
				updated.ProjectCount = 3
				updated.BrokenProjectCount = 1
			}
			winner := current
			winner.State = entity.RequestStateFailed
			winner.RequestVersion = 3
			winner.StateTimestampMs = testNowMs + 1
			winner.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
			winner.Version++
			updatedWinner := winner
			updatedWinner.ProjectResultsCompletedAtMs = log.TimestampMs
			if event == entity.RequestEventProjectFactsRecorded {
				updatedWinner.ProjectCount = 3
				updatedWinner.BrokenProjectCount = 1
			}
			gomock.InOrder(
				logs.EXPECT().Create(gomock.Any(), log).Return(nil),
				summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(current, nil),
				summaries.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(storage.ErrVersionMismatch),
				summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(winner, nil),
				summaries.EXPECT().Update(gomock.Any(), updatedWinner, int32(2), int32(3)).Return(nil),
			)
			require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
		})
	}
}

func TestMaterializerCompletionRequiresExistingSummary(t *testing.T) {
	for _, event := range []entity.RequestEvent{entity.RequestEventProjectFactsRecorded, entity.RequestEventRecordAbandoned} {
		t.Run(string(event), func(t *testing.T) {
			materializer, stores, logs, summaries, _ := newTestMaterializer(t)
			log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, event, "test", nil)
			logs.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			summaries.EXPECT().Get(gomock.Any(), testRequestID).Return(entity.RequestSummary{}, storage.ErrNotFound)
			require.ErrorIs(t, materializer.PersistLog(context.Background(), stores, log), storage.ErrNotFound)
		})
	}
}

func TestProjectRequestSummaryProjectCounts(t *testing.T) {
	for _, tt := range []struct {
		name          string
		metadata      map[string]string
		count, broken int32
		wantErr       bool
	}{
		{name: "mixed outcomes", metadata: map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "2"}, count: 3, broken: 2},
		{name: "all green", metadata: map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "0"}, count: 3},
		{name: "empty results", metadata: map[string]string{MetadataKeyProjectFactCount: "0", MetadataKeyBrokenProjectFactCount: "0"}},
		{name: "missing broken", metadata: map[string]string{MetadataKeyProjectFactCount: "3"}, wantErr: true},
		{name: "missing counts", wantErr: true},
		{name: "missing total", metadata: map[string]string{MetadataKeyBrokenProjectFactCount: "1"}, wantErr: true},
		{name: "invalid total", metadata: map[string]string{MetadataKeyProjectFactCount: "oops", MetadataKeyBrokenProjectFactCount: "1"}, wantErr: true},
		{name: "invalid broken", metadata: map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "oops"}, wantErr: true},
		{name: "negative total", metadata: map[string]string{MetadataKeyProjectFactCount: "-1", MetadataKeyBrokenProjectFactCount: "0"}, wantErr: true},
		{name: "negative broken", metadata: map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "-1"}, wantErr: true},
		{name: "broken exceeds total", metadata: map[string]string{MetadataKeyProjectFactCount: "3", MetadataKeyBrokenProjectFactCount: "4"}, wantErr: true},
		{name: "overflow", metadata: map[string]string{MetadataKeyProjectFactCount: "2147483648", MetadataKeyBrokenProjectFactCount: "1"}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := seededRequestSummary()
			log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, entity.RequestEventProjectFactsRecorded, "all", tt.metadata)
			log.TimestampMs = testNowMs
			got, err := projectRequestSummary(current, log)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			want := current
			want.ProjectResultsCompletedAtMs = testNowMs
			want.ProjectCount = tt.count
			want.BrokenProjectCount = tt.broken
			require.Equal(t, want, got)
			replayed, err := projectRequestSummary(got, log)
			require.NoError(t, err)
			require.Equal(t, got, replayed)
		})
	}
}

func TestProjectRequestSummaryUsesRetainedCounts(t *testing.T) {
	current := seededRequestSummary()
	current.ProjectResultsCompletedAtMs = testNowMs
	current.ProjectCount = 3
	current.BrokenProjectCount = 1
	log := NewRequestEventLog(entity.Request{ID: testRequestID, Queue: testQueue}, entity.RequestEventProjectFactsRecorded, "all", map[string]string{
		MetadataKeyProjectFactCount: "4", MetadataKeyBrokenProjectFactCount: "2",
	})
	log.TimestampMs = testNowMs
	got, err := projectRequestSummary(current, log)
	require.NoError(t, err)
	want := current
	want.ProjectCount = 4
	want.BrokenProjectCount = 2
	require.Equal(t, want, got)
}
