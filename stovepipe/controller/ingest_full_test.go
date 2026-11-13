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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

func TestIngestFullAcceptedRequest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		create          bool
		createRace      bool
		alreadyFull     bool
		ordinary        bool
		conflict        bool
		concurrentForce bool
	}{
		{name: "new head starts with full strategy", create: true},
		{name: "same waiting head is updated"},
		{name: "repeated force preserves full strategy", alreadyFull: true},
		{name: "ordinary poll preserves full strategy", alreadyFull: true, ordinary: true},
		{name: "concurrent ordinary create is upgraded", create: true, createRace: true},
		{name: "force retries accepted version conflict", conflict: true},
		{name: "concurrent force converges", conflict: true, concurrentForce: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newIngestController(t, ctrl)
			expectHeadAndEnablement(m)
			if tc.create {
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("", storage.ErrNotFound)
				m.counter.EXPECT().Next(gomock.Any(), counterDomainRequest).Return(int64(7), nil)
				m.uriStore.EXPECT().Create(gomock.Any(), testURI, "7").Return(nil)
			} else {
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil)
			}
			current := acceptedRequest("7")
			if tc.alreadyFull {
				current.BuildStrategy = entity.BuildStrategyFull
			}
			var persisted *gomock.Call
			if tc.create {
				m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(entity.Request{}, storage.ErrNotFound)
				created := current
				created.BuildStrategy = entity.BuildStrategyFull
				if tc.createRace {
					create := m.reqStore.EXPECT().Create(gomock.Any(), created).Return(storage.ErrAlreadyExists)
					m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(current, nil).After(create)
				} else {
					persisted = m.reqStore.EXPECT().Create(gomock.Any(), created).Return(nil)
					current = created
				}
			} else {
				m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(current, nil)
			}
			if current.BuildStrategy != entity.BuildStrategyFull {
				updated := current
				updated.BuildStrategy = entity.BuildStrategyFull
				if tc.conflict {
					first := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(storage.ErrVersionMismatch)
					current.Version = 2
					if tc.concurrentForce {
						current.BuildStrategy = entity.BuildStrategyFull
					}
					m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(current, nil).After(first)
					updated = current
					updated.BuildStrategy = entity.BuildStrategyFull
				}
				if !tc.concurrentForce {
					persisted = m.reqStore.EXPECT().Update(gomock.Any(), updated, current.Version, current.Version+1).Return(nil)
					updated.Version++
					current = updated
				}
			}
			logCall := m.materializer.EXPECT().PersistLog(gomock.Any(), m.store,
				requestlog.NewRequestStateLog(acceptedRequest("7"), entity.RequestOutcomeReasonUnknown)).Return(nil)
			if persisted != nil {
				logCall.After(persisted)
			}
			expectAdvanceLatestRequestIDNoOp(m, testQueue, "7")
			m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil).After(logCall)

			result, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, ForceFull: !tc.ordinary})
			require.NoError(t, err)
			assert.Equal(t, "7", result.ID)
		})
	}
}

func TestIngestFullPreservesAcceptanceHistory(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newIngestController(t, ctrl)
	c.materializer = requestlog.NewMaterializer(tally.NoopScope)
	expectHeadAndEnablement(m)
	m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil)
	request := acceptedRequest("7")
	m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(request, nil)
	updated := request
	updated.BuildStrategy = entity.BuildStrategyFull
	m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)

	logs := storagemock.NewMockRequestLogStore(ctrl)
	summaries := storagemock.NewMockRequestSummaryStore(ctrl)
	acceptances := storagemock.NewMockRequestAcceptanceStore(ctrl)
	m.store.EXPECT().GetRequestLogStore().Return(logs)
	m.store.EXPECT().GetRequestSummaryStore().Return(summaries)
	m.store.EXPECT().GetRequestAcceptanceStore().Return(acceptances)
	original := requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
	original.TimestampMs = 1000
	logs.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log entity.RequestLog) error {
		assert.Equal(t, original.ID, log.ID)
		return storage.ErrAlreadyExists
	})
	logs.EXPECT().Get(gomock.Any(), "7", original.ID).Return(original, nil)
	summaries.EXPECT().Get(gomock.Any(), "7").Return(entity.RequestSummary{
		RequestID: "7", Queue: testQueue, URI: testURI, State: entity.RequestStateAccepted,
		RequestVersion: 1, AcceptedAtMs: 1000, StateTimestampMs: 1000, Version: 1,
	}, nil)
	acceptances.EXPECT().Create(gomock.Any(), entity.RequestAcceptance{
		Queue: testQueue, RequestID: "7", AcceptedAtMs: 1000,
	}).Return(storage.ErrAlreadyExists)
	expectAdvanceLatestRequestIDNoOp(m, testQueue, "7")
	m.publisher.EXPECT().Publish(gomock.Any(), "process", gomock.Any()).Return(nil)

	result, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, ForceFull: true})
	require.NoError(t, err)
	assert.Equal(t, "7", result.ID)
}

func TestIngestFullRejectsStartedOrTerminalRequest(t *testing.T) {
	for _, state := range []entity.RequestState{
		entity.RequestStateProcessing, entity.RequestStateSuperseded,
		entity.RequestStateSucceeded, entity.RequestStateFailed, entity.RequestStateCancelled,
	} {
		for _, strategy := range []entity.BuildStrategy{entity.BuildStrategyIncrementalSinceGreen, entity.BuildStrategyFull} {
			t.Run(string(state)+"/"+string(strategy), func(t *testing.T) {
				ctrl := gomock.NewController(t)
				c, m := newIngestController(t, ctrl)
				expectHeadAndEnablement(m)
				m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil)
				request := acceptedRequest("7")
				request.State, request.BuildStrategy = state, strategy
				m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(request, nil)

				_, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, ForceFull: true})
				require.ErrorIs(t, err, ErrInvalidRequest)
			})
		}
	}
}

func TestIngestFullUpdateFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		conflict    bool
		reloadError bool
	}{
		{name: "storage failure prevents publication"},
		{name: "admission wins the race", conflict: true},
		{name: "failed reload prevents publication", conflict: true, reloadError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newIngestController(t, ctrl)
			expectHeadAndEnablement(m)
			m.uriStore.EXPECT().GetIDByURI(gomock.Any(), testURI).Return("7", nil)
			current := acceptedRequest("7")
			m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(current, nil)
			updated := current
			updated.BuildStrategy = entity.BuildStrategyFull
			failure := errors.New("storage unavailable")
			if tc.conflict {
				failure = storage.ErrVersionMismatch
			}
			write := m.reqStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(failure)
			if tc.conflict {
				if tc.reloadError {
					m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(entity.Request{}, errors.New("storage unavailable")).After(write)
				} else {
					current.State = entity.RequestStateProcessing
					current.Version = 2
					m.reqStore.EXPECT().Get(gomock.Any(), "7").Return(current, nil).After(write)
				}
			}

			_, err := c.Ingest(context.Background(), entity.IngestRequest{Queue: testQueue, ForceFull: true})
			require.Error(t, err)
			if tc.conflict && !tc.reloadError {
				require.ErrorIs(t, err, ErrInvalidRequest)
			}
		})
	}
}
