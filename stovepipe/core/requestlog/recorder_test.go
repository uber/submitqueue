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

package requestlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

const (
	testQueue     = "monorepo/main"
	testRequestID = "request/monorepo/main/1"
	testNowMs     = int64(1735689600000)
)

func newTestRecorder(t *testing.T) (*recorder, *storagemock.MockRequestLogStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	return &recorder{
		scope: tally.NoopScope,
		now:   func() time.Time { return time.UnixMilli(testNowMs) },
	}, storagemock.NewMockRequestLogStore(ctrl)
}

func TestRecorderRecordRequestState(t *testing.T) {
	tests := []struct {
		name          string
		state         entity.RequestState
		outcomeReason entity.RequestOutcomeReason
		wantErr       bool
	}{
		{name: "accepted", state: entity.RequestStateAccepted},
		{name: "processing", state: entity.RequestStateProcessing},
		{
			name:          "superseded",
			state:         entity.RequestStateSuperseded,
			outcomeReason: entity.RequestOutcomeReasonSupersededByNewerHead,
		},
		{
			name:          "succeeded",
			state:         entity.RequestStateSucceeded,
			outcomeReason: entity.RequestOutcomeReasonBuildSucceeded,
		},
		{name: "terminal reason required", state: entity.RequestStateSucceeded, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder, store := newTestRecorder(t)
			request := entity.Request{
				ID:      testRequestID,
				Queue:   testQueue,
				State:   tt.state,
				Version: 2,
			}
			if !tt.wantErr {
				store.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, entry entity.RequestLog) error {
					assert.NotEmpty(t, entry.ID)
					assert.Equal(t, testNowMs, entry.TimestampMs)
					assert.Equal(t, testQueue, entry.Queue)
					assert.Equal(t, testRequestID, entry.RequestID)
					assert.Equal(t, tt.state, entry.State)
					assert.Equal(t, int32(2), entry.RequestVersion)
					assert.Equal(t, tt.outcomeReason, entry.OutcomeReason)
					require.NoError(t, entry.Validate())
					return nil
				})
			}

			err := recorder.RecordRequestState(context.Background(), store, request, tt.outcomeReason)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRecorderExistingIdenticalOccurrenceIsSuccess(t *testing.T) {
	recorder, store := newTestRecorder(t)
	request := entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1}

	var candidate entity.RequestLog
	store.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, entry entity.RequestLog) error {
		candidate = entry
		return storage.ErrAlreadyExists
	})
	store.EXPECT().Get(gomock.Any(), testRequestID, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, entryID string) (entity.RequestLog, error) {
			stored := candidate
			stored.ID = entryID
			stored.TimestampMs = candidate.TimestampMs - 1000
			stored.Metadata = map[string]string{}
			return stored, nil
		},
	)

	require.NoError(t, recorder.RecordRequestState(context.Background(), store, request, entity.RequestOutcomeReasonUnknown))
}

func TestRecorderExistingConflictingOccurrenceFails(t *testing.T) {
	recorder, store := newTestRecorder(t)
	request := entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateSucceeded, Version: 3}

	var candidate entity.RequestLog
	store.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, entry entity.RequestLog) error {
		candidate = entry
		return storage.ErrAlreadyExists
	})
	store.EXPECT().Get(gomock.Any(), testRequestID, gomock.Any()).DoAndReturn(
		func(context.Context, string, string) (entity.RequestLog, error) {
			stored := candidate
			stored.OutcomeReason = entity.RequestOutcomeReasonBuildFailed
			return stored, nil
		},
	)

	require.Error(t, recorder.RecordRequestState(context.Background(), store, request, entity.RequestOutcomeReasonBuildSucceeded))
}

func TestRecorderStorageFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*storagemock.MockRequestLogStore)
	}{
		{
			name: "create",
			setup: func(store *storagemock.MockRequestLogStore) {
				store.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("create"))
			},
		},
		{
			name: "reload duplicate",
			setup: func(store *storagemock.MockRequestLogStore) {
				store.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
				store.EXPECT().Get(gomock.Any(), testRequestID, gomock.Any()).Return(entity.RequestLog{}, errors.New("get"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder, store := newTestRecorder(t)
			tt.setup(store)
			err := recorder.RecordRequestState(context.Background(), store, entity.Request{
				ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1,
			}, entity.RequestOutcomeReasonUnknown)
			require.Error(t, err)
		})
	}
}

func TestOccurrenceID(t *testing.T) {
	assert.Equal(t, occurrenceID("queue", "request", "state", "1"), occurrenceID("queue", "request", "state", "1"))
	assert.NotEqual(t, occurrenceID("queue", "request", "state", "1"), occurrenceID("queue", "request", "state", "2"))
	assert.NotEqual(t, occurrenceID("queue", "request", "ab", "c"), occurrenceID("queue", "request", "a", "bc"))
}

func TestSameOccurrenceMetadata(t *testing.T) {
	base := entity.RequestLog{ID: "log/1", Queue: testQueue, RequestID: testRequestID, State: entity.RequestStateAccepted, RequestVersion: 1}
	stored := base
	stored.Metadata = map[string]string{}
	assert.True(t, sameOccurrence(stored, base))

	stored.Metadata["source"] = "hook"
	assert.False(t, sameOccurrence(stored, base))
}
