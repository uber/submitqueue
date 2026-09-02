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
	"github.com/uber/submitqueue/platform/metrics"
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

func newTestMaterializer(t *testing.T) (*materializer, *storagemock.MockStorage, *storagemock.MockRequestLogStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	stores := storagemock.NewMockStorage(ctrl)
	store := storagemock.NewMockRequestLogStore(ctrl)
	stores.EXPECT().GetRequestLogStore().Return(store).AnyTimes()
	return &materializer{
		scope: tally.NoopScope,
		now:   func() time.Time { return time.UnixMilli(testNowMs) },
	}, stores, store
}

func TestMaterializerPersistRequestStateLog(t *testing.T) {
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
			materializer, stores, store := newTestMaterializer(t)
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

			log := NewRequestStateLog(request, tt.outcomeReason)
			err := materializer.PersistLog(context.Background(), stores, log)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMaterializerExistingIdenticalOccurrenceIsSuccess(t *testing.T) {
	materializer, stores, store := newTestMaterializer(t)
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

	require.NoError(t, materializer.PersistLog(context.Background(), stores, NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)))
}

func TestMaterializerPreservesSuppliedTimestamp(t *testing.T) {
	materializer, stores, store := newTestMaterializer(t)
	log := NewRequestStateLog(
		entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1},
		entity.RequestOutcomeReasonUnknown,
	)
	log.TimestampMs = testNowMs - 1000
	store.EXPECT().Create(gomock.Any(), log).Return(nil)

	require.NoError(t, materializer.PersistLog(context.Background(), stores, log))
}

func TestMaterializerExistingConflictingOccurrenceFails(t *testing.T) {
	materializer, stores, store := newTestMaterializer(t)
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

	require.Error(t, materializer.PersistLog(context.Background(), stores, NewRequestStateLog(request, entity.RequestOutcomeReasonBuildSucceeded)))
}

func TestMaterializerStorageFailures(t *testing.T) {
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
			materializer, stores, store := newTestMaterializer(t)
			tt.setup(store)
			log := NewRequestStateLog(entity.Request{
				ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1,
			}, entity.RequestOutcomeReasonUnknown)
			err := materializer.PersistLog(context.Background(), stores, log)
			require.Error(t, err)
		})
	}
}

func TestNewRequestStateLogStableID(t *testing.T) {
	request := entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1}
	first := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
	retry := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)
	request.Version++
	next := NewRequestStateLog(request, entity.RequestOutcomeReasonUnknown)

	assert.Equal(t, "state/1", first.ID)
	assert.Equal(t, first.ID, retry.ID)
	assert.NotEqual(t, first.ID, next.ID)
}

func TestNewRequestEventLogStableID(t *testing.T) {
	request := entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateProcessing, Version: 2}
	metadata := map[string]string{MetadataKeyBuildID: "bk-1"}
	first := NewRequestEventLog(request, entity.RequestEventBuildTriggered, "bk-1", metadata)
	retry := NewRequestEventLog(request, entity.RequestEventBuildTriggered, "bk-1", metadata)
	next := NewRequestEventLog(request, entity.RequestEventBuildFinished, "bk-1", metadata)

	assert.Equal(t, "event/build_triggered/bk-1", first.ID)
	assert.Equal(t, first.ID, retry.ID)
	assert.NotEqual(t, first.ID, next.ID)
	assert.Equal(t, testQueue, first.Queue)
	assert.Equal(t, testRequestID, first.RequestID)
	assert.Equal(t, entity.RequestEventBuildTriggered, first.Event)
	assert.Empty(t, first.State)
	assert.Zero(t, first.RequestVersion)
	assert.Empty(t, first.OutcomeReason)
	assert.Equal(t, metadata, first.Metadata)
}

func TestSameSemanticOccurrenceMetadata(t *testing.T) {
	base := entity.RequestLog{ID: "log/1", Queue: testQueue, RequestID: testRequestID, State: entity.RequestStateAccepted, RequestVersion: 1}
	stored := base
	stored.Metadata = map[string]string{}
	assert.True(t, sameSemanticOccurrence(stored, base))

	stored.Metadata["source"] = "hook"
	assert.True(t, sameSemanticOccurrence(stored, base))

	candidate := base
	candidate.Metadata = map[string]string{"source": "ingest"}
	assert.False(t, sameSemanticOccurrence(stored, candidate))

	candidate.Metadata["source"] = "hook"
	candidate.Metadata["new_key"] = "new_value"
	stored.Metadata["new_key"] = "new_value"
	assert.True(t, sameSemanticOccurrence(stored, candidate))
}

func TestMaterializerMetricsIncludeContextTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("test", nil)
	materializer := &materializer{
		scope: scope,
		now:   func() time.Time { return time.UnixMilli(testNowMs) },
	}
	stores := storagemock.NewMockStorage(ctrl)
	store := storagemock.NewMockRequestLogStore(ctrl)
	stores.EXPECT().GetRequestLogStore().Return(store)
	store.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	ctx := metrics.WithContextTags(context.Background(), metrics.NewTag("queue", testQueue))

	log := NewRequestStateLog(entity.Request{
		ID: testRequestID, Queue: testQueue, State: entity.RequestStateAccepted, Version: 1,
	}, entity.RequestOutcomeReasonUnknown)
	require.NoError(t, materializer.PersistLog(ctx, stores, log))

	counter, ok := scope.Snapshot().Counters()["test.persist.created+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}
