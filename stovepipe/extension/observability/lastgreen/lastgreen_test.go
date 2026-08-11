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

package lastgreen

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/observability"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	sourcecontrolmock "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
)

const testQueue = "monorepo/main"

func TestReport_EmitsLastGreenAge(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("stovepipe", nil)
	stores := storagemock.NewMockFactory(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	queueStore := storagemock.NewMockQueueStore(ctrl)
	sourceControls := sourcecontrolmock.NewMockFactory(ctrl)
	source := sourcecontrolmock.NewMockSourceControl(ctrl)
	createdAt := time.Now().Add(-time.Hour)

	stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(store, nil)
	store.EXPECT().GetQueueStore().Return(queueStore)
	queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
		Name:         testQueue,
		LastGreenURI: "git://github.com/uber-code/repo/refs%2Fheads%2Fmain/abc",
	}, nil)
	sourceControls.EXPECT().For(sourcecontrol.Config{QueueName: testQueue}).Return(source, nil)
	source.EXPECT().ChangeInfo(gomock.Any(), gomock.Any()).Return(sourcecontrol.ChangeInfo{
		CreatedAt: createdAt,
	}, nil)

	reporter, err := NewFactory(scope, stores, sourceControls).For(observability.Config{QueueName: testQueue})
	require.NoError(t, err)
	reporter.Report(context.Background())

	gauge, ok := scope.Snapshot().Gauges()["stovepipe.last_green.age_seconds+queue=monorepo/main"]
	require.True(t, ok)
	assert.InDelta(t, time.Since(createdAt).Seconds(), gauge.Value(), 1)
}

func TestReport_RecordsMissingLastGreen(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("stovepipe", nil)
	store := storagemock.NewMockStorage(ctrl)
	queueStore := storagemock.NewMockQueueStore(ctrl)

	store.EXPECT().GetQueueStore().Return(queueStore)
	queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue}, nil)

	New(scope, testQueue, store, sourcecontrolmock.NewMockSourceControl(ctrl)).Report(context.Background())

	counter, ok := scope.Snapshot().Counters()["stovepipe.last_green.age_missing+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestReport_RecordsQueueReadFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("stovepipe", nil)
	store := storagemock.NewMockStorage(ctrl)
	queueStore := storagemock.NewMockQueueStore(ctrl)

	store.EXPECT().GetQueueStore().Return(queueStore)
	queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, errors.New("boom"))

	New(scope, testQueue, store, sourcecontrolmock.NewMockSourceControl(ctrl)).Report(context.Background())

	counter, ok := scope.Snapshot().Counters()["stovepipe.last_green.age_errors+queue=monorepo/main,stage=get_queue"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestFor_PropagatesResolutionFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("stovepipe", nil)
	stores := storagemock.NewMockFactory(ctrl)
	sourceControls := sourcecontrolmock.NewMockFactory(ctrl)

	stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(nil, errors.New("boom"))

	_, err := NewFactory(scope, stores, sourceControls).For(observability.Config{QueueName: testQueue})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve storage")
}

func TestReport_RecordsUnobservableAge(t *testing.T) {
	tests := []struct {
		name  string
		stage string
		info  sourcecontrol.ChangeInfo
		err   error
	}{
		{
			name:  "change info fails",
			stage: "get_change_info",
			err:   errors.New("boom"),
		},
		{
			name:  "change is undated",
			stage: "get_change_info",
			info:  sourcecontrol.ChangeInfo{},
		},
		{
			name:  "change is dated in the future",
			stage: "future_change",
			info:  sourcecontrol.ChangeInfo{CreatedAt: time.Now().Add(time.Hour)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			scope := tally.NewTestScope("stovepipe", nil)
			store := storagemock.NewMockStorage(ctrl)
			queueStore := storagemock.NewMockQueueStore(ctrl)
			source := sourcecontrolmock.NewMockSourceControl(ctrl)

			store.EXPECT().GetQueueStore().Return(queueStore)
			queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{
				Name:         testQueue,
				LastGreenURI: "git://github.com/uber-code/repo/refs%2Fheads%2Fmain/abc",
			}, nil)
			source.EXPECT().ChangeInfo(gomock.Any(), gomock.Any()).Return(test.info, test.err)

			New(scope, testQueue, store, source).Report(context.Background())

			snapshot := scope.Snapshot()
			assert.Empty(t, snapshot.Gauges(), "no age may be reported when it cannot be observed")
			counter, ok := snapshot.Counters()["stovepipe.last_green.age_errors+queue=monorepo/main,stage="+test.stage]
			require.True(t, ok)
			assert.EqualValues(t, 1, counter.Value())
		})
	}
}
