package lastgreen

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/entity"
	sourcecontrol "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
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

	New(scope, stores, sourceControls).Report(context.Background(), testQueue)

	gauge, ok := scope.Snapshot().Gauges()["stovepipe.last_green.age_seconds+queue=monorepo/main"]
	require.True(t, ok)
	assert.InDelta(t, time.Since(createdAt).Seconds(), gauge.Value(), 1)
}

func TestReport_RecordsMissingLastGreen(t *testing.T) {
	ctrl := gomock.NewController(t)
	scope := tally.NewTestScope("stovepipe", nil)
	stores := storagemock.NewMockFactory(ctrl)
	store := storagemock.NewMockStorage(ctrl)
	queueStore := storagemock.NewMockQueueStore(ctrl)

	stores.EXPECT().For(storage.Config{QueueName: testQueue}).Return(store, nil)
	store.EXPECT().GetQueueStore().Return(queueStore)
	queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{Name: testQueue}, nil)

	New(scope, stores, sourcecontrolmock.NewMockFactory(ctrl)).Report(context.Background(), testQueue)

	counter, ok := scope.Snapshot().Counters()["stovepipe.last_green.age_missing+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}
