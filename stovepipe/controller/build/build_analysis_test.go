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

package build

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/stovepipe/controller/buildanalysis"
	"github.com/uber/submitqueue/stovepipe/controller/buildsignal"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildissue"
	buildissuemock "github.com/uber/submitqueue/stovepipe/extension/buildissue/mock"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestBuildAnalysisPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	buildController, m := newController(t, ctrl)
	ctx := queueContext()

	var savedBuild entity.Build
	request := processingRequest(entity.BuildStrategyFull, "")
	queue := entity.Queue{Name: testQueue, InFlightCount: 1, Version: 1}

	queueStore := storagemock.NewMockQueueStore(ctrl)
	issueStore := storagemock.NewMockBuildIssueStore(ctrl)
	m.store.EXPECT().GetQueueStore().Return(queueStore).AnyTimes()
	m.store.EXPECT().GetBuildIssueStore().Return(issueStore).AnyTimes()

	m.reqStore.EXPECT().Get(gomock.Any(), testID).DoAndReturn(func(context.Context, string) (entity.Request, error) {
		return request, nil
	}).AnyTimes()
	m.reqStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.Request, old, next int32) error {
		require.Equal(t, request.Version, old)
		request = value
		request.Version = next
		return nil
	})

	queueStore.EXPECT().Get(gomock.Any(), testQueue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
		return queue, nil
	}).AnyTimes()
	queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.Queue, old, next int32) error {
		require.Equal(t, queue.Version, old)
		queue = value
		queue.Version = next
		return nil
	})

	m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.Build) error {
		savedBuild = value
		return nil
	})
	m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).DoAndReturn(func(context.Context, string) (entity.Build, error) {
		return savedBuild, nil
	}).AnyTimes()
	m.buildStore.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, value entity.Build, old, next int32) error {
		require.Equal(t, savedBuild.Version, old)
		savedBuild = value
		savedBuild.Version = next
		return nil
	})

	m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil).AnyTimes()

	m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil).Times(2)
	m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
	m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusFailed, nil, nil)

	published := map[string][]entityqueue.Message{}
	m.publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, topic string, msg entityqueue.Message) error {
		require.Equal(t, testBuildID, savedBuild.ID)
		published[topic] = append(published[topic], msg)
		return nil
	}).AnyTimes()

	a := entity.BuildIssue{TaskID: "validation", UnitID: "unit-a"}
	b := entity.BuildIssue{TaskID: "validation", UnitID: "unit-b"}
	persisted := map[entity.BuildIssue]bool{}
	issueStore.EXPECT().Create(gomock.Any(), testBuildID, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, issue entity.BuildIssue) error {
		if persisted[issue] {
			return storage.ErrAlreadyExists
		}
		persisted[issue] = true
		return nil
	}).AnyTimes()

	detectorFactory := buildissuemock.NewMockDetectorFactory(ctrl)
	detector := buildissuemock.NewMockDetector(ctrl)
	detectorFactory.EXPECT().For(buildissue.DetectorConfig{QueueName: testQueue}).Return(detector, nil).Times(3)
	gomock.InOrder(
		detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{a}, nil),
		detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{a, b}, nil),
		detector.EXPECT().Detect(gomock.Any(), entity.BuildID{ID: testBuildID}).Return([]entity.BuildIssue{a, b}, nil),
	)

	analysisController := buildanalysis.NewController(zap.NewNop().Sugar(), tally.NoopScope,
		staticStorageFactory{store: m.store}, detectorFactory, stovepipemq.TopicKeyBuildAnalysis, "analysis")
	statusController := buildsignal.NewController(zap.NewNop().Sugar(), tally.NoopScope,
		staticStorageFactory{store: m.store}, m.materializer, m.runnerFactory, buildController.registry,
		stovepipemq.TopicKeyBuildSignal, "signal")

	delivered := func(msg entityqueue.Message) *consumermock.MockDelivery {
		d := consumermock.NewMockDelivery(ctrl)
		d.EXPECT().Message().Return(msg)
		return d
	}

	require.NoError(t, buildController.Process(ctx, delivery(t, ctrl, buildPayload(t, testID))))
	require.Len(t, published["buildanalysis"], 1)
	require.Len(t, published["buildsignal"], 1)

	firstPoll := delivered(published["buildanalysis"][0])
	firstPoll.EXPECT().Hold(buildanalysis.PollDelayMs)
	require.NoError(t, analysisController.Process(ctx, firstPoll))
	assert.True(t, persisted[a])
	assert.False(t, persisted[b])

	require.NoError(t, statusController.Process(ctx, delivered(published["buildsignal"][0])))
	require.Len(t, published["record"], 1)
	require.Len(t, published["buildanalysis"], 2)
	assert.Equal(t, entity.BuildStatusFailed, savedBuild.Status)
	assert.Equal(t, entity.RequestStateFailed, request.State)
	assert.Zero(t, queue.InFlightCount)
	assert.False(t, persisted[b])
	assert.NotEqual(t, published["buildanalysis"][0].ID, published["buildanalysis"][1].ID)

	require.NoError(t, analysisController.Process(ctx, delivered(published["buildanalysis"][0])))
	require.NoError(t, analysisController.Process(ctx, delivered(published["buildanalysis"][1])))
	assert.Len(t, persisted, 2)
	assert.True(t, persisted[b])
}
