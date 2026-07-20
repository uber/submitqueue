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

package build

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/buildrunner"
	buildrunnermock "github.com/uber/submitqueue/stovepipe/extension/buildrunner/mock"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	storagemock "github.com/uber/submitqueue/stovepipe/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

const (
	testQueue   = "monorepo/main"
	testID      = "request/monorepo/main/7"
	testHeadURI = "git://repo/monorepo/main/head"
	testBaseURI = "git://repo/monorepo/main/base"
	testBuildID = "bk-1"
)

// buildMocks bundles the mocks a build controller test case wires expectations on.
type buildMocks struct {
	reqStore      *storagemock.MockRequestStore
	buildStore    *storagemock.MockBuildStore
	runnerFactory *buildrunnermock.MockFactory
	runner        *buildrunnermock.MockBuildRunner
	publisher     *mqmock.MockPublisher
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, buildMocks) {
	t.Helper()

	m := buildMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		buildStore:    storagemock.NewMockBuildStore(ctrl),
		runnerFactory: buildrunnermock.NewMockFactory(ctrl),
		runner:        buildrunnermock.NewMockBuildRunner(ctrl),
		publisher:     mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyBuildSignal, Name: "buildsignal", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, m.runnerFactory, registry, stovepipemq.TopicKeyBuild, "stovepipe-build")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := mqmock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testID, payload, testID, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func buildPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id})
	require.NoError(t, err)
	return b
}

// processingRequest returns a Request past process's admit, with the given
// already-decided scope.
func processingRequest(strategy entity.BuildStrategy, baseURI string) entity.Request {
	return entity.Request{
		ID:            testID,
		Queue:         testQueue,
		URI:           testHeadURI,
		BuildStrategy: strategy,
		BaseURI:       baseURI,
		State:         entity.RequestStateProcessing,
		Version:       1,
	}
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		setup     func(m buildMocks)
		wantErr   bool
		wantRetry bool
	}{
		{
			name: "incremental build triggers and publishes",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyIncrementalSinceGreen, testBaseURI)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), testBaseURI, testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				build := entity.Build{
					ID:        testBuildID,
					RequestID: testID,
					Status:    entity.BuildStatusAccepted,
					Version:   1,
				}
				m.buildStore.EXPECT().Create(gomock.Any(), build).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(nil)
			},
		},
		{
			name: "full build ignores stale base uri",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, testBaseURI)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				build := entity.Build{
					ID:        testBuildID,
					RequestID: testID,
					Status:    entity.BuildStatusAccepted,
					Version:   1,
				}
				m.buildStore.EXPECT().Create(gomock.Any(), build).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(nil)
			},
		},
		{
			name: "superseded is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateSuperseded
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
			},
		},
		{
			name: "recorded green is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateRecordedGreen
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
			},
		},
		{
			name: "recorded not green is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateRecordedNotGreen
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
			},
		},
		{
			name:      "build strategy not yet visible is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyUnknown, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
			},
		},
		{
			name:      "request not found is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m buildMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name:      "request storage error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildMocks) {
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errors.New("db down"))
			},
		},
		{
			name:      "factory lookup failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(nil, errors.New("no runner"))
			},
		},
		{
			name:      "trigger failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{}, errors.New("runner down"))
			},
		},
		{
			name: "already exists on create is swallowed and publish still happens",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(nil)
			},
		},
		{
			name:      "build store error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(errors.New("db down"))
			},
		},
		{
			name:      "publish failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(errors.New("queue down"))
			},
		},
		{
			name:      "malformed payload is not retryable",
			payload:   []byte("not-json"),
			wantErr:   true,
			wantRetry: false,
			setup:     func(m buildMocks) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			if tt.setup != nil {
				tt.setup(m)
			}

			payload := tt.payload
			if payload == nil {
				payload = buildPayload(t, testID)
			}

			err := c.Process(context.Background(), delivery(t, ctrl, payload))

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}
