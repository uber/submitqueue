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
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
	"github.com/uber/submitqueue/platform/errs"
	mysqlerrs "github.com/uber/submitqueue/platform/errs/mysql"
	mqmock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/platform/publish"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	requestlogmock "github.com/uber/submitqueue/stovepipe/core/requestlog/mock"
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
	testID      = "7"
	testHeadURI = "git://repo/monorepo/main/head"
	testBaseURI = "git://repo/monorepo/main/base"
	testBuildID = "bk-1"
	testNowMs   = int64(1700000000000)
)

func queueContext() context.Context {
	ctx := entityqueue.WithQueueName(context.Background(), testQueue)
	return metrics.WithContextTags(ctx, metrics.NewTag("queue", testQueue))
}

// buildMocks bundles the mocks a build controller test case wires expectations on.
type buildMocks struct {
	reqStore      *storagemock.MockRequestStore
	buildStore    *storagemock.MockBuildStore
	store         *storagemock.MockStorage
	materializer  *requestlogmock.MockMaterializer
	runnerFactory *buildrunnermock.MockFactory
	runner        *buildrunnermock.MockBuildRunner
	publisher     *mqmock.MockPublisher
	metricsScope  tally.TestScope
}

// staticStorageFactory resolves every queue to one fixed store aggregate.
type staticStorageFactory struct{ store storage.Storage }

// For returns the fixed store aggregate for any queue.
func (f staticStorageFactory) For(storage.Config) (storage.Storage, error) { return f.store, nil }

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, buildMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := buildMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		buildStore:    storagemock.NewMockBuildStore(ctrl),
		store:         storagemock.NewMockStorage(ctrl),
		materializer:  requestlogmock.NewMockMaterializer(ctrl),
		runnerFactory: buildrunnermock.NewMockFactory(ctrl),
		runner:        buildrunnermock.NewMockBuildRunner(ctrl),
		publisher:     mqmock.NewMockPublisher(ctrl),
		metricsScope:  scope,
	}

	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyBuildSignal, Name: "buildsignal", Queue: queue},
		{Key: stovepipemq.TopicKeyBuildAnalysis, Name: "buildanalysis", Queue: queue},
		{Key: stovepipemq.TopicKeyIssueTrack, Name: "issuetrack", Queue: queue},
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), scope, staticStorageFactory{store: m.store}, m.materializer, m.runnerFactory, registry, stovepipemq.TopicKeyBuild, "stovepipe-build")
	c.now = func() time.Time { return time.UnixMilli(testNowMs) }
	return c, m
}

func expectBuildTriggered(m buildMocks) *gomock.Call {
	request := entity.Request{ID: testID, Queue: testQueue}
	return m.materializer.EXPECT().PersistLog(
		gomock.Any(),
		m.store,
		requestlog.NewRequestEventLog(
			request,
			entity.RequestEventBuildTriggered,
			testBuildID,
			map[string]string{requestlog.MetadataKeyBuildID: testBuildID},
		),
	).Return(nil)
}

func TestProcessTagsMetricsWithQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	m.reqStore.EXPECT().Get(gomock.Any(), testID).
		Return(processingRequest(entity.BuildStrategyUnknown, ""), nil)
	m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)

	require.Error(t, c.Process(queueContext(), delivery(t, ctrl, buildPayload(t, testID))))
	counter, ok := m.metricsScope.Snapshot().Counters()["test.build_controller.build.strategy_not_visible+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestProcessTagsDeserializeErrorsFromContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	require.Error(t, c.Process(queueContext(), delivery(t, ctrl, []byte("not-protojson"))))
	counter, ok := m.metricsScope.Snapshot().Counters()["test.build_controller.build.deserialize_errors+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestProcessRejectsTenantPayloadQueueMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)
	msg := entityqueue.NewMessage(testID, buildPayload(t, testID), testQueue, nil)
	msg.Tenant = "other-queue"
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()

	err := c.Process(queueContext(), d)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func TestPublishBuildSignalCarriesQueueMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	var got entityqueue.Message
	m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			got = msg
			return nil
		})

	require.NoError(t, c.publishBuildSignal(queueContext(), testBuildID, testQueue))
	assert.Equal(t, testBuildID, got.PartitionKey)
	assert.Equal(t, testQueue, got.Tenant)
	assert.Equal(t, testQueue, got.Metadata[entityqueue.MetadataKeyQueueName])
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	msg := entityqueue.NewMessage(testID, payload, testID, nil)
	msg.Tenant = testQueue
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func buildPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.BuildRequest{Id: id, QueueName: testQueue})
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
					CreatedAt: testNowMs,
					Version:   1,
				}
				createCall := m.buildStore.EXPECT().Create(gomock.Any(), build).Return(nil)
				logCall := expectBuildTriggered(m).After(createCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
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
					CreatedAt: testNowMs,
					Version:   1,
				}
				createCall := m.buildStore.EXPECT().Create(gomock.Any(), build).Return(nil)
				logCall := expectBuildTriggered(m).After(createCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
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
			name: "succeeded request is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateSucceeded
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
			},
		},
		{
			name: "failed request is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateFailed
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
			},
		},
		{
			name: "cancelled request is a no-op",
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				req.State = entity.RequestStateCancelled
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
			name:      "request not found is not retryable",
			wantErr:   true,
			wantRetry: false,
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
			name:    "duplicate runner build id is rejected",
			wantErr: true,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(storage.ErrAlreadyExists)
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
			name:    "event persistence failure stops before buildsignal",
			wantErr: true,
			setup: func(m buildMocks) {
				req := processingRequest(entity.BuildStrategyFull, "")
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(req, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)
				createCall := m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				request := entity.Request{ID: testID, Queue: testQueue}
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestEventLog(
						request,
						entity.RequestEventBuildTriggered,
						testBuildID,
						map[string]string{requestlog.MetadataKeyBuildID: testBuildID},
					),
				).Return(errors.New("db down")).After(createCall)
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
				createCall := m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
				logCall := expectBuildTriggered(m).After(createCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(errors.New("queue down")).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
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

			err := c.Process(queueContext(), delivery(t, ctrl, payload))

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestProcessPublishesBothPollsAfterPersistence(t *testing.T) {
	signalFailure := errors.New("status publish unavailable")
	analysisFailure := errors.New("analysis publish unavailable")

	for _, tt := range []struct {
		name        string
		signalErr   error
		analysisErr error
		wantRetry   bool
	}{
		{name: "both succeed"},
		{name: "status publish fails", signalErr: signalFailure},
		{name: "analysis publish fails", analysisErr: analysisFailure},
		{name: "both fail", signalErr: signalFailure, analysisErr: analysisFailure},
		{name: "transient status publish", signalErr: driver.ErrBadConn, wantRetry: true},
		{name: "transient analysis publish", analysisErr: driver.ErrBadConn, wantRetry: true},
		{name: "permanent status and transient analysis", signalErr: sql.ErrTxDone, analysisErr: driver.ErrBadConn, wantRetry: true},
		{name: "transient status and permanent analysis", signalErr: driver.ErrBadConn, analysisErr: sql.ErrTxDone, wantRetry: true},
		{name: "both permanent", signalErr: sql.ErrTxDone, analysisErr: sql.ErrTxDone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(processingRequest(entity.BuildStrategyFull, ""), nil)

			m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
			m.runner.EXPECT().Trigger(gomock.Any(), "", testHeadURI, entity.BuildMetadata(nil)).Return(entity.BuildID{ID: testBuildID}, nil)

			persisted := m.buildStore.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
			logged := expectBuildTriggered(m).After(persisted)

			m.publisher.EXPECT().Publish(gomock.Any(), "buildsignal", gomock.Any()).Return(tt.signalErr).After(logged)

			m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
				var payload stovepipemq.BuildAnalysis
				require.NoError(t, stovepipemq.Unmarshal(msg.Payload, &payload))
				assert.Equal(t, testBuildID, payload.Id)
				assert.Equal(t, testQueue, payload.QueueName)
				assert.Equal(t, testBuildID, msg.PartitionKey)
				assert.Equal(t, testQueue, msg.Tenant)
				assert.Equal(t, testQueue, msg.Metadata[entityqueue.MetadataKeyQueueName])
				assert.Equal(t, publish.IntentID(testBuildID, "started"), msg.ID)
				return tt.analysisErr
			}).After(logged)

			err := c.Process(queueContext(), delivery(t, ctrl, buildPayload(t, testID)))
			if tt.signalErr == nil && tt.analysisErr == nil {
				require.NoError(t, err)
			}

			if tt.signalErr != nil {
				require.ErrorIs(t, err, tt.signalErr)
			}

			if tt.analysisErr != nil {
				require.ErrorIs(t, err, tt.analysisErr)
			}

			if err != nil {
				processed := errs.NewClassifierProcessor(mysqlerrs.Classifier).Process(err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(processed))
			}
		})
	}
}

func TestAnalysisPublishDeduplicatesRetriesOfTheSameOccasion(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	var ids []string

	m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
		ids = append(ids, msg.ID)
		return nil
	}).Times(2)

	require.NoError(t, c.publishBuildAnalysis(queueContext(), testBuildID, testQueue))
	require.NoError(t, c.publishBuildAnalysis(queueContext(), testBuildID, testQueue))

	assert.Equal(t, ids[0], ids[1])
	assert.NotEqual(t, publish.IntentID(testBuildID, "finished"), ids[0])
}
