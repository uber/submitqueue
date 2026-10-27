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

package buildsignal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/uber/submitqueue/platform/publish"

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
	testOtherID = "8"
	testBuildID = "bk-1"
)

func queueContext() context.Context {
	ctx := entityqueue.WithQueueName(context.Background(), testQueue)
	return metrics.WithContextTags(ctx, metrics.NewTag("queue", testQueue))
}

// buildsignalMocks bundles the mocks a buildsignal controller test case wires
// expectations on.
type buildsignalMocks struct {
	reqStore      *storagemock.MockRequestStore
	buildStore    *storagemock.MockBuildStore
	queueStore    *storagemock.MockQueueStore
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

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, buildsignalMocks) {
	t.Helper()

	scope := tally.NewTestScope("test", nil)
	m := buildsignalMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		buildStore:    storagemock.NewMockBuildStore(ctrl),
		queueStore:    storagemock.NewMockQueueStore(ctrl),
		store:         storagemock.NewMockStorage(ctrl),
		materializer:  requestlogmock.NewMockMaterializer(ctrl),
		runnerFactory: buildrunnermock.NewMockFactory(ctrl),
		runner:        buildrunnermock.NewMockBuildRunner(ctrl),
		publisher:     mqmock.NewMockPublisher(ctrl),
		metricsScope:  scope,
	}

	m.store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	m.store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()
	m.store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyBuildSignal, Name: "buildsignal", Queue: queue},
		{Key: stovepipemq.TopicKeyBuildAnalysis, Name: "buildanalysis", Queue: queue},
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), scope, staticStorageFactory{store: m.store}, m.materializer, m.runnerFactory, registry, stovepipemq.TopicKeyBuildSignal, "stovepipe-buildsignal")
	return c, m
}

func TestProcessTagsMetricsWithQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, assert.AnError)

	require.Error(t, c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID))))
	counter, ok := m.metricsScope.Snapshot().Counters()["test.buildsignal_controller.buildsignal.storage_errors+queue=monorepo/main"]
	require.True(t, ok)
	assert.EqualValues(t, 1, counter.Value())
}

func TestProcessRejectsTenantPayloadQueueMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, _ := newController(t, ctrl)
	msg := entityqueue.NewMessage(testBuildID, buildSignalPayload(t, testID), testQueue, nil)
	msg.Tenant = "other-queue"
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()

	err := c.Process(queueContext(), d)
	require.Error(t, err)
	assert.False(t, errs.IsRetryable(err))
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	msg := entityqueue.NewMessage(testBuildID, payload, testBuildID, nil)
	msg.Tenant = testQueue
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func buildSignalPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: id, QueueName: testQueue})
	require.NoError(t, err)
	return b
}

// requestWithState returns a Request past process's admit, in the given state.
func requestWithState(state entity.RequestState) entity.Request {
	request := entity.Request{
		ID:      testID,
		Queue:   testQueue,
		State:   state,
		Version: 1,
	}
	if state.HasBuildOutcome() {
		request.TerminalBuildID = testBuildID
	}
	return request
}

// build returns a Build with the given status/version, tied to testID.
func build(status entity.BuildStatus, version int32) entity.Build {
	return entity.Build{
		ID:        testBuildID,
		RequestID: testID,
		Status:    status,
		Version:   version,
	}
}

// queueRow returns the testQueue's row whose build slots are held by inFlightRequestIDs.
func queueRow(inFlightRequestIDs []string, version int32) entity.Queue {
	return entity.Queue{
		Name:               testQueue,
		InFlightRequestIDs: inFlightRequestIDs,
		Version:            version,
	}
}

func expectFinishWrites(m buildsignalMocks, state entity.RequestState) *gomock.Call {
	eventCall := expectBuildFinished(m)
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow([]string{testID}, 4), nil).After(eventCall)
	m.queueStore.EXPECT().Update(gomock.Any(), queueRow([]string{}, 4), int32(4), int32(5)).Return(nil)
	outcomeCall := m.reqStore.EXPECT().Update(gomock.Any(), requestWithState(state), int32(1), int32(2)).Return(nil)
	if state == entity.RequestStateSucceeded {
		return expectAdvanceLastGreen(m, outcomeCall)
	}
	return outcomeCall
}

func expectAdvanceLastGreen(m buildsignalMocks, after *gomock.Call) *gomock.Call {
	getCall := m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow([]string{}, 5), nil)
	if after != nil {
		getCall.After(after)
	}
	updated := queueRow([]string{}, 5)
	updated.LastGreenURI = requestWithState(entity.RequestStateSucceeded).URI
	updated.LastGreenRequestID = testID
	return m.queueStore.EXPECT().Update(gomock.Any(), updated, int32(5), int32(6)).Return(nil).After(getCall)
}

func expectBuildFinished(m buildsignalMocks) *gomock.Call {
	request := entity.Request{ID: testID, Queue: testQueue}
	return m.materializer.EXPECT().PersistLog(
		gomock.Any(),
		m.store,
		requestlog.NewRequestEventLog(
			request,
			entity.RequestEventBuildFinished,
			testBuildID,
			map[string]string{requestlog.MetadataKeyBuildID: testBuildID},
		),
	).Return(nil)
}

func expectFinish(m buildsignalMocks, state entity.RequestState) *gomock.Call {
	return expectOutcomeLog(m, state, 2).After(expectFinishWrites(m, state))
}

func expectOutcomeLog(m buildsignalMocks, state entity.RequestState, version int32) *gomock.Call {
	reason := map[entity.RequestState]entity.RequestOutcomeReason{
		entity.RequestStateSucceeded: entity.RequestOutcomeReasonBuildSucceeded,
		entity.RequestStateFailed:    entity.RequestOutcomeReasonBuildFailed,
		entity.RequestStateCancelled: entity.RequestOutcomeReasonBuildCancelled,
	}[state]
	request := requestWithState(state)
	request.Version = version
	log := requestlog.NewRequestStateLog(request, reason)
	log.Metadata[requestlog.MetadataKeyBuildID] = request.TerminalBuildID
	return m.materializer.EXPECT().PersistLog(
		gomock.Any(),
		m.store,
		log,
	).Return(nil)
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		setup   func(m buildsignalMocks)
		// wantHoldMs, when non-zero, expects the delivery to be held for the
		// next poll with exactly this delay. Cases without it fail on any Hold.
		wantHoldMs int64
		wantErr    bool
		wantRetry  bool
	}{
		{
			name:      "build not found is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, storage.ErrNotFound)
			},
		},
		{
			name:      "build storage error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(entity.Build{}, errors.New("db down"))
			},
		},
		{
			name:      "request not found is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, storage.ErrNotFound)
			},
		},
		{
			name:      "request storage error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(entity.Request{}, errors.New("db down"))
			},
		},
		{
			name: "superseded request is a no-op",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSuperseded), nil)
			},
		},
		{
			// Neither state can reach the poll loop today — a build only exists once
			// process admitted the request. The guard covers them anyway because
			// the terminal path has no build outcome to record for them.
			name: "request that never claimed a slot is a no-op",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateAccepted), nil)
			},
		},
		{
			name:      "factory lookup failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(nil, errors.New("no runner"))
			},
		},
		{
			name:      "status call error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatus(""), nil, errors.New("runner unavailable"))
			},
		},
		{
			name:       "unchanged status skips write and holds for next poll",
			wantHoldMs: PollDelayRunningMs,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
			},
		},
		{
			name:       "status transition persists and holds for next poll",
			wantHoldMs: PollDelayRunningMs,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
				updated := build(entity.BuildStatusRunning, 1)
				m.buildStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
			},
		},
		{
			name: "succeeded build frees the slot, marks the request and publishes to record",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), build(entity.BuildStatusSucceeded, 2), int32(2), int32(3)).Return(nil)
				logCall := expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name: "failed build marks the request failed",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusFailed, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), build(entity.BuildStatusFailed, 2), int32(2), int32(3)).Return(nil)
				logCall := expectFinish(m, entity.RequestStateFailed)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name: "cancelled build marks the request cancelled, not failed",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusCancelled, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), build(entity.BuildStatusCancelled, 2), int32(2), int32(3)).Return(nil)
				logCall := expectFinish(m, entity.RequestStateCancelled)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name: "redelivery of an already-stamped request republishes without touching the slot",
			setup: func(m buildsignalMocks) {
				request := requestWithState(entity.RequestStateSucceeded)
				request.Version = 2
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				// No build/queue/request write: the status is unchanged and the outcome
				// and bookmark are already recorded, so the slot must not be released a second time.
				eventCall := expectBuildFinished(m)
				queue := queueRow([]string{testOtherID}, 6)
				queue.LastGreenRequestID = testID
				queue.LastGreenURI = request.URI
				getCall := m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queue, nil).After(eventCall)
				logCall := expectOutcomeLog(m, entity.RequestStateSucceeded, 2).After(getCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name:    "build event failure stops before request outcome",
			wantErr: true,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				request := entity.Request{ID: testID, Queue: testQueue}
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					requestlog.NewRequestEventLog(
						request,
						entity.RequestEventBuildFinished,
						testBuildID,
						map[string]string{requestlog.MetadataKeyBuildID: testBuildID},
					),
				).Return(errors.New("db down"))
			},
		},
		{
			name:    "redelivery stops before record when outcome log persistence fails",
			wantErr: true,
			setup: func(m buildsignalMocks) {
				request := requestWithState(entity.RequestStateSucceeded)
				request.Version = 2
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				eventCall := expectBuildFinished(m)
				bookmarkCall := expectAdvanceLastGreen(m, eventCall)
				log := requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonBuildSucceeded)
				log.Metadata[requestlog.MetadataKeyBuildID] = request.TerminalBuildID
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					log,
				).Return(errors.New("db down")).After(bookmarkCall)
			},
		},
		{
			name: "write-once: stored terminal status is never overwritten",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 5), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusFailed, nil, nil)
				// No build Update: a stored terminal status is write-once. The stored
				// status, not the poll, decides the request's outcome.
				logCall := expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name:      "version conflict is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storage.ErrVersionMismatch)
			},
		},
		{
			name:      "build store update error is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(errors.New("db down"))
			},
		},
		{
			name:      "outcome write failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				eventCall := expectBuildFinished(m)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow([]string{testID}, 4), nil).After(eventCall)
				m.queueStore.EXPECT().Update(gomock.Any(), queueRow([]string{}, 4), int32(4), int32(5)).Return(nil)
				m.reqStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(errors.New("db down"))
			},
		},
		{
			name: "outcome retry after its slot was released leaves another request's slot alone",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				eventCall := expectBuildFinished(m)
				queue := queueRow([]string{testOtherID}, 5)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queue, nil).Times(2).After(eventCall)
				outcomeCall := m.reqStore.EXPECT().Update(gomock.Any(), requestWithState(entity.RequestStateSucceeded), int32(1), int32(2)).Return(nil)
				bookmarked := queue
				bookmarked.LastGreenURI = requestWithState(entity.RequestStateSucceeded).URI
				bookmarked.LastGreenRequestID = testID
				bookmarkCall := m.queueStore.EXPECT().Update(gomock.Any(), bookmarked, int32(5), int32(6)).Return(nil).After(outcomeCall)
				logCall := expectOutcomeLog(m, entity.RequestStateSucceeded, 2).After(bookmarkCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name:      "slot release failure leaves the request non-terminal",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				eventCall := expectBuildFinished(m)
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, errors.New("db down")).After(eventCall)
				// No request Update and no record publish: marking the request terminal
				// while it still holds a slot would strand the slot, since a terminal
				// request is skipped by both redelivery and the DLQ reconciler.
			},
		},
		{
			name:    "outcome log failure retains terminal request and released slot",
			wantErr: true,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				updateCall := expectFinishWrites(m, entity.RequestStateSucceeded)
				request := requestWithState(entity.RequestStateSucceeded)
				request.Version = 2
				log := requestlog.NewRequestStateLog(request, entity.RequestOutcomeReasonBuildSucceeded)
				log.Metadata[requestlog.MetadataKeyBuildID] = request.TerminalBuildID
				m.materializer.EXPECT().PersistLog(
					gomock.Any(),
					m.store,
					log,
				).Return(errors.New("db down")).After(updateCall)
			},
		},
		{
			name:      "publish to record failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(nil)
				logCall := expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(errors.New("queue down")).After(logCall)
				m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil).After(logCall)
			},
		},
		{
			name:      "malformed payload is not retryable",
			payload:   []byte("not-json"),
			wantErr:   true,
			wantRetry: false,
			setup:     func(m buildsignalMocks) {},
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
				payload = buildSignalPayload(t, testBuildID)
			}

			d := delivery(t, ctrl, payload)
			if tt.wantHoldMs > 0 {
				d.EXPECT().Hold(tt.wantHoldMs)
			}

			err := c.Process(queueContext(), d)

			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.wantRetry, errs.IsRetryable(err))
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestPublishRecordCarriesRequestID pins the record contract: the payload and the
// message id are the request id, not the build id, so record never has to reach a
// Build. The message id is stable so a redelivery dedups instead of enqueuing twice.
func TestPublishRecordCarriesRequestID(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	var got entityqueue.Message
	m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
			got = msg
			return nil
		})

	require.NoError(t, c.publishRecord(queueContext(), testID, "monorepo/main"))

	var payload stovepipemq.Record
	require.NoError(t, stovepipemq.Unmarshal(got.Payload, &payload))
	assert.Equal(t, testID, payload.Id)
	assert.Equal(t, testID, got.ID)
	assert.Equal(t, testID, got.PartitionKey)
	assert.Equal(t, "monorepo/main", got.Tenant)
	assert.Equal(t, "monorepo/main", got.Metadata[entityqueue.MetadataKeyQueueName])
}

func TestOutcomeState(t *testing.T) {
	tests := []struct {
		name     string
		status   entity.BuildStatus
		expected entity.RequestState
	}{
		{name: "succeeded", status: entity.BuildStatusSucceeded, expected: entity.RequestStateSucceeded},
		{name: "failed", status: entity.BuildStatusFailed, expected: entity.RequestStateFailed},
		{name: "cancelled", status: entity.BuildStatusCancelled, expected: entity.RequestStateCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, outcomeState(tt.status))
		})
	}
}

func TestTerminalAnalysisPublicationDoesNotGateRecordPublication(t *testing.T) {
	recordFailure := errors.New("record publish unavailable")
	analysisFailure := errors.New("analysis publish unavailable")

	for _, tt := range []struct {
		name        string
		recordErr   error
		analysisErr error
		wantRetry   bool
	}{
		{name: "both succeed"},
		{name: "record publish fails", recordErr: recordFailure},
		{name: "analysis publish fails", analysisErr: analysisFailure},
		{name: "both fail", recordErr: recordFailure, analysisErr: analysisFailure},
		{name: "transient record publish", recordErr: driver.ErrBadConn, wantRetry: true},
		{name: "transient analysis publish", analysisErr: driver.ErrBadConn, wantRetry: true},
		{name: "permanent record and transient analysis", recordErr: sql.ErrTxDone, analysisErr: driver.ErrBadConn, wantRetry: true},
		{name: "transient record and permanent analysis", recordErr: driver.ErrBadConn, analysisErr: sql.ErrTxDone, wantRetry: true},
		{name: "both permanent", recordErr: sql.ErrTxDone, analysisErr: sql.ErrTxDone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusFailed, 2), nil)
			request := requestWithState(entity.RequestStateFailed)

			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)

			m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
			m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusFailed, nil, nil)

			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil).Times(2)

			m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(tt.recordErr)

			m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message) error {
				var payload stovepipemq.BuildAnalysis
				require.NoError(t, stovepipemq.Unmarshal(msg.Payload, &payload))
				assert.Equal(t, testBuildID, payload.Id)
				assert.Equal(t, testQueue, payload.QueueName)
				assert.Equal(t, testBuildID, msg.PartitionKey)
				assert.Equal(t, testQueue, msg.Tenant)
				assert.Equal(t, publish.IntentID(testBuildID, "finished"), msg.ID)
				assert.NotEqual(t, publish.IntentID(testBuildID, "started"), msg.ID)
				return tt.analysisErr
			})

			err := c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID)))
			if tt.recordErr == nil && tt.analysisErr == nil {
				require.NoError(t, err)
			}

			if tt.recordErr != nil {
				require.ErrorIs(t, err, tt.recordErr)
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

func TestNonterminalPollDoesNotPublishTerminalAnalysis(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)

	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)

	m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
	m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)

	d := delivery(t, ctrl, buildSignalPayload(t, testBuildID))
	d.EXPECT().Hold(PollDelayRunningMs)

	require.NoError(t, c.Process(queueContext(), d))
}

func TestProcessRetriesBookmarkWithoutReleasingSlotAgain(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "write failed", true: "write committed but response lost"}[committed], func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			request := requestWithState(entity.RequestStateSucceeded)
			m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil).Times(2)
			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil).Times(2)
			m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil).Times(2)
			m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil).Times(2)
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil).Times(3)
			queue := queueRow([]string{testOtherID}, 5)
			m.queueStore.EXPECT().Get(gomock.Any(), testQueue).DoAndReturn(func(context.Context, string) (entity.Queue, error) {
				return queue, nil
			}).Times(2)
			m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(5), int32(6)).DoAndReturn(
				func(_ context.Context, updated entity.Queue, _, version int32) error {
					assert.Equal(t, []string{testOtherID}, updated.InFlightRequestIDs)
					assert.Equal(t, request.ID, updated.LastGreenRequestID)
					assert.Equal(t, request.URI, updated.LastGreenURI)
					if committed {
						queue = updated
						queue.Version = version
					}
					return driver.ErrBadConn
				})
			require.ErrorIs(t, c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID))), driver.ErrBadConn)
			if !committed {
				updated := queue
				updated.LastGreenRequestID = request.ID
				updated.LastGreenURI = request.URI
				m.queueStore.EXPECT().Update(gomock.Any(), updated, int32(5), int32(6)).Return(nil)
			}
			m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
			m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil)
			require.NoError(t, c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID))))
		})
	}
}

func TestProcessBookmarkPreservesConcurrentQueueUpdates(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)
	request := requestWithState(entity.RequestStateSucceeded)
	m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
	m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(request, nil)
	m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
	m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
	m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil).Times(2)
	stale := queueRow([]string{}, 5)
	fresh := queueRow([]string{testOtherID}, 6)
	fresh.LastGreenRequestID = "9"
	fresh.LastGreenURI = "newer-commit"
	gomock.InOrder(
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(stale, nil),
		m.queueStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(5), int32(6)).Return(storage.ErrVersionMismatch),
		m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(fresh, nil),
	)
	m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
	m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil)
	require.NoError(t, c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID))))
}

func TestProcessBookmarkUsesWinningRequestOutcome(t *testing.T) {
	for _, state := range []entity.RequestState{entity.RequestStateSucceeded, entity.RequestStateFailed} {
		t.Run(string(state), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, m := newController(t, ctrl)
			status := entity.BuildStatusSucceeded
			if state == entity.RequestStateSucceeded {
				status = entity.BuildStatusFailed
			}
			m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(status, 3), nil)
			m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
			m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
			m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(status, nil, nil)
			m.materializer.EXPECT().PersistLog(gomock.Any(), m.store, gomock.Any()).Return(nil).Times(2)
			gomock.InOrder(
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow([]string{testID}, 4), nil),
				m.queueStore.EXPECT().Update(gomock.Any(), queueRow([]string{}, 4), int32(4), int32(5)).Return(nil),
				m.reqStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(storage.ErrVersionMismatch),
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(state), nil),
			)
			if state == entity.RequestStateSucceeded {
				expectAdvanceLastGreen(m, nil)
			}
			m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
			m.publisher.EXPECT().Publish(gomock.Any(), "buildanalysis", gomock.Any()).Return(nil)
			require.NoError(t, c.Process(queueContext(), delivery(t, ctrl, buildSignalPayload(t, testBuildID))))
		})
	}
}
