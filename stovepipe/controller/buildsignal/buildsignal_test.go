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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	consumermock "github.com/uber/submitqueue/platform/consumer/mock"
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
	testBuildID = "bk-1"
)

// buildsignalMocks bundles the mocks a buildsignal controller test case wires
// expectations on.
type buildsignalMocks struct {
	reqStore      *storagemock.MockRequestStore
	buildStore    *storagemock.MockBuildStore
	queueStore    *storagemock.MockQueueStore
	runnerFactory *buildrunnermock.MockFactory
	runner        *buildrunnermock.MockBuildRunner
	publisher     *mqmock.MockPublisher
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, buildsignalMocks) {
	t.Helper()

	m := buildsignalMocks{
		reqStore:      storagemock.NewMockRequestStore(ctrl),
		buildStore:    storagemock.NewMockBuildStore(ctrl),
		queueStore:    storagemock.NewMockQueueStore(ctrl),
		runnerFactory: buildrunnermock.NewMockFactory(ctrl),
		runner:        buildrunnermock.NewMockBuildRunner(ctrl),
		publisher:     mqmock.NewMockPublisher(ctrl),
	}

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(m.reqStore).AnyTimes()
	store.EXPECT().GetBuildStore().Return(m.buildStore).AnyTimes()
	store.EXPECT().GetQueueStore().Return(m.queueStore).AnyTimes()

	queue := mqmock.NewMockQueue(ctrl)
	queue.EXPECT().Publisher().Return(m.publisher).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: stovepipemq.TopicKeyBuildSignal, Name: "buildsignal", Queue: queue},
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, m.runnerFactory, registry, stovepipemq.TopicKeyBuildSignal, "stovepipe-buildsignal")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) *consumermock.MockDelivery {
	t.Helper()
	d := consumermock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(entityqueue.NewMessage(testBuildID, payload, testBuildID, nil)).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

func buildSignalPayload(t *testing.T, id string) []byte {
	t.Helper()
	b, err := stovepipemq.Marshal(&stovepipemq.BuildSignal{Id: id})
	require.NoError(t, err)
	return b
}

// requestWithState returns a Request past process's admit, in the given state.
func requestWithState(state entity.RequestState) entity.Request {
	return entity.Request{
		ID:      testID,
		Queue:   testQueue,
		State:   state,
		Version: 1,
	}
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

// queueRow returns the testQueue's row holding inFlight admitted validations.
func queueRow(inFlight, version int32) entity.Queue {
	return entity.Queue{
		Name:          testQueue,
		InFlightCount: inFlight,
		Version:       version,
	}
}

// expectFinish wires the slot release and outcome transition a terminal build
// performs: the queue row is CAS-decremented, then the request is CAS-moved from
// processing to state.
func expectFinish(m buildsignalMocks, state entity.RequestState) {
	m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow(1, 4), nil)
	m.queueStore.EXPECT().Update(gomock.Any(), queueRow(0, 4), int32(4), int32(5)).Return(nil)
	m.reqStore.EXPECT().Update(gomock.Any(), requestWithState(state), int32(1), int32(2)).Return(nil)
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
			// finishRequest would release a build slot they never claimed.
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
				expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
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
				expectFinish(m, entity.RequestStateFailed)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
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
				expectFinish(m, entity.RequestStateCancelled)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
			},
		},
		{
			name: "redelivery of an already-stamped request republishes without touching the slot",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusSucceeded, 3), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateSucceeded), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				// No build/queue/request write: the status is unchanged and the outcome
				// is already recorded, so the slot must not be released a second time.
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
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
				expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(nil)
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
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(queueRow(1, 4), nil)
				m.queueStore.EXPECT().Update(gomock.Any(), queueRow(0, 4), int32(4), int32(5)).Return(nil)
				m.reqStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(1), int32(2)).Return(errors.New("db down"))
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
				m.queueStore.EXPECT().Get(gomock.Any(), testQueue).Return(entity.Queue{}, errors.New("db down"))
				// No request Update and no record publish: marking the request terminal
				// while it still holds a slot would strand the slot, since a terminal
				// request is skipped by both redelivery and the DLQ reconciler.
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
				expectFinish(m, entity.RequestStateSucceeded)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(errors.New("queue down"))
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

			err := c.Process(context.Background(), d)

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

	require.NoError(t, c.publishRecord(context.Background(), testID))

	var payload stovepipemq.Record
	require.NoError(t, stovepipemq.Unmarshal(got.Payload, &payload))
	assert.Equal(t, testID, payload.Id)
	assert.Equal(t, testID, got.ID)
	assert.Equal(t, testID, got.PartitionKey)
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
