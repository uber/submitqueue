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
	runnerFactory *buildrunnermock.MockFactory
	runner        *buildrunnermock.MockBuildRunner
	publisher     *mqmock.MockPublisher
}

func newController(t *testing.T, ctrl *gomock.Controller) (*Controller, buildsignalMocks) {
	t.Helper()

	m := buildsignalMocks{
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
		{Key: stovepipemq.TopicKeyRecord, Name: "record", Queue: queue},
	})
	require.NoError(t, err)

	c := NewController(zap.NewNop().Sugar(), tally.NewTestScope("test", nil), store, m.runnerFactory, registry, stovepipemq.TopicKeyBuildSignal, "stovepipe-buildsignal")
	return c, m
}

func delivery(t *testing.T, ctrl *gomock.Controller, payload []byte) consumer.Delivery {
	t.Helper()
	d := mqmock.NewMockDelivery(ctrl)
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

func TestProcess(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		setup     func(m buildsignalMocks)
		wantErr   bool
		wantRetry bool
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
			name: "recorded green request is a no-op",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateRecordedGreen), nil)
			},
		},
		{
			name: "recorded not green request is a no-op",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateRecordedNotGreen), nil)
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
			name: "unchanged status skips write and reschedules",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
				m.publisher.EXPECT().PublishAfter(gomock.Any(), "buildsignal", gomock.Any(), PollDelayRunningMs).Return(nil)
			},
		},
		{
			name: "status transition persists and reschedules",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusRunning, nil, nil)
				updated := build(entity.BuildStatusRunning, 1)
				m.buildStore.EXPECT().Update(gomock.Any(), updated, int32(1), int32(2)).Return(nil)
				m.publisher.EXPECT().PublishAfter(gomock.Any(), "buildsignal", gomock.Any(), PollDelayRunningMs).Return(nil)
			},
		},
		{
			name: "transition to terminal persists and publishes to record",
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				updated := build(entity.BuildStatusSucceeded, 2)
				m.buildStore.EXPECT().Update(gomock.Any(), updated, int32(2), int32(3)).Return(nil)
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
				// No Update call expected: a stored terminal status is write-once.
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
			name:      "publish to record failure is not retryable",
			wantErr:   true,
			wantRetry: false,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusRunning, 2), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusSucceeded, nil, nil)
				m.buildStore.EXPECT().Update(gomock.Any(), gomock.Any(), int32(2), int32(3)).Return(nil)
				m.publisher.EXPECT().Publish(gomock.Any(), "record", gomock.Any()).Return(errors.New("queue down"))
			},
		},
		{
			name:      "reschedule publish failure is retryable",
			wantErr:   true,
			wantRetry: true,
			setup: func(m buildsignalMocks) {
				m.buildStore.EXPECT().Get(gomock.Any(), testBuildID).Return(build(entity.BuildStatusAccepted, 1), nil)
				m.reqStore.EXPECT().Get(gomock.Any(), testID).Return(requestWithState(entity.RequestStateProcessing), nil)
				m.runnerFactory.EXPECT().For(buildrunner.Config{QueueName: testQueue}).Return(m.runner, nil)
				m.runner.EXPECT().Status(gomock.Any(), entity.BuildID{ID: testBuildID}).Return(entity.BuildStatusAccepted, nil, nil)
				m.publisher.EXPECT().PublishAfter(gomock.Any(), "buildsignal", gomock.Any(), PollDelayAcceptedMs).Return(errors.New("queue down"))
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

// TestPublishBuildSignalAdvancesPollGeneration is the regression test for the poll
// loop stalling. The queue dedups on (topic, partition_key, id) and the delivery that
// scheduled a re-poll is still un-acked when the re-poll is published, so a reused
// message id makes the reschedule a silent no-op and the build is never polled again.
// The generation therefore has to advance each tick. The partition key must stay the
// build id so each poll loop keeps its own partition.
func TestPublishBuildSignalAdvancesPollGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	var ids []string
	m.publisher.EXPECT().PublishAfter(gomock.Any(), "buildsignal", gomock.Any(), PollDelayRunningMs).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message, _ int64) error {
			ids = append(ids, msg.ID)
			assert.Equal(t, testBuildID, msg.PartitionKey)
			return nil
		}).Times(3)

	// Walk a chain: each publish is scheduled by the message the previous one minted.
	current := testBuildID
	for range 3 {
		require.NoError(t, c.publishBuildSignal(context.Background(), testBuildID, current, PollDelayRunningMs))
		current = ids[len(ids)-1]
	}

	assert.Equal(t, []string{
		testBuildID + "/poll/1",
		testBuildID + "/poll/2",
		testBuildID + "/poll/3",
	}, ids, "each tick must mint a fresh id, or the reschedule dedups against the message that scheduled it")
}

// TestPublishBuildSignalIsIdempotentPerDelivery pins the other half of the contract:
// the next id is a pure function of the delivery being processed. A redelivery racing
// the original computes the same id, so dedup collapses them and the build keeps a
// single poll chain rather than forking a second one that doubles the poll rate and
// races the first one's status CAS.
func TestPublishBuildSignalIsIdempotentPerDelivery(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, m := newController(t, ctrl)

	var ids []string
	m.publisher.EXPECT().PublishAfter(gomock.Any(), "buildsignal", gomock.Any(), PollDelayRunningMs).
		DoAndReturn(func(_ context.Context, _ string, msg entityqueue.Message, _ int64) error {
			ids = append(ids, msg.ID)
			return nil
		}).Times(2)

	scheduledBy := testBuildID + "/poll/7"
	require.NoError(t, c.publishBuildSignal(context.Background(), testBuildID, scheduledBy, PollDelayRunningMs))
	require.NoError(t, c.publishBuildSignal(context.Background(), testBuildID, scheduledBy, PollDelayRunningMs))

	assert.Equal(t, ids[0], ids[1], "a redelivery of the same message must republish the same id so dedup collapses it")
	assert.Equal(t, testBuildID+"/poll/8", ids[0])
}

func TestNextPollMessageID(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		expected string
	}{
		{name: "initial publish from build starts at one", current: testBuildID, expected: testBuildID + "/poll/1"},
		{name: "advances the generation", current: testBuildID + "/poll/4", expected: testBuildID + "/poll/5"},
		{name: "unparsable generation restarts at one", current: testBuildID + "/poll/x", expected: testBuildID + "/poll/1"},
		{name: "another build's id is not a prefix match", current: "other/poll/9", expected: testBuildID + "/poll/1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, nextPollMessageID(testBuildID, tt.current))
		})
	}
}
