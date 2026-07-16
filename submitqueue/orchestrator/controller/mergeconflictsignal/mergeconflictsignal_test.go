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

package mergeconflictsignal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	runwaymq "github.com/uber/submitqueue/api/runway/messagequeue"
	runwaypb "github.com/uber/submitqueue/api/runway/messagequeue/protopb"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"
)

func resultPayload(t *testing.T, res runwaymq.MergeResult) []byte {
	payload, err := runwaymq.Marshal(&res)
	require.NoError(t, err)
	return payload
}

func newDelivery(ctrl *gomock.Controller, msg entityqueue.Message) *queuemock.MockDelivery {
	d := queuemock.NewMockDelivery(ctrl)
	d.EXPECT().Message().Return(msg).AnyTimes()
	d.EXPECT().Attempt().Return(1).AnyTimes()
	return d
}

const (
	testRequestID = "test-queue/1"
	testQueue     = "test-queue"
)

func TestProcess_MergeablePublishesToBatch(t *testing.T) {
	ctrl := gomock.NewController(t)

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testRequestID).Return(
		entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateStarted, Version: 1}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	published := make(map[string][]byte)
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			published[topic] = msg.Payload
			return nil
		},
	).Times(2)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyBatch, Name: "batch", Queue: q},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: q},
		},
	)
	require.NoError(t, err)

	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, registry,
		runwaymq.TopicKeyMergeConflictCheckSignal, "orchestrator-mergeconflictsignal")

	res := runwaymq.MergeResult{Id: testRequestID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testRequestID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(ctrl, msg)))

	logEntry, err := entity.RequestLogFromBytes(published["log"])
	require.NoError(t, err)
	assert.Equal(t, entity.RequestStatusValidated, logEntry.Status)
	assert.Equal(t, "mergeconflictsignal", logEntry.Metadata["controller"])

	rid, err := entity.RequestIDFromBytes(published["batch"])
	require.NoError(t, err)
	assert.Equal(t, testRequestID, rid.ID)
}

func TestProcess_NotMergeableMarksRequestError(t *testing.T) {
	ctrl := gomock.NewController(t)

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testRequestID).Return(
		entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateStarted, Version: 1}, nil)
	// The request is driven to terminal Error inline (version 1 -> 2).
	reqStore.EXPECT().UpdateState(gomock.Any(), testRequestID, int32(1), int32(2), entity.RequestStateError).Return(nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	// One publish is expected — the terminal log entry to the log topic. A publish
	// to the batch topic would be a bug (gomock fails on the unexpected 2nd call).
	var gotTopic string
	var gotPayload []byte
	pub := queuemock.NewMockPublisher(ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, msg entityqueue.Message) error {
			gotTopic = topic
			gotPayload = msg.Payload
			return nil
		},
	)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{
			{Key: topickey.TopicKeyBatch, Name: "batch", Queue: q},
			{Key: topickey.TopicKeyLog, Name: "log", Queue: q},
		},
	)
	require.NoError(t, err)

	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, registry,
		runwaymq.TopicKeyMergeConflictCheckSignal, "orchestrator-mergeconflictsignal")

	res := runwaymq.MergeResult{Id: testRequestID, Outcome: runwaypb.Outcome_FAILED, Reason: "conflict in foo.go"}
	msg := entityqueue.NewMessage(testRequestID, resultPayload(t, res), testQueue, nil)
	// Not-mergeable is an expected terminal outcome, so Process acks (no error).
	require.NoError(t, controller.Process(context.Background(), newDelivery(ctrl, msg)))

	// The single publish is the terminal log entry carrying the conflict reason.
	assert.Equal(t, "log", gotTopic)
	logEntry, err := entity.RequestLogFromBytes(gotPayload)
	require.NoError(t, err)
	assert.Equal(t, entity.RequestStatusError, logEntry.Status)
	assert.Equal(t, int32(2), logEntry.RequestVersion)
	assert.Equal(t, "conflict in foo.go", logEntry.LastError)
}

func TestProcess_HaltedRequestSkips(t *testing.T) {
	ctrl := gomock.NewController(t)

	reqStore := storagemock.NewMockRequestStore(ctrl)
	reqStore.EXPECT().Get(gomock.Any(), testRequestID).Return(
		entity.Request{ID: testRequestID, Queue: testQueue, State: entity.RequestStateCancelled, Version: 4}, nil)

	store := storagemock.NewMockStorage(ctrl)
	store.EXPECT().GetRequestStore().Return(reqStore).AnyTimes()

	// No publish: gomock fails if a batch publish runs for a halted request.
	pub := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()
	registry, err := consumer.NewTopicRegistry(
		[]consumer.TopicConfig{{Key: topickey.TopicKeyBatch, Name: "batch", Queue: q}},
	)
	require.NoError(t, err)

	controller := NewController(zaptest.NewLogger(t).Sugar(), tally.NoopScope, store, registry,
		runwaymq.TopicKeyMergeConflictCheckSignal, "orchestrator-mergeconflictsignal")

	res := runwaymq.MergeResult{Id: testRequestID, Outcome: runwaypb.Outcome_SUCCEEDED}
	msg := entityqueue.NewMessage(testRequestID, resultPayload(t, res), testQueue, nil)
	require.NoError(t, controller.Process(context.Background(), newDelivery(ctrl, msg)))
}
