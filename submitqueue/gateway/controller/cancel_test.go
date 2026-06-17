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

package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	"github.com/uber/submitqueue/platform/consumer"
	"github.com/uber/submitqueue/platform/errs"
	queuemock "github.com/uber/submitqueue/platform/extension/messagequeue/mock"
	"github.com/uber/submitqueue/submitqueue/core/topickey"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// newCancelTestRegistry builds a single-entry TopicRegistry for TopicKeyCancel wired
// to a mock Queue/Publisher and returns both the registry and the publisher mock.
func newCancelTestRegistry(t *testing.T, ctrl *gomock.Controller) (consumer.TopicRegistry, *queuemock.MockPublisher) {
	t.Helper()
	pub := queuemock.NewMockPublisher(ctrl)
	q := queuemock.NewMockQueue(ctrl)
	q.EXPECT().Publisher().Return(pub).AnyTimes()

	registry, err := consumer.NewTopicRegistry([]consumer.TopicConfig{
		{Key: topickey.TopicKeyCancel, Name: "cancel", Queue: q},
	})
	require.NoError(t, err)
	return registry, pub
}

// newCancelTestRegistryWithNoopPublisher returns a registry whose publisher silently accepts any Publish call.
func newCancelTestRegistryWithNoopPublisher(t *testing.T, ctrl *gomock.Controller) consumer.TopicRegistry {
	t.Helper()
	registry, pub := newCancelTestRegistry(t, ctrl)
	pub.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return registry
}

// newRequestLogStoreNoop returns a RequestLogStore mock whose List returns a single
// dummy entry (so existence check passes) and whose Insert silently succeeds for any input.
func newRequestLogStoreNoop(t *testing.T, ctrl *gomock.Controller) *storagemock.MockRequestLogStore {
	t.Helper()
	store := storagemock.NewMockRequestLogStore(ctrl)
	store.EXPECT().List(gomock.Any(), gomock.Any()).Return([]entity.RequestLog{{}}, nil).AnyTimes()
	store.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return store
}

func TestNewCancelController(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newRequestLogStoreNoop(t, ctrl), newCancelTestRegistryWithNoopPublisher(t, ctrl))
	require.NotNil(t, controller)
}

func TestCancel_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newRequestLogStoreNoop(t, ctrl), newCancelTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "test-queue/42", Reason: "user changed their mind"}
	resp, err := controller.Cancel(ctx, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestCancel_ReturnsErrorOnEmptySqid(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newRequestLogStoreNoop(t, ctrl), newCancelTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "", Reason: "anything"}
	_, err := controller.Cancel(ctx, req)

	require.Error(t, err)
	assert.True(t, IsInvalidRequest(err))
}

func TestCancel_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage entityqueue.Message

	ctrl := gomock.NewController(t)

	registry, publisher := newCancelTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg entityqueue.Message) error {
			publishedTopic = topic
			publishedMessage = msg
			return nil
		},
	)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newRequestLogStoreNoop(t, ctrl), registry)
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "my-queue/7", Reason: "obsolete change"}
	_, err := controller.Cancel(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, "cancel", publishedTopic)
	assert.Equal(t, "my-queue/7", publishedMessage.ID)
	assert.Equal(t, "my-queue/7", publishedMessage.PartitionKey)

	deserialized, err := entity.CancelRequestFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "my-queue/7", deserialized.ID)
	assert.Equal(t, "obsolete change", deserialized.Reason)
}

// TestCancel_InsertsCancellingLog asserts that Cancel records a RequestStatusCancelling
// log entry (intent) carrying the reason in metadata, and that the entry is written
// before the cancel topic publish so observers see intent the moment Cancel returns.
func TestCancel_InsertsCancellingLog(t *testing.T) {
	ctrl := gomock.NewController(t)

	var insertedLog entity.RequestLog
	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().List(gomock.Any(), "my-queue/42").Return([]entity.RequestLog{{}}, nil)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, entry entity.RequestLog) error {
			insertedLog = entry
			return nil
		},
	).Times(1)

	registry, publisher := newCancelTestRegistry(t, ctrl)
	insertedBeforePublish := false
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ entityqueue.Message) error {
			insertedBeforePublish = insertedLog.RequestID != ""
			return nil
		},
	)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, logStore, registry)

	req := &pb.CancelRequest{Sqid: "my-queue/42", Reason: "obsolete change"}
	_, err := controller.Cancel(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, "my-queue/42", insertedLog.RequestID)
	assert.Equal(t, entity.RequestStatusCancelling, insertedLog.Status)
	assert.Equal(t, "obsolete change", insertedLog.Metadata["reason"])
	assert.True(t, insertedBeforePublish, "log entry must be inserted before publish to the cancel topic")
}

// TestCancel_LogInsertFailure asserts that a failure to insert the Cancelling log entry
// short-circuits the RPC with an error and the cancel topic is never published to.
func TestCancel_LogInsertFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().List(gomock.Any(), "q/1").Return([]entity.RequestLog{{}}, nil)
	logStore.EXPECT().Insert(gomock.Any(), gomock.Any()).Return(fmt.Errorf("db unavailable"))

	registry, publisher := newCancelTestRegistry(t, ctrl)
	// No Publish expectation: log insert must fail before publish runs.
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, logStore, registry)
	_, err := controller.Cancel(context.Background(), &pb.CancelRequest{Sqid: "q/1"})
	require.Error(t, err)
}

func TestCancel_ReturnsErrorOnPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	registry, publisher := newCancelTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newRequestLogStoreNoop(t, ctrl), registry)
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "test-queue/1"}
	_, err := controller.Cancel(ctx, req)

	require.Error(t, err)
}

// TestCancel_UnknownSqidIsUserError asserts that Cancel for a sqid with no
// request_log history fails fast with a RequestNotFoundError (user error) and
// never inserts a cancelling log row or publishes to the cancel topic.
func TestCancel_UnknownSqidIsUserError(t *testing.T) {
	ctrl := gomock.NewController(t)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().List(gomock.Any(), "ghost/1").Return(nil, storage.ErrNotFound)
	// No Insert expectation: existence check must short-circuit before Insert.

	registry, publisher := newCancelTestRegistry(t, ctrl)
	// No Publish expectation: existence check must short-circuit before Publish.
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, logStore, registry)
	_, err := controller.Cancel(context.Background(), &pb.CancelRequest{Sqid: "ghost/1"})
	require.Error(t, err)
	assert.True(t, IsRequestNotFound(err))
	assert.True(t, errs.IsUserError(err))
	assert.False(t, errs.IsRetryable(err))

	var typed *RequestNotFoundError
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, "ghost/1", typed.Sqid)
}

// TestCancel_RequestLogLookupFailure asserts that an infrastructure failure on
// the existence check propagates as a (non-user) error and skips the rest of
// the pipeline.
func TestCancel_RequestLogLookupFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	logStore := storagemock.NewMockRequestLogStore(ctrl)
	logStore.EXPECT().List(gomock.Any(), "q/1").Return(nil, fmt.Errorf("log backend down"))

	registry, publisher := newCancelTestRegistry(t, ctrl)
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, logStore, registry)
	_, err := controller.Cancel(context.Background(), &pb.CancelRequest{Sqid: "q/1"})
	require.Error(t, err)
	assert.False(t, errs.IsUserError(err))
	assert.False(t, IsRequestNotFound(err))
}
