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
	storagemock "github.com/uber/submitqueue/submitqueue/extension/storage/mock"
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

// newCancelStorageFixture returns a storage fixture with one received request.
func newCancelStorageFixture(ctrl *gomock.Controller, requestID string) *controllerStorageFixture {
	fixture := newControllerStorageFixture(ctrl)
	if requestID != "" {
		fixture.addSummary(entity.RequestSummary{
			RequestID: requestID, Queue: "test-queue", ChangeURIs: []string{}, ReceivedAtMs: 1,
			Status: entity.RequestStatusAccepted, StatusTimestampMs: 1, Version: 1, Metadata: map[string]string{},
		})
	}
	return fixture
}

// testCancelRequest returns a valid entity.CancelRequest for testing.
func testCancelRequest(sqid string, reason string) entity.CancelRequest {
	return entity.CancelRequest{ID: sqid, Reason: reason}
}

func TestNewCancelController(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newCancelStorageFixture(ctrl, "test-queue/42").storage, newCancelTestRegistryWithNoopPublisher(t, ctrl))
	require.NotNil(t, controller)
}

func TestCancel_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newCancelStorageFixture(ctrl, "test-queue/42").storage, newCancelTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	err := controller.Cancel(ctx, testCancelRequest("test-queue/42", "user changed their mind"))

	require.NoError(t, err)
}

func TestCancel_ReturnsErrorOnEmptySqid(t *testing.T) {
	ctrl := gomock.NewController(t)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newCancelStorageFixture(ctrl, "test-queue/42").storage, newCancelTestRegistryWithNoopPublisher(t, ctrl))
	ctx := context.Background()

	err := controller.Cancel(ctx, testCancelRequest("", "anything"))

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

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newCancelStorageFixture(ctrl, "my-queue/7").storage, registry)
	ctx := context.Background()

	err := controller.Cancel(ctx, testCancelRequest("my-queue/7", "obsolete change"))
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
	fixture := newCancelStorageFixture(ctrl, "my-queue/42")

	registry, publisher := newCancelTestRegistry(t, ctrl)
	insertedBeforePublish := false
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ entityqueue.Message) error {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			insertedBeforePublish = len(fixture.logs) == 1
			return nil
		},
	)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, fixture.storage, registry)

	err := controller.Cancel(context.Background(), testCancelRequest("my-queue/42", "obsolete change"))
	require.NoError(t, err)

	fixture.mu.Lock()
	require.Len(t, fixture.logs, 1)
	insertedLog := fixture.logs[0]
	fixture.mu.Unlock()
	assert.Equal(t, "my-queue/42", insertedLog.RequestID)
	assert.Equal(t, entity.RequestStatusCancelling, insertedLog.Status)
	assert.Equal(t, "obsolete change", insertedLog.Metadata["reason"])
	assert.True(t, insertedBeforePublish, "log entry must be inserted before publish to the cancel topic")
}

// TestCancel_LogInsertFailure asserts that a failure to insert the Cancelling log entry
// short-circuits the RPC with an error and the cancel topic is never published to.
func TestCancel_LogInsertFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	fixture := newCancelStorageFixture(ctrl, "q/1")
	fixture.setLogInsertError(fmt.Errorf("db unavailable"))

	registry, publisher := newCancelTestRegistry(t, ctrl)
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, fixture.storage, registry)
	err := controller.Cancel(context.Background(), testCancelRequest("q/1", ""))
	require.Error(t, err)
}

func TestCancel_ReturnsErrorOnPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	registry, publisher := newCancelTestRegistry(t, ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, newCancelStorageFixture(ctrl, "test-queue/1").storage, registry)
	ctx := context.Background()

	err := controller.Cancel(ctx, testCancelRequest("test-queue/1", ""))

	require.Error(t, err)
}

func TestCancel_UnknownSqidIsUserError(t *testing.T) {
	ctrl := gomock.NewController(t)
	fixture := newCancelStorageFixture(ctrl, "")

	registry, publisher := newCancelTestRegistry(t, ctrl)
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, fixture.storage, registry)
	err := controller.Cancel(context.Background(), testCancelRequest("ghost/1", ""))
	require.Error(t, err)
	assert.True(t, IsRequestNotFound(err))
	assert.True(t, errs.IsUserError(err))
	assert.False(t, errs.IsRetryable(err))

	var typed *RequestNotFoundError
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, "ghost/1", typed.Sqid)
}

// TestCancel_RequestSummaryLookupFailure asserts that an infrastructure failure on
// the existence check propagates as a (non-user) error and skips the rest of
// the pipeline.
func TestCancel_RequestSummaryLookupFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	store := storagemock.NewMockStorage(ctrl)
	summaryStore := storagemock.NewMockRequestSummaryStore(ctrl)
	store.EXPECT().GetRequestSummaryStore().Return(summaryStore)
	summaryStore.EXPECT().Get(gomock.Any(), "q/1").Return(entity.RequestSummary{}, fmt.Errorf("summary backend down"))

	registry, publisher := newCancelTestRegistry(t, ctrl)
	_ = publisher

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, store, registry)
	err := controller.Cancel(context.Background(), testCancelRequest("q/1", ""))
	require.Error(t, err)
	assert.False(t, errs.IsUserError(err))
	assert.False(t, IsRequestNotFound(err))
}
