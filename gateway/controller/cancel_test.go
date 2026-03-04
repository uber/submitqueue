package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/entity/queue"
	queuemock "github.com/uber/submitqueue/extension/queue/mock"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestNewCancelController(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, noopPublisher(ctrl), "cancel")
	require.NotNil(t, controller)
}

func TestCancel_ReturnsErrorOnEmptySqid(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, noopPublisher(ctrl), "cancel")
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: ""}
	resp, err := controller.Cancel(ctx, req)

	require.NoError(t, err)
	assert.Empty(t, resp.Sqid)
	assert.Equal(t, pb.RequestStatus_UNKNOWN, resp.CurrentStatus)
	require.NotNil(t, resp.Error)
	assert.Equal(t, "sqid is required", resp.Error.Message)
}

func TestCancel_ReturnsSqid(t *testing.T) {
	ctrl := gomock.NewController(t)
	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, noopPublisher(ctrl), "cancel")
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "test-queue/123"}
	resp, err := controller.Cancel(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "test-queue/123", resp.Sqid)
	assert.Equal(t, pb.RequestStatus_CANCELLATION_ACCEPTED, resp.CurrentStatus)
}

func TestCancel_PublishesToQueue(t *testing.T) {
	var publishedTopic string
	var publishedMessage queue.Message

	ctrl := gomock.NewController(t)
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, topic string, msg queue.Message) error {
			publishedTopic = topic
			publishedMessage = msg
			return nil
		},
	)

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, publisher, "cancel")
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "my-queue/42"}
	resp, err := controller.Cancel(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "my-queue/42", resp.Sqid)
	assert.Equal(t, pb.RequestStatus_CANCELLATION_ACCEPTED, resp.CurrentStatus)

	// Verify message was published to the cancel topic
	assert.Equal(t, "cancel", publishedTopic)
	assert.Equal(t, "my-queue/42", publishedMessage.ID)
	assert.Equal(t, "my-queue/42", publishedMessage.PartitionKey)

	// Verify payload is a serialized Cancel entity
	cancel, err := entity.CancelFromBytes(publishedMessage.Payload)
	require.NoError(t, err)
	assert.Equal(t, "my-queue/42", cancel.Sqid)
}

func TestCancel_ReturnsErrorOnPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	publisher := queuemock.NewMockPublisher(ctrl)
	publisher.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("queue unavailable"))

	controller := NewCancelController(zap.NewNop().Sugar(), tally.NoopScope, publisher, "cancel")
	ctx := context.Background()

	req := &pb.CancelRequest{Sqid: "test-queue/999"}
	_, err := controller.Cancel(ctx, req)

	require.Error(t, err)
}
