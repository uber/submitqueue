package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

func TestNewPingController(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	require.NotNil(t, controller)
}

func TestPing_DefaultMessage(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "pong", resp.Message)
}

func TestPing_CustomMessage(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple message", "hello", "echo: hello"},
		{"message with spaces", "hello world", "echo: hello world"},
		{"special characters", "test!@#", "echo: test!@#"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &pb.PingRequest{Message: tc.input}
			resp, err := controller.Ping(ctx, req)

			require.NoError(t, err)
			assert.Equal(t, tc.expected, resp.Message)
		})
	}
}

func TestPing_ServiceName(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "gateway", resp.ServiceName)
}

func TestPing_Timestamp(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	before := time.Now().Unix()
	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)
	after := time.Now().Unix()

	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.Timestamp, before)
	assert.LessOrEqual(t, resp.Timestamp, after)
}

func TestPing_Hostname(t *testing.T) {
	controller := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.NotEmpty(t, resp.Hostname)
}
