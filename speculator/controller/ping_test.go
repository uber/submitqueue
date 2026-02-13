package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/speculator/protopb"
)

func TestNewPingController(t *testing.T) {
	controller := NewPingController(nil, nil)
	require.NotNil(t, controller)
}

func TestPing_DefaultMessage(t *testing.T) {
	controller := NewPingController(nil, nil)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "pong", resp.Message)
}

func TestPing_CustomMessage(t *testing.T) {
	controller := NewPingController(nil, nil)
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
	controller := NewPingController(nil, nil)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "speculator", resp.ServiceName)
}

func TestPing_Timestamp(t *testing.T) {
	controller := NewPingController(nil, nil)
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
	controller := NewPingController(nil, nil)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := controller.Ping(ctx, req)

	require.NoError(t, err)
	assert.NotEmpty(t, resp.Hostname)
}
