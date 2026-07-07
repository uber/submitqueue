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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/submitqueue/orchestrator/protopb"
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
	assert.Equal(t, "pong!", resp.Message)
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
	assert.Equal(t, "orchestrator", resp.ServiceName)
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
