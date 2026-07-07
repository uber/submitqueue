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
	pb "github.com/uber/submitqueue/api/runway/protopb"
	"go.uber.org/zap"
)

func TestNewPingController(t *testing.T) {
	ctrl := NewPingController(zap.NewNop(), tally.NoopScope)
	require.NotNil(t, ctrl)
}

func TestPing_DefaultMessage(t *testing.T) {
	ctrl := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := ctrl.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "pong!", resp.Message)
}

func TestPing_ServiceName(t *testing.T) {
	ctrl := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.PingRequest{}
	resp, err := ctrl.Ping(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, "runway", resp.ServiceName)
}

func TestPing_Timestamp(t *testing.T) {
	ctrl := NewPingController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	before := time.Now().Unix()
	req := &pb.PingRequest{}
	resp, err := ctrl.Ping(ctx, req)
	after := time.Now().Unix()

	require.NoError(t, err)
	assert.GreaterOrEqual(t, resp.Timestamp, before)
	assert.LessOrEqual(t, resp.Timestamp, after)
}
