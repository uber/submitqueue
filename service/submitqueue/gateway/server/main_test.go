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

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	qcmock "github.com/uber/submitqueue/submitqueue/extension/queueconfig/mock"
	"github.com/uber/submitqueue/submitqueue/gateway/controller"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGatewayServerListQueues(t *testing.T) {
	configErr := errors.New("queue config unavailable")
	tests := []struct {
		name   string
		queues []entity.QueueConfig
		err    error
		want   []string
	}{
		{
			name:   "configured queues exposed in name order",
			queues: []entity.QueueConfig{{Name: "release"}, {Name: "main"}},
			want:   []string{"main", "release"},
		},
		{
			name: "no configured queues",
			want: []string{},
		},
		{
			name: "configuration failure",
			err:  configErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueConfigs := qcmock.NewMockStore(gomock.NewController(t))
			queueConfigs.EXPECT().List(gomock.Any()).Return(tt.queues, tt.err)
			server := &GatewayServer{
				listQueuesController: controller.NewListQueuesController(zap.NewNop().Sugar(), tally.NoopScope, queueConfigs),
			}

			resp, err := server.ListQueues(context.Background(), &pb.ListQueuesRequest{})
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			names := make([]string, 0, len(resp.GetQueues()))
			for _, queue := range resp.GetQueues() {
				names = append(names, queue.GetName())
			}
			assert.Equal(t, tt.want, names)
		})
	}
}

func TestValidateConfiguredQueueTenants(t *testing.T) {
	require.NoError(t, validateConfiguredQueueTenants(
		[]string{"queue-a", "queue-b"},
		[]string{"queue-b", "queue-a"},
	))
	require.Error(t, validateConfiguredQueueTenants(
		[]string{"queue-a"},
		[]string{"queue-a", "queue-b"},
	))
}

func TestGatewayStatusError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "invalid request",
			err:  controller.ErrInvalidRequest,
			code: codes.InvalidArgument,
		},
		{
			name: "unrecognized queue",
			err:  &controller.UnrecognizedQueueError{Queue: "missing"},
			code: codes.InvalidArgument,
		},
		{
			name: "request not found",
			err:  &controller.RequestNotFoundError{Sqid: "queue/1"},
			code: codes.NotFound,
		},
		{
			name: "too many change requests",
			err:  &controller.TooManyChangeRequestsError{ChangeURI: "uri", Limit: 100},
			code: codes.ResourceExhausted,
		},
		{
			name: "internal consistency",
			err:  &controller.InternalConsistencyError{Message: "inconsistent"},
			code: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.code, status.Code(gatewayStatusError(tt.err)))
		})
	}

	infraErr := errors.New("storage unavailable")
	assert.Equal(t, infraErr, gatewayStatusError(infraErr))
}
