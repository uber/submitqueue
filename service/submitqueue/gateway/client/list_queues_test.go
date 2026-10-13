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
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRunListQueues(t *testing.T) {
	tests := []struct {
		name   string
		queues []*pb.Queue
		err    error
		want   string
	}{
		{
			name:   "one name per line",
			queues: []*pb.Queue{{Name: "main"}, {Name: "release"}},
			want:   "main\nrelease\n",
		},
		{
			name: "no configured queues produce no output",
		},
		{
			name: "gateway failure produces no output",
			err:  status.Error(codes.Unavailable, "queue config unavailable"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			pb.RegisterSubmitQueueGatewayServer(server, &queueListingGateway{queues: tt.queues, err: tt.err})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)

			sq, err := client.New(client.Options{Addr: listener.Addr().String()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = sq.Close() })
			var output bytes.Buffer

			err = runListQueues(context.Background(), sq, nil, &output)
			if tt.err != nil {
				require.Error(t, err)
				assert.Equal(t, status.Code(tt.err), status.Code(err))
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, output.String())
		})
	}
}

func TestRunListQueuesRejectsArguments(t *testing.T) {
	for _, args := range [][]string{{"extra"}, {"-queue", "main"}} {
		t.Run(args[0], func(t *testing.T) {
			require.Error(t, runListQueues(context.Background(), nil, args, io.Discard))
		})
	}
}

type queueListingGateway struct {
	pb.UnimplementedSubmitQueueGatewayServer
	queues []*pb.Queue
	err    error
}

func (g *queueListingGateway) ListQueues(context.Context, *pb.ListQueuesRequest) (*pb.ListQueuesResponse, error) {
	if g.err != nil {
		return nil, g.err
	}
	return &pb.ListQueuesResponse{Queues: g.queues}, nil
}
