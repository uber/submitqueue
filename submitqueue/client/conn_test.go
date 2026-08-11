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

package client

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

func TestNewRejectsAnEmptyAddress(t *testing.T) {
	_, err := New(Options{})
	require.Error(t, err)
}

// TestCredentialsReachTheServerOverPlaintext is the one that matters for a
// local stack: gRPC refuses to attach per-RPC credentials to an insecure
// connection unless they say they do not need transport security, so a token
// that works over TLS can silently never be sent without it.
func TestCredentialsReachTheServerOverPlaintext(t *testing.T) {
	tests := []struct {
		name     string
		tokenEnv string
		token    string
		want     string
	}{
		{
			name:     "a token is sent as a bearer credential",
			tokenEnv: "SQ_TEST_CONN_TOKEN",
			token:    "s3cret",
			want:     "Bearer s3cret",
		},
		{
			name:     "an unset variable sends nothing",
			tokenEnv: "SQ_TEST_CONN_TOKEN",
		},
		{
			name:  "no variable named sends nothing",
			token: "s3cret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tokenEnv != "" {
				t.Setenv(tt.tokenEnv, tt.token)
			}

			gw := &recordingGateway{}
			addr, stop := serve(t, gw)
			defer stop()

			sq, err := New(Options{Addr: addr, TokenEnv: tt.tokenEnv})
			require.NoError(t, err)
			defer sq.Close()

			_, err = sq.Ping(context.Background(), "hello")
			require.NoError(t, err)

			assert.Equal(t, tt.want, gw.authorization())
		})
	}
}

// recordingGateway answers Ping and remembers what metadata the call carried.
type recordingGateway struct {
	pb.UnimplementedSubmitQueueGatewayServer
	md metadata.MD
}

func (g *recordingGateway) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	g.md, _ = metadata.FromIncomingContext(ctx)
	return &pb.PingResponse{Message: req.GetMessage()}, nil
}

// authorization is the single Authorization header the last call carried, or
// empty when it carried none.
func (g *recordingGateway) authorization() string {
	values := g.md.Get("authorization")
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// serve starts a real gRPC server on a loopback port and returns its address.
// A real listener rather than an in-memory pipe, so the dialling path under
// test is the one a caller actually uses.
func serve(t *testing.T, gw pb.SubmitQueueGatewayServer) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb.RegisterSubmitQueueGatewayServer(srv, gw)
	go func() { _ = srv.Serve(lis) }()

	return lis.Addr().String(), srv.Stop
}
