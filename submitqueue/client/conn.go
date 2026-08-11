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

// Package client is the SubmitQueue client: dialling a gateway, the calls a
// caller makes against it, and the terminal view of what a queue is doing.
//
// It exists so the tools are thin. A binary here is flag parsing over this
// package, which is what keeps the gateway CLI and the demo from growing their
// own dialling, their own strategy parsing, and their own status table.
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
)

// DefaultTokenEnv is the environment variable a client reads its bearer token
// from unless told otherwise.
const DefaultTokenEnv = "SQ_TOKEN"

// Options is how a caller reaches a gateway.
//
// It is a plain struct rather than a set of flags so the package stays usable
// from a test or another program; binaries parse their own flags and fill it.
type Options struct {
	// Addr is the gRPC target. It is passed to the dialler untouched, so the
	// full target syntax works — a plain host:port, but also dns:///host:port
	// or unix:///path/to.sock.
	Addr string

	// TLS dials with transport security instead of plaintext. Off by default,
	// which is what a local stack wants and nothing else should.
	TLS bool

	// TokenEnv names the environment variable holding a bearer token. The
	// variable is named rather than the token passed directly, so a credential
	// never reaches a command line, where it would be visible in the shell
	// history and to anyone running ps. Empty sends no credential.
	TokenEnv string
}

// Client is a connected gateway client.
type Client struct {
	conn *grpc.ClientConn
	gw   pb.SubmitQueueGatewayClient
}

// New dials the gateway described by opts.
//
// The caller closes the returned client. Dialling is lazy, as gRPC prefers, so
// an unreachable address surfaces on the first call rather than here.
func New(opts Options) (*Client, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("address must not be empty")
	}

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(transportCredentials(opts.TLS))}
	if creds, ok := bearerFrom(opts.TokenEnv); ok {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(creds))
	}

	conn, err := grpc.NewClient(opts.Addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gateway at %s: %w", opts.Addr, err)
	}
	return &Client{conn: conn, gw: pb.NewSubmitQueueGatewayClient(conn)}, nil
}

// Close releases the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Gateway is the generated client underneath, for a call this package does not
// wrap yet. Prefer the wrappers; this is the escape hatch, not the interface.
func (c *Client) Gateway() pb.SubmitQueueGatewayClient {
	return c.gw
}

// Ping checks the gateway answers, returning its reply.
func (c *Client) Ping(ctx context.Context, message string) (*pb.PingResponse, error) {
	resp, err := c.gw.Ping(ctx, &pb.PingRequest{Message: message})
	if err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return resp, nil
}

func transportCredentials(useTLS bool) credentials.TransportCredentials {
	if useTLS {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	return insecure.NewCredentials()
}

// bearer sends a token as an Authorization header on every call.
//
// Nothing in this repository checks it: the gateway admits every caller. It is
// here for a gateway reached through something that does — a proxy, a sidecar,
// an ingress terminating auth ahead of the service.
type bearer struct {
	token string
}

// GetRequestMetadata renders the credential as the header a server would read.
func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

// RequireTransportSecurity reports false so a token can be sent over a
// plaintext connection.
//
// gRPC otherwise refuses to attach per-RPC credentials without transport
// security, which is the right default and the wrong one for a local stack
// that has no certificates. The token is only as protected as the transport
// carrying it, so a deployment reachable by anyone else should also set TLS.
func (b bearer) RequireTransportSecurity() bool {
	return false
}

// bearerFrom reads the token out of the named variable, reporting whether there
// is one to send. An unset or empty variable is not an error: it is how a client
// against a gateway that wants no credential runs.
func bearerFrom(tokenEnv string) (bearer, bool) {
	if tokenEnv == "" {
		return bearer{}, false
	}
	token := os.Getenv(tokenEnv)
	if token == "" {
		return bearer{}, false
	}
	return bearer{token: token}, true
}

// WithTimeout derives a context carrying timeout, or the parent unchanged when
// timeout is not positive.
//
// A non-positive timeout means "no deadline", which is what a watch needs: it
// runs until its queue settles or the operator stops it, and a deadline meant
// for a single call would cut it short.
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
