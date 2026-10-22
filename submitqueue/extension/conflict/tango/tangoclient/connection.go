// Copyright (c) 2026 Uber Technologies, Inc.
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

package tangoclient

import (
	"fmt"

	"github.com/uber/tango/tangopb"
	"go.uber.org/yarpc"
	yarpcgrpc "go.uber.org/yarpc/transport/grpc"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
)

// tangoServiceName is the name Tango's server registers under; a call is
// routed to the outbound with this name.
const tangoServiceName = "tango"

var strategies = map[string]tangopb.ComputationStrategy{
	"unset":  tangopb.COMPUTATION_STRATEGY_UNSET,
	"shell":  tangopb.COMPUTATION_STRATEGY_SHELL,
	"native": tangopb.COMPUTATION_STRATEGY_NATIVE,
}

// ValidStrategy reports whether name is a computation strategy this package can
// request: unset, shell or native.
func ValidStrategy(name string) bool {
	_, ok := strategies[name]
	return ok
}

// Connection is a gRPC connection to one Tango service, shared by every
// adapter built from it. It is safe for concurrent use.
type Connection struct {
	dispatcher *yarpc.Dispatcher
	client     tangopb.TangoYARPCClient
}

// Dial opens a connection to the Tango service at address, a host:port. The
// connection is established on first use, so a service that is not up yet does
// not fail Dial. Close releases it.
func Dial(address string) (*Connection, error) {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "submitqueue",
		Outbounds: yarpc.Outbounds{
			tangoServiceName: {Stream: yarpcgrpc.NewTransport().NewSingleOutbound(address)},
		},
	})
	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("failed to start the connection to Tango at %s: %w", address, err)
	}
	return &Connection{
		dispatcher: dispatcher,
		client:     tangopb.NewTangoYARPCClient(dispatcher.ClientConfig(tangoServiceName)),
	}, nil
}

// ChangedTargetsClient returns an adapter for the repository Tango knows as
// remote. An empty strategy means unset.
func (c *Connection) ChangedTargetsClient(remote, strategy string) (tango.ChangedTargetsClient, error) {
	if strategy == "" {
		strategy = "unset"
	}
	computation, ok := strategies[strategy]
	if !ok {
		return nil, fmt.Errorf("unknown Tango computation strategy %q", strategy)
	}
	return New(c.client, Config{Remote: remote, Strategy: computation}), nil
}

// Close stops the connection.
func (c *Connection) Close() error {
	return c.dispatcher.Stop()
}
