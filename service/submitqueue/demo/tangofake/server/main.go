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

// Command server serves a Tango server over gRPC that derives changed
// targets from the TARGET files of a git repository (see package tangofake),
// for running the SubmitQueue demo without a build system.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/uber/tango/tangopb"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"

	"github.com/uber/submitqueue/platform/extension/gitworkspace/local"
	"github.com/uber/submitqueue/service/submitqueue/demo/tangofake"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	repo := flag.String("repo", "", "path to the git repository holding the TARGET files")
	address := flag.String("address", "127.0.0.1:0", "address to listen on for gRPC; port 0 picks a free port, which is logged")
	flag.Parse()
	if *repo == "" {
		return fmt.Errorf("-repo is required")
	}

	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *address, err)
	}

	transport := grpc.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "tango",
		Inbounds: yarpc.Inbounds{transport.NewInbound(listener)},
	})
	dispatcher.Register(tangopb.BuildTangoYARPCProcedures(tangofake.New(tangofake.Params{
		Workspaces: local.NewFactory(local.Params{Dir: *repo}),
	})))
	if err := dispatcher.Start(); err != nil {
		return fmt.Errorf("failed to start the server: %w", err)
	}
	defer func() { _ = dispatcher.Stop() }()

	log.Printf("fake tango serving %s on %s", *repo, listener.Addr())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return nil
}
