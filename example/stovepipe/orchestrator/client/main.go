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
	"flag"
	"fmt"
	"os"
	"time"

	pb "github.com/uber/submitqueue/stovepipe/orchestrator/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:8084", "stovepipe orchestrator server address")
	message := flag.String("message", "", "message to send in ping request")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if err := run(*addr, *message, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(addr, message string, timeout time.Duration) error {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	client := pb.NewStovepipeOrchestratorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req := &pb.PingRequest{
		Message: message,
	}

	fmt.Printf("Sending ping to stovepipe orchestrator at %s...\n", addr)
	resp, err := client.Ping(ctx, req)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	fmt.Printf("\nResponse:\n")
	fmt.Printf("  Message:      %s\n", resp.Message)
	fmt.Printf("  Service Name: %s\n", resp.ServiceName)
	fmt.Printf("  Timestamp:    %d (%s)\n", resp.Timestamp, time.Unix(resp.Timestamp, 0))
	fmt.Printf("  Hostname:     %s\n", resp.Hostname)

	return nil
}
