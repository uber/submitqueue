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
	"os"
	"time"

	"github.com/uber-go/tally"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/platform/metrics"
	"go.uber.org/zap"
)

// PingController handles ping business logic for the stovepipe
type PingController struct {
	logger       *zap.Logger
	metricsScope tally.Scope
}

// NewPingController creates a new instance of the stovepipe ping controller
func NewPingController(logger *zap.Logger, scope tally.Scope) *PingController {
	return &PingController{
		logger:       logger,
		metricsScope: scope,
	}
}

// Ping handles the ping request and returns a response
func (c *PingController) Ping(ctx context.Context, req *pb.PingRequest) (resp *pb.PingResponse, retErr error) {
	const opName = "ping"

	op := metrics.Begin(c.metricsScope, opName, metrics.FastLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	message := "pong!"
	isEcho := false
	if req.Message != "" {
		message = "echo: " + req.Message
		isEcho = true
		metrics.NamedCounter(c.metricsScope, opName, "echo_requests", 1)
	}

	hostname, _ := os.Hostname()

	c.logger.Info("ping request received",
		zap.String("message", req.Message),
		zap.Bool("is_echo", isEcho),
		zap.String("hostname", hostname),
	)

	return &pb.PingResponse{
		Message:     message,
		ServiceName: "stovepipe",
		Timestamp:   time.Now().Unix(),
		Hostname:    hostname,
	}, nil
}
