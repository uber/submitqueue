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
	"errors"
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/errs"
	"github.com/uber/submitqueue/submitqueue/core/request"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	pb "github.com/uber/submitqueue/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

// RequestNotFoundError indicates that no request log records exist for the
// requested sqid. Either the sqid is wrong or the request has not been
// accepted yet.
type RequestNotFoundError struct {
	Sqid string
}

// Error implements the error interface.
func (e *RequestNotFoundError) Error() string {
	return fmt.Sprintf("request not found for sqid %q", e.Sqid)
}

// IsRequestNotFound returns true if any error in the chain is a
// *RequestNotFoundError.
func IsRequestNotFound(err error) bool {
	var target *RequestNotFoundError
	return errors.As(err, &target)
}

// StatusController handles request status business logic for the gateway.
type StatusController struct {
	logger          *zap.SugaredLogger
	metricsScope    tally.Scope
	requestLogStore storage.RequestLogStore
}

// NewStatusController creates a new instance of the gateway status controller.
func NewStatusController(logger *zap.SugaredLogger, scope tally.Scope, requestLogStore storage.RequestLogStore) *StatusController {
	return &StatusController{
		logger:          logger,
		metricsScope:    scope,
		requestLogStore: requestLogStore,
	}
}

// Status returns the current reconciled status of a request identified by its sqid.
func (c *StatusController) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	start := time.Now()
	defer func() {
		c.metricsScope.Timer("status_latency").Record(time.Since(start))
	}()

	c.metricsScope.Counter("status_count").Inc(1)

	if req.Sqid == "" {
		return nil, fmt.Errorf("StatusController requires the request to have a sqid specified: %w", ErrInvalidRequest)
	}

	state, err := request.GetCurrentStateFromRequestLog(ctx, c.requestLogStore, req.Sqid)
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, errs.NewUserError(&RequestNotFoundError{Sqid: req.Sqid})
		}
		return nil, fmt.Errorf("StatusController failed to get current state for sqid=%s: %w", req.Sqid, err)
	}

	c.logger.Debugw("request status retrieved",
		"sqid", req.Sqid,
		"status", string(state.Status),
	)

	return &pb.StatusResponse{
		Status:    string(state.Status),
		LastError: state.LastError,
		Metadata:  state.Metadata,
	}, nil
}
