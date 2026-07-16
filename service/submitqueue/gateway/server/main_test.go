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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/submitqueue/submitqueue/gateway/controller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
