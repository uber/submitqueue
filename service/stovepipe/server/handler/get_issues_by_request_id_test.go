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

package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/controller"
	controllermock "github.com/uber/submitqueue/stovepipe/controller/mock"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetIssuesByRequestID(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"success", nil, codes.OK}, {"invalid", controller.ErrInvalidRequest, codes.InvalidArgument},
		{"missing request", storage.ErrNotFound, codes.NotFound}, {"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded}, {"dependency", assert.AnError, codes.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := controllermock.NewMockGetIssuesByRequestIDController(gomock.NewController(t))
			c.EXPECT().GetIssuesByRequestID(gomock.Any(), entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12", PageSize: 10, PageToken: "token"}).Return(entity.GetIssuesByRequestIDResult{Status: entity.RequestIssuesStatusIncomplete, NextPageToken: "next"}, tt.err)
			server := NewStovepipeServer(nil, nil, nil, nil, nil, nil, nil, c)
			response, err := server.GetIssuesByRequestID(context.Background(), &pb.GetIssuesByRequestIDRequest{Queue: "queue", RequestId: "12", PageSize: 10, PageToken: "token"})
			require.Equal(t, tt.code, status.Code(err))
			if tt.err == nil {
				require.Equal(t, "incomplete", response.Status)
				require.Equal(t, "next", response.NextPageToken)
			} else {
				require.Nil(t, response)
			}
		})
	}
}
